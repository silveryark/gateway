package com.silveryark.gateway;

import com.silveryark.rpc.gateway.OutboundMessage;
import com.silveryark.rpc.serializer.OutboundMessageSerializer;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 处理下行消息，后端GS服务器如果要给客户端发消息的话会通过 ZeroMQ的broker发送消息（纯MQ模型）
 */
@Service
public class GatewayConsumer {

    public static final int TIMEOUT_FOR_TERMINATION_WORKER = 10;
    private static final Logger LOGGER = LoggerFactory.getLogger(GatewayConsumer.class);
    //管理所有客户端的链接
    private final ChannelManager channelManager;
    private final OutboundMessageSerializer outboundMessageSerializer;
    //链接broker地址
    @Value("${broker.connections}")
    private String brokerConnections;
    private ExecutorService workers = Executors.newSingleThreadExecutor();
    //这个worker专门处理后端来的控制数据，和用户消息线程分开
    private ExecutorService cmdWorkers = Executors.newSingleThreadExecutor();
    private ZContext context;
    private ZMQ.Socket subscriber;
    //专门的通道来接收控制消息
    private ZMQ.Socket cmdSubscriber;

    @Autowired
    public GatewayConsumer(ChannelManager channelManager, OutboundMessageSerializer outboundMessageSerializer) {
        this.outboundMessageSerializer = outboundMessageSerializer;
        this.channelManager = channelManager;
    }

    //注册broker，启动event loop
    @PostConstruct
    public void init() {
        context = new ZContext();
        subscriber = context.createSocket(ZMQ.SUB);
        cmdSubscriber = context.createSocket(ZMQ.SUB);
        for (String connection : brokerConnections.split(",")) {
            subscriber.connect(connection);
            cmdSubscriber.connect(connection);
        }
        subscriber.subscribe("worker");
        cmdSubscriber.subscribe("cmd");
        //处理控制命令
        cmdWorkers.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                //drop zmq topic message
                cmdSubscriber.recvStr();
                byte[] message = cmdSubscriber.recv();
                //TODO: 处理控制命令
            }
        });
        //提交任务
        workers.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                //drop zmq topic message
                subscriber.recvStr();
                //deal with real message
                byte[] message = subscriber.recv();
                try {
                    OutboundMessage outboundMessage = outboundMessageSerializer.deserialize(message);
                    //需要解出topic，然后再按照topic来发消息，如果没有topic或者topic无效，那就群发
                    String topic = outboundMessage.getTopic();
                    Set<Channel> channels = channelManager.topicChannels(topic);
                    if (channels != null) {
                        LOGGER.debug("Gateway Recv broadcast message: {} to topic: {}", message, topic);
                    } else {
                        channels = channelManager.allChannels();
                        LOGGER.debug("Gateway Recv broadcast message: {}", message);
                    }
                    String uid = outboundMessage.getUid();
                    if (uid != null) {
                        channels =
                                channels.stream()
                                        .filter(channel -> uid.contentEquals(channelManager.uid(channel)))
                                        .collect(Collectors.toSet());
                    }
                    for (Channel channel : channels) {
                        channel.writeAndFlush(outboundMessage.getPayload());
                    }
                } catch (IOException e) {
                    LOGGER.error("deserialize message {} error", message, e);
                }
            }
        });

    }

    @PreDestroy
    public void destroy() {
        //先把新任务停掉
        subscriber.close();
        cmdSubscriber.close();
        context.close();
        //再等待正在处理中的任务完成
        workers.shutdownNow();
        cmdWorkers.shutdownNow();
        try {
            while (!workers.isTerminated()) {
                workers.awaitTermination(TIMEOUT_FOR_TERMINATION_WORKER, TimeUnit.MILLISECONDS);
            }
            while (!cmdWorkers.isTerminated()) {
                cmdWorkers.awaitTermination(TIMEOUT_FOR_TERMINATION_WORKER, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            //如果被强行打断（应该不会）就直接退出
            LOGGER.debug("Interruped when shutdown", e);
            Thread.currentThread().interrupt();
        }
    }
}
