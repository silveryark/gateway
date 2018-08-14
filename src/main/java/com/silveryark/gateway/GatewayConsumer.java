package com.silveryark.gateway;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class GatewayConsumer {

    private final ChannelManager channelManager;
    @Value("${broker.connection}")
    private String brokerConnection;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private ExecutorService workers = Executors.newSingleThreadExecutor();

    @Autowired
    public GatewayConsumer(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @PostConstruct
    public void init() {
        try (ZContext context = new ZContext(); ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB)) {
            subscriber.connect(brokerConnection);
            subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);
            workers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    String message = subscriber.recvStr();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Gateway Recv broadcast message: {}", message);
                    }
                    for (Channel channel : channelManager.allChannels()) {
                        channel.writeAndFlush(message);
                    }
                }
            });
        }
    }

    @PreDestroy
    public void destroy() {
        workers.shutdown();
        try {
            while (!workers.isTerminated()) {
                workers.awaitTermination(10, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(e.getMessage(), e);
            }
        }
    }
}
