package com.silveryark.gateway;

import com.silveryark.gateway.handler.MQTTServerHandler;
import com.silveryark.gateway.handler.NettyMqttIdleTimeoutHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;

/**
 * 网关服务，负责服务起动，处理类注册以及服务关闭
 */
@Service
public class GatewayServer {

    public static final int READER_IDLE_TIME_SECONDS = 60;
    public static final int SO_BACKLOG = 128;
    private static final Logger LOGGER = LoggerFactory.getLogger(GatewayServer.class);
    private final NettyMqttIdleTimeoutHandler timeoutHandler;
    private final MQTTServerHandler serverHandler;
    //监听端口(MQTT的端口)
    @Value("${server.port}")
    private int port;
    //监听地址
    @Value("${server.address}")
    private String address;
    private EventLoopGroup bossGroup = new NioEventLoopGroup();
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

    @Autowired
    public GatewayServer(NettyMqttIdleTimeoutHandler timeoutHandler, MQTTServerHandler serverHandler) {
        this.timeoutHandler = timeoutHandler;
        this.serverHandler = serverHandler;
    }

    @PostConstruct
    public void init() {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        //处理心跳
                        pipeline.addFirst("NettyIdleStateHandler", new IdleStateHandler(READER_IDLE_TIME_SECONDS, 0,
                                0));
                        //超时就触发断开
                        pipeline.addAfter("NettyIdleStateHandler", "NettyIdleStateEventHandler", timeoutHandler);
                        pipeline.addLast("MQTTDecoder", new MqttDecoder());
                        pipeline.addLast("MQTTEncoder", MqttEncoder.INSTANCE);
                        //具体处理逻辑
                        pipeline.addLast("MQTTServerHandler", serverHandler);

                    }
                })
                //链接配置
                .option(ChannelOption.SO_BACKLOG, SO_BACKLOG)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        // Bind and start to accept incoming connections.
        b.bind(new InetSocketAddress(address, port)).addListener((ChannelFuture channelFuture) -> {
            if (channelFuture.isSuccess()) {
                LOGGER.info("NettyServer Started Succeeded on {}:{}, registry is complete, waiting for client " +
                        "connect...", address, port);
            } else {
                //如果失败就要退出，不然就sb了
                LOGGER.error("NettyServer Started Failed on {}:{}, registry is incomplete {}", address, port,
                        channelFuture.cause());
                System.exit(1);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
