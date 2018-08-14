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

@Service
public class GatewayServer {

    private final NettyMqttIdleTimeoutHandler timeoutHandler;
    private final MQTTServerHandler serverHandler;

    @Value("${server.port}")
    private int port;

    @Value("${server.address}")
    private String address;

    private Logger logger = LoggerFactory.getLogger(getClass());
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
                        pipeline.addFirst("NettyIdleStateHandler", new IdleStateHandler(60, 0, 0));
                        pipeline.addAfter("NettyIdleStateHandler", "NettyIdleStateEventHandler", timeoutHandler);
                        pipeline.addLast("MQTTDecoder", new MqttDecoder());
                        pipeline.addLast("MQTTEncoder", MqttEncoder.INSTANCE);
                        pipeline.addLast("MQTTServerHandler", serverHandler);

                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

        // Bind and start to accept incoming connections.
        b.bind(new InetSocketAddress(address, port)).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                logger.info("NettyServer Started Succeeded on {}:{}, registry is complete, waiting for client connect...", address, port);
            } else {
                logger.error("NettyServer Started Failed on {}:{}, registry is incomplete {}", address, port, channelFuture.cause());
            }
        });
    }

    @PreDestroy
    public void destroy() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
