package com.silveryark.gateway.handler;

import com.silveryark.gateway.ChannelManager;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 超时处理
 */
@ChannelHandler.Sharable
@Service
public class NettyMqttIdleTimeoutHandler extends ChannelDuplexHandler {

    private final ChannelManager channelManager;

    @Autowired
    public NettyMqttIdleTimeoutHandler(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleState e = ((IdleStateEvent) evt).state();
            //如果用户IDLE太久了，就踢下线
            if (e == IdleState.ALL_IDLE) {
                ctx.close();
            }
            channelManager.unregister(ctx.channel());
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
