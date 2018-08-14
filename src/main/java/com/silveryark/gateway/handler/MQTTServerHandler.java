package com.silveryark.gateway.handler;

import com.silveryark.gateway.ChannelManager;
import com.silveryark.gateway.Sidecar;
import com.silveryark.gateway.serializer.GatewayRequestSerializer;
import com.silveryark.rpc.BooleanResponse;
import com.silveryark.rpc.ExceptionResponse;
import com.silveryark.rpc.GenericRequest;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.authentication.AuthorizeRequest;
import com.silveryark.rpc.gateway.GatewayRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

@ChannelHandler.Sharable
@Service
public class MQTTServerHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final GatewayRequestSerializer requestSerializer;
    private final ChannelManager channelManager;
    private Sidecar sidecar;
    private WebClient.Builder clientBuilder;

    @Value("${server.group.id}")
    private int groupId;
    @Value("${server.group.version}")
    private String groupVersion;
    @Value("${server.group.name}")
    private String groupName;
    @Value("${service.authentication}")
    private String authenticationService;

    @Autowired
    public MQTTServerHandler(GatewayRequestSerializer requestSerializer, Sidecar sidecar, ChannelManager channelManager) {
        this.requestSerializer = requestSerializer;
        this.sidecar = sidecar;
        this.channelManager = channelManager;
    }

    @PostConstruct
    protected void init() {
        clientBuilder = WebClient.builder()
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8.toString())
                .defaultHeader(HttpHeaders.USER_AGENT, "gateway");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        MqttMessage msg = (MqttMessage) message;

        if (msg.fixedHeader() == null) {
            logger.error("NettyMqttServerHandler channelRead empty msg {}", msg);
            return;
        }
        WebClient client = clientBuilder.baseUrl(String.format("http://%s", authenticationService)).build();
        try {
            Mono<MqttMessage> mqttMessageMono = Mono.empty();
            switch (msg.fixedHeader().messageType()) {
                case CONNECT: {
                    //verify username and password
                    MqttConnectMessage connectMessage = (MqttConnectMessage) msg;
                    MqttConnectPayload connectPayload = connectMessage.payload();
                    if (connectPayload.userName() == null || connectPayload.passwordInBytes() == null) {
                        MqttConnAckVariableHeader mqttVariableHeader = new MqttConnAckVariableHeader
                                (MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
                        MqttFixedHeader fixedHeader =
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                        mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, mqttVariableHeader,
                                null));
                    } else {
                        Mono<RPCResponse> resp = client
                                .post()
                                .uri("/token")
                                .body(BodyInserters.fromObject(new AuthorizeRequest(new AuthorizeRequest.Credential(connectPayload.userName(), new String(connectPayload.passwordInBytes(), CharsetUtil.UTF_8)))))
                                .retrieve()
                                .bodyToMono(RPCResponse.class);
                        //FIXME: 需要确定是否需要向下传递链接消息
                        //reply ack
                        mqttMessageMono = resp.map(rpcResponse -> {
                            MqttConnAckVariableHeader mqttVariableHeader;
                            if (rpcResponse instanceof ExceptionResponse) {
                                mqttVariableHeader =
                                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
                            } else if (rpcResponse.getPayload() instanceof String) {
                                String token = (String) rpcResponse.getPayload();
                                channelManager.register(ctx.channel(), token);
                                mqttVariableHeader =
                                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
                                if (connectPayload.willTopic() != null && connectPayload.willMessageInBytes() != null) {
                                    //TODO: 这里会忽略掉publish message出异常的情况，统一返回CONNACK，如果出异常了可能导致BUG
                                    publishMessage(connectPayload.willTopic(), token,
                                            connectPayload.willMessageInBytes());
                                }
                            } else {
                                mqttVariableHeader =
                                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, false);
                            }
                            MqttFixedHeader fixedHeader =
                                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                            return MqttMessageFactory.newMessage(fixedHeader, mqttVariableHeader, null);
                        });
                    }
                    break;
                }
                case PUBLISH: {
                    MqttPublishMessage publishMessage = (MqttPublishMessage) msg;
                    byte[] bytes = new byte[publishMessage.content().readableBytes()];
                    publishMessage.content().readBytes(bytes);
                    Mono<BooleanResponse> resp = publishMessage(publishMessage.variableHeader().topicName(),
                            channelManager.token(ctx.channel()),
                            bytes);
                    //reply ack
                    mqttMessageMono = resp.map(booleanResponse -> {
                        if (booleanResponse.getStatus() == RPCResponse.STATUS.OK) {
                            MqttFixedHeader fixedHeader =
                                    new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                            MqttPublishVariableHeader variableHeader =
                                    new MqttPublishVariableHeader(publishMessage.variableHeader().topicName(),
                                            publishMessage.variableHeader().packetId());
                            return MqttMessageFactory.newMessage(fixedHeader, variableHeader, null);
                        } else {
                            //TODO: 这里没有进行出错的处理，需要进一步考证是否客户端在没收到 PUBACK(收到InvalidMessage) 的时候是否会重试，如果重试那么逻辑就是预期的，如果没重试，那就需要修正
                            logger.error(String.format("send publish message error, %s", booleanResponse));
                            return MqttMessageFactory.newInvalidMessage(booleanResponse.getThrowable());
                        }
                    });
                    break;
                }
                case SUBSCRIBE: {
                    //subscribe in topic list
                    MqttSubscribeMessage subscribeMessage = (MqttSubscribeMessage) msg;
                    for (MqttTopicSubscription subscription : subscribeMessage.payload().topicSubscriptions()) {
                        channelManager.add(subscription.topicName(), ctx.channel());
                    }
                    //reply ack
                    List<Integer> qosList = subscribeMessage.payload().topicSubscriptions().stream().map(mqttTopicSubscription -> mqttTopicSubscription.qualityOfService().value()).collect(Collectors.toList());
                    MqttFixedHeader fixedHeader =
                            new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader =
                            MqttMessageIdVariableHeader.from(subscribeMessage.variableHeader().messageId());
                    MqttSubAckPayload payload = new MqttSubAckPayload(qosList);
                    mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, variableHeader, payload));
                    break;
                }
                case UNSUBSCRIBE: {
                    //unsubscribe topic
                    MqttUnsubscribeMessage unsubscribeMessage = (MqttUnsubscribeMessage) msg;
                    for (String subscription : unsubscribeMessage.payload().topics()) {
                        channelManager.remove(subscription, ctx.channel());
                    }
                    //reply ack
                    MqttFixedHeader fixedHeader =
                            new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(unsubscribeMessage.variableHeader().messageId());
                    mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, variableHeader, null));
                    break;
                }
                case PINGREQ: {
                    //reply ack
                    MqttFixedHeader fixedHeader =
                            new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, null, null));
                    break;
                }
                case DISCONNECT: {
                    channelManager.unregister(ctx.channel());
                    //FIXME: 需要确定是否需要向下传递链接断开消息
                    break;
                }
                default:
                    mqttMessageMono = Mono.just(MqttMessageFactory.newInvalidMessage(new Exception(String.format(
                            "invalid message type: %s", msg.fixedHeader().messageType()))));
                    break;
            }
            mqttMessageMono.doOnNext(mqttMessage -> {
                ctx.channel().writeAndFlush(mqttMessage);
            });
        } catch (Exception ex) {
            logger.error("NettyMqttServerHandler Bad error in processing the message", ex);
            ctx.fireExceptionCaught(ex);
        }
    }

    private Mono<BooleanResponse> publishMessage(String topic, String token, byte[] content) {
        GatewayRequest gatewayRequest = requestSerializer.deserialize(content);
        Object requestPayload = gatewayRequest.getPayload();
        //部署的时候将会在一个pod里，不同业务用端口作区分
        WebClient client = clientBuilder.baseUrl(String.format("http://localhost:%s", sidecar.getServicePort
                (topic)))
                .build();
        WebClient.RequestBodySpec requestBodySpec = client.post()
                .uri("/messages");
        if (token != null) {
            requestBodySpec = requestBodySpec.header("Authorization", String.format("Bearer %s", token));
        }
        return requestBodySpec
                .body(BodyInserters.fromObject(new GenericRequest(requestPayload)))
                .retrieve()
                .bodyToMono(RPCResponse.class)
                //只要收到STATUS 为 OK的消息，就说明在传输层上正确传输，如果业务层有问题也和传输层无关了
                .map(rpcResponse -> new BooleanResponse(gatewayRequest.getRequestId(), rpcResponse.getStatus(),
                        rpcResponse.getStatus() == RPCResponse.STATUS.OK));
    }
}
