package com.silveryark.gateway.handler;

import com.silveryark.gateway.ChannelManager;
import com.silveryark.rpc.GenericRequest;
import com.silveryark.rpc.GenericResponse;
import com.silveryark.rpc.RPCHttpHeaders;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.authentication.AuthorizeRequest;
import com.silveryark.rpc.authentication.AuthorizeResponse;
import com.silveryark.rpc.gateway.InboundMessage;
import com.silveryark.rpc.serializer.InboundMessageSerializer;
import com.silveryark.utils.Services;
import io.netty.channel.Channel;
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
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@ChannelHandler.Sharable
@Service
public class MQTTServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQTTServerHandler.class);
    private final InboundMessageSerializer requestSerializer;
    private final ChannelManager channelManager;
    private final Services services;
    private WebClient.Builder clientBuilder;

    @Value("${server.group.id}")
    private int groupId;
    @Value("${server.group.version}")
    private String groupVersion;
    @Value("${server.group.name}")
    private String groupName;

    @Autowired
    public MQTTServerHandler(InboundMessageSerializer requestSerializer, Services services,
                             ChannelManager channelManager) {
        this.requestSerializer = requestSerializer;
        this.services = services;
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
            LOGGER.error("NettyMqttServerHandler channelRead empty msg {}", msg);
            return;
        }
        Mono<MqttMessage> mqttMessageMono = Mono.empty();
        switch (msg.fixedHeader().messageType()) {
            //鉴权，然后发送消息(如果有的话)，如果鉴权失败直接就断开了
            case CONNECT:
                WebClient client = clientBuilder.baseUrl(services.getService("authentication")).build();
                mqttMessageMono = handleConnect(ctx.channel(), (MqttConnectMessage) msg, client);
                break;
            //发消息
            case PUBLISH:
                mqttMessageMono = handlePublish(ctx.channel(), (MqttPublishMessage) msg);
                break;
            //订阅到一个topic上
            case SUBSCRIBE:
                mqttMessageMono = handleSubscribe(ctx.channel(), (MqttSubscribeMessage) msg);
                break;
            //取消订阅
            case UNSUBSCRIBE:
                mqttMessageMono = handleUnsubscribe(ctx.channel(), (MqttUnsubscribeMessage) msg);
                break;
            case PINGREQ:
                mqttMessageMono = handlePing(ctx.channel());
                break;
            case DISCONNECT:
                handleDisconnect(ctx.channel());
                break;
            default:
                mqttMessageMono = Mono.just(MqttMessageFactory.newInvalidMessage(new Exception(String.format(
                        "invalid message type: %s", msg.fixedHeader().messageType()))));
                break;
        }
        mqttMessageMono
                .map(mqttMessage -> ctx.channel().writeAndFlush(mqttMessage))
                .block();
    }

    private Mono<MqttMessage> handleConnect(Channel channel, MqttConnectMessage msg, WebClient client) {
        //verify username and password
        Mono<MqttMessage> mqttMessageMono;
        MqttConnectMessage connectMessage = msg;
        MqttConnectPayload connectPayload = connectMessage.payload();
        LOGGER.debug("Handle connect message with payload {}", connectPayload);
        //没有username和password，踢
        if (connectPayload.userName() == null || connectPayload.passwordInBytes() == null) {
            LOGGER.debug("No username or password found, return BAD_USER_OR_PASSWORD");
            MqttConnAckVariableHeader mqttVariableHeader = new MqttConnAckVariableHeader
                    (MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
            MqttFixedHeader fixedHeader =
                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, mqttVariableHeader,
                    null));
        } else {
            LOGGER.debug("Forward authentication request to authorization service");
            AuthorizeRequest authorizeRequest = new AuthorizeRequest(
                    new AuthorizeRequest.Credential(
                            connectPayload.userName(),
                            new String(connectPayload.passwordInBytes(), CharsetUtil.UTF_8)));
            Mono<AuthorizeResponse> resp = client
                    .post()
                    .uri("/token")
                    .header(RPCHttpHeaders.REQUEST_ID, authorizeRequest.getRequestId())
                    .body(BodyInserters.fromObject(authorizeRequest))
                    .retrieve()
                    .bodyToMono(AuthorizeResponse.class);
            //FIXME: 需要确定是否需要向下传递链接消息
            //reply ack
            mqttMessageMono = resp.map((AuthorizeResponse authorizeResponse) -> {
                LOGGER.debug("Authorization service response {}", authorizeResponse);
                //如果有异常，踢
                if (authorizeResponse.getPayload() instanceof Exception) {
                    return new MqttConnAckVariableHeader(
                            MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
                    //String的话就意味着这是一个token
                } else if (authorizeResponse.getPayload() instanceof String) {
                    String token = (String) authorizeResponse.getPayload();
                    String uid = authorizeResponse.getUid();
                    channelManager.register(channel, token, uid);
                    if (connectPayload.willTopic() != null && connectPayload.willMessageInBytes() != null) {
                        LOGGER.debug("Connect message has payload to publish {}/{}", connectPayload.willTopic(),
                                connectPayload.willMessageInBytes());
                        //这里会忽略掉publish message出异常的情况，统一返回CONNACK，如果出异常了可能导致BUG
                        publishMessage(connectPayload.willTopic(), token, connectPayload.willMessageInBytes());
                    }
                    return new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
                    //其它情况，直接踢
                } else {
                    LOGGER.debug("Unknown response, drop connection");
                    return new MqttConnAckVariableHeader(
                            MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, false);
                }
            }).map((MqttConnAckVariableHeader variableHeader) -> {
                MqttFixedHeader fixedHeader =
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                return MqttMessageFactory.newMessage(fixedHeader, variableHeader, null);
            });
        }
        return mqttMessageMono;
    }

    private Mono<MqttMessage> handlePublish(Channel channel, MqttPublishMessage msg) {
        Mono<MqttMessage> mqttMessageMono;
        MqttPublishMessage publishMessage = msg;
        byte[] bytes = new byte[publishMessage.content().readableBytes()];
        publishMessage.content().readBytes(bytes);
        String topicName = publishMessage.variableHeader().topicName();
        String token = channelManager.token(channel);
        Mono<GenericResponse> resp = publishMessage(topicName, token, bytes);
        //reply ack
        mqttMessageMono = resp.map((GenericResponse genericResponse) -> {
            LOGGER.debug("User {} publish connection Response {}", token, genericResponse);
            if (genericResponse.getStatus() == RPCResponse.STATUS.OK) {
                MqttFixedHeader fixedHeader =
                        new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                MqttPublishVariableHeader variableHeader =
                        new MqttPublishVariableHeader(topicName,
                                publishMessage.variableHeader().packetId());
                return MqttMessageFactory.newMessage(fixedHeader, variableHeader, null);
            } else {
                LOGGER.debug("Fail due to communication layer error {}", genericResponse);
                LOGGER.error(String.format("send publish message error, %s", genericResponse));
                return MqttMessageFactory.newInvalidMessage(genericResponse.getThrowable());
            }
        });
        return mqttMessageMono;
    }

    private Mono<MqttMessage> handleSubscribe(Channel channel, MqttSubscribeMessage msg) {
        Mono<MqttMessage> mqttMessageMono;
        MqttSubscribeMessage subscribeMessage = msg;
        List<MqttTopicSubscription> topicSubscriptions = subscribeMessage.payload().topicSubscriptions();
        LOGGER.debug("User {} subscribe to {}", channelManager.token(channel), topicSubscriptions);
        for (MqttTopicSubscription subscription : topicSubscriptions) {
            channelManager.add(subscription.topicName(), channel);
        }
        //reply ack
        List<Integer> qosList =
                topicSubscriptions.stream()
                        .map(mqttTopicSubscription -> mqttTopicSubscription.qualityOfService().value())
                        .collect(Collectors.toList());
        MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader =
                MqttMessageIdVariableHeader.from(subscribeMessage.variableHeader().messageId());
        MqttSubAckPayload payload = new MqttSubAckPayload(qosList);
        mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, variableHeader, payload));
        return mqttMessageMono;
    }

    private Mono<MqttMessage> handleUnsubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        Mono<MqttMessage> mqttMessageMono;
        MqttUnsubscribeMessage unsubscribeMessage = msg;
        List<String> topics = unsubscribeMessage.payload().topics();
        LOGGER.debug("User {} unsubscribe from {}", channelManager.token(channel), topics);
        for (String subscription : topics) {
            channelManager.remove(subscription, channel);
        }
        //reply ack
        MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(
                unsubscribeMessage.variableHeader().messageId());
        mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, variableHeader, null));
        return mqttMessageMono;
    }

    private Mono<MqttMessage> handlePing(Channel channel) {
        LOGGER.debug("User {} ping", channelManager.token(channel));
        Mono<MqttMessage> mqttMessageMono;
        MqttFixedHeader fixedHeader =
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        mqttMessageMono = Mono.just(MqttMessageFactory.newMessage(fixedHeader, null, null));
        return mqttMessageMono;
    }

    private void handleDisconnect(Channel channel) {
        LOGGER.debug("User {} disconnect", channelManager.token(channel));
        channelManager.unregister(channel);
        //FIXME: 需要确定是否需要向下传递链接断开消息
    }

    private Mono<GenericResponse> publishMessage(String topic, String token, byte[] content) {
        LOGGER.debug("Try to publish message {} to {} with token {}", content, topic, token);
        try {
            InboundMessage gatewayRequest = requestSerializer.deserialize(content);
            Object requestPayload = gatewayRequest.getPayload();
            //部署的时候将会在一个pod里，不同业务用端口作区分
            String serviceEndpoint = services.getService(topic);
            LOGGER.debug("{} Send to endpoint {}", token, serviceEndpoint);
            if (serviceEndpoint != null) {
                WebClient client = clientBuilder.baseUrl(serviceEndpoint).build();
                WebClient.RequestBodySpec requestBodySpec = client.post()
                        .uri("/messages");
                if (token != null) {
                    requestBodySpec = requestBodySpec.header("Authorization", String.format("Bearer %s", token));
                }
                GenericRequest genericRequest = new GenericRequest(requestPayload);
                return requestBodySpec
                        .header(RPCHttpHeaders.REQUEST_ID, genericRequest.getRequestId())
                        .body(BodyInserters.fromObject(genericRequest))
                        .retrieve()
                        .bodyToMono(GenericResponse.class)
                        //只要收到STATUS 为 OK的消息，就说明在传输层上正确传输，如果业务层有问题也和传输层无关了
                        .map(rpcResponse -> new GenericResponse(gatewayRequest.getRequestId(), rpcResponse.getStatus(),
                                rpcResponse.getStatus() == RPCResponse.STATUS.OK));
            }
            //客户端传了一个不存在的topic
            LOGGER.debug("{} try to find service of topic {} not found, client error", token, topic);
            return Mono.just(new GenericResponse(gatewayRequest.getRequestId(), RPCResponse.STATUS.CLIENT_ERROR,
                    Boolean.FALSE));
        } catch (IOException e) {
            LOGGER.error("Exception on publishMessage {}", content, e);
            //如果出错，基本上就是JSON解析有问题，在这种情况下没有requestId
            return Mono.just(new GenericResponse(null, RPCResponse.STATUS.CLIENT_ERROR, Boolean.FALSE));
        }
    }
}
