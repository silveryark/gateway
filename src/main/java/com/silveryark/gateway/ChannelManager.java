package com.silveryark.gateway;

import io.netty.channel.Channel;
import io.netty.util.internal.ConcurrentSet;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class ChannelManager {

    private Map<String, Set<Channel>> topicSubscriptions = new ConcurrentHashMap<>();

    private Map<Channel, String> tokens = new ConcurrentHashMap<>();

    public void register(Channel channel, String token) {
        tokens.put(channel, token);
    }

    public void unregister(Channel channel) {
        tokens.remove(channel);
    }

    public String token(Channel channel) {
        return tokens.get(channel);
    }

    public void add(String topic, Channel channel) {
        Set<Channel> channels = topicSubscriptions.putIfAbsent(topic, new ConcurrentSet<>());
        channels.add(channel);
    }

    public void remove(String topic, Channel channel) {
        Set<Channel> channels = topicSubscriptions.putIfAbsent(topic, new ConcurrentSet<>());
        channels.remove(channel);
    }

    public Set<Channel> topicChannels(String topic) {
        return Collections.unmodifiableSet(topicSubscriptions.get(topic));
    }

    public Set<Channel> allChannels() {
        Collection<Set<Channel>> channelSets = topicSubscriptions.values();
        return channelSets.stream().flatMap(collection -> collection.stream()).collect(Collectors.toSet());
    }
}
