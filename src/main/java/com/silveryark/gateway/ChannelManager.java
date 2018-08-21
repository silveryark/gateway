package com.silveryark.gateway;

import io.netty.channel.Channel;
import io.netty.util.internal.ConcurrentSet;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 管理所有的channel，两个对应关系：1. topic -> channels, 2.channel -> 使用的token（已经鉴权的标志）
 */
@Service
public class ChannelManager {

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static Map<String, Set<Channel>> topicSubscriptions = new ConcurrentHashMap<>();

    private static Map<Channel, String> tokens = new ConcurrentHashMap<>();

    //用户logout或者掉线，这样就不带票了，而且已经认证过的topic都要踢掉
    public void unregister(Channel channel) {
        Lock writeLock = ChannelManager.lock.writeLock();
        try {
            writeLock.lock();
            tokens.remove(channel);
            topicSubscriptions.entrySet().forEach(
                    (Map.Entry<String, Set<Channel>> entry) -> entry.getValue().remove(channel));
        } finally {
            writeLock.unlock();
        }
    }

    //用户鉴权完毕保存
    public void register(Channel channel, String token) {
        Lock writeLock = ChannelManager.lock.writeLock();
        try {
            writeLock.lock();
            tokens.put(channel, token);
        } finally {
            writeLock.unlock();
        }
    }

    //取用户token，在发请求的时候可以用
    public String token(Channel channel) {
        Lock readLock = ChannelManager.lock.readLock();
        try {
            readLock.lock();
            return tokens.get(channel);
        } finally {
            readLock.unlock();
        }
    }

    //要做token校验，不然无法控制client的链接了
    public void add(String topic, Channel channel) {
        if (tokens.containsKey(channel)) {
            Lock writeLock = ChannelManager.lock.writeLock();
            try {
                writeLock.lock();
                Set<Channel> channels = topicSubscriptions.putIfAbsent(topic, new ConcurrentSet<>());
                channels.add(channel);
            } finally {
                writeLock.unlock();
            }
        }
    }

    //从topic订阅里出来
    public void remove(String topic, Channel channel) {
        Lock writeLock = ChannelManager.lock.writeLock();
        try {
            writeLock.lock();
            Set<Channel> channels = topicSubscriptions.putIfAbsent(topic, new ConcurrentSet<>());
            channels.remove(channel);
        } finally {
            writeLock.unlock();
        }
    }

    //取topic下的channels
    public Set<Channel> topicChannels(String topic) {
        Lock readLock = ChannelManager.lock.readLock();
        try {
            readLock.lock();
            return Collections.unmodifiableSet(topicSubscriptions.get(topic));
        } finally {
            readLock.unlock();
        }
    }

    //取全部channels
    public Set<Channel> allChannels() {
        Lock readLock = ChannelManager.lock.readLock();
        try {
            readLock.lock();
            Collection<Set<Channel>> channelSets = topicSubscriptions.values();
            return channelSets.stream().flatMap(Collection::stream).collect(Collectors.toSet());
        } finally {
            readLock.unlock();
        }
    }
}
