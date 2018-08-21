package com.silveryark.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Sidecar 主要功能就是维护在同一个Pod里的不同服务端口，因为通过k8s去部署的话同一个pod相当于同一台机器，一个pod做为一个部署单元实现高性能
 */
@Component
@ConfigurationProperties("")
public class Services {
    //服务对应的端口
    private Map<String, String> services;

    public String getService(String service) {
        return services.get(service);
    }

    //注册端口，主要都是通过application.properties来完成，不用手动调用
    public void setServices(Map<String, String> services) {
        this.services = services;
    }
}
