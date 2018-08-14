package com.silveryark.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties("sidecar")
public class Sidecar {
    private Map<String, Integer> servicePort;

    public Integer getServicePort(String service) {
        return servicePort.get(service);
    }

    public void setService(Map<String, Integer> service) {
        this.servicePort = service;
    }
}
