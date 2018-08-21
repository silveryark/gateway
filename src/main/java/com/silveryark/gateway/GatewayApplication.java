package com.silveryark.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import reactor.util.Loggers;

/**
 * 服务启动类，不做啥特别的事情，如果有spring配置的话走这里
 */
@SpringBootApplication(scanBasePackages = "com.silveryark")
@EnableConfigurationProperties
public class GatewayApplication {

    public static void main(String[] args) {
        Loggers.useSl4jLoggers();
        SpringApplication.run(GatewayApplication.class, args);
    }
}
