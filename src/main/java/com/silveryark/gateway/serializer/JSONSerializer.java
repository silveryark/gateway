package com.silveryark.gateway.serializer;

import com.silveryark.rpc.gateway.GatewayRequest;
import org.springframework.stereotype.Service;

@Service
public class JSONSerializer implements GatewayRequestSerializer {


    @Override
    public byte[] serialize(GatewayRequest request) {
        return new byte[0];
    }

    @Override
    public GatewayRequest deserialize(byte[] msg) {
        return null;
    }
}
