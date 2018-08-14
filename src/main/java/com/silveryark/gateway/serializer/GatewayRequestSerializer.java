package com.silveryark.gateway.serializer;

import com.silveryark.rpc.gateway.GatewayRequest;

public interface GatewayRequestSerializer {

    public byte[] serialize(GatewayRequest request);

    public GatewayRequest deserialize(byte[] msg);
}
