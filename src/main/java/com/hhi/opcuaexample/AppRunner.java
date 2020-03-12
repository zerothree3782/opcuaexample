package com.hhi.opcuaexample;

import com.hhi.opcuaexample.config.OpcuaClientConfig;
import com.hhi.opcuaexample.service.OpcuaClientRunner;
import lombok.RequiredArgsConstructor;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Map;


@Component
@RequiredArgsConstructor
public class AppRunner implements ApplicationRunner {

    private final OpcuaClientConfig opcuaClientConfig;

    private final OpcuaClientRunner opcuaClientRunner;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        Map<String, OpcUaClient> clientList = opcuaClientConfig.crateClients();

        //client갯수 만큼 thread를 분리하여 실행.
        for (Map.Entry<String, OpcUaClient> entry : clientList.entrySet()) {
            String key = entry.getKey();
            OpcUaClient value = entry.getValue();
            opcuaClientRunner.run(key, value);
        }
    }
}
