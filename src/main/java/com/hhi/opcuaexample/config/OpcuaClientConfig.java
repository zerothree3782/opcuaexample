package com.hhi.opcuaexample.config;

import com.hhi.opcuaexample.properties.ClientProperties;
import com.hhi.opcuaexample.properties.OpcuaProperties;
import com.hhi.opcuaexample.properties.OpcuaPropertiesList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.Stack;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@RequiredArgsConstructor
@Slf4j
@Component
@EnableConfigurationProperties(OpcuaPropertiesList.class)
public class OpcuaClientConfig {

    private final OpcuaPropertiesList opcuaPropertiesList;

    public Map<String, OpcUaClient> crateClients() throws Exception {

        List<OpcuaProperties> clientList = opcuaPropertiesList.getList();

        Map<String, OpcUaClient> clientMap = new HashMap<>();

        for (OpcuaProperties opcuaProperties : clientList) {

            String endPointUrl = opcuaProperties.getEndPointUrl();
            String userName = opcuaProperties.getUserName();
            String password = opcuaProperties.getPassword();

            //Crate ThreadPool
            ExecutorService executorService = createExecutor(endPointUrl);

            //Setting Client
            ClientProperties clientProperties = new ClientProperties();
            clientProperties.setEndpointUrl(endPointUrl);
            clientProperties.setEndpointFilter(e -> true);
            //계정 유무 체크
            if (userName == null || password == null)
                clientProperties.setIdentityProvider(new AnonymousProvider());
            else
                clientProperties.setIdentityProvider(new UsernameProvider(userName, password));
            clientProperties.setSecurityPolicy(SecurityPolicy.None);

            Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "security");
            Files.createDirectories(securityTempDir);
            if (!Files.exists(securityTempDir)) {
                throw new Exception("unable to create security dir: " + securityTempDir);
            }
            LoggerFactory.getLogger(getClass())
                    .info("security temp dir: {}", securityTempDir.toAbsolutePath());

            //KeyStoreLoader loader = new KeyStoreLoader().load(securityTempDir);

            SecurityPolicy securityPolicy = clientProperties.getSecurityPolicy();

            List<EndpointDescription> endpoints;

            try {
                log.info("####endPointUrl######::{}", clientProperties.getEndpointUrl());
                endpoints = DiscoveryClient.getEndpoints(clientProperties.getEndpointUrl()).get();
            } catch (Throwable ex) {
                try {
                    // try the explicit discovery endpoint as well
                    String discoveryUrl = clientProperties.getEndpointUrl();

                    if (!discoveryUrl.endsWith("/")) {
                        discoveryUrl += "/";
                    }
                    discoveryUrl += "discovery";

                    log.info("Trying explicit discovery URL: {}", discoveryUrl);
                    endpoints = DiscoveryClient.getEndpoints(discoveryUrl).get();

                } catch (Throwable e) {
                    log.warn("{} connect failed!",endPointUrl);
                    continue;
                }
            }

            EndpointDescription endpoint = endpoints.stream()
                    .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getUri()))
                    .filter(clientProperties.getEndpointFilter())
                    .findFirst()
                    .orElseThrow(() -> new Exception("no desired endpoints returned"));

            log.info("Using endpoint: {} [{}/{}]",
                    endpoint.getEndpointUrl(), securityPolicy, endpoint.getSecurityMode());

            OpcUaClientConfig config = OpcUaClientConfig.builder()
                    .setApplicationName(LocalizedText.english(endPointUrl + "-client"))
                    .setApplicationUri("urn:eclipse:milo:examples:client")
                    //.setCertificate(loader.getClientCertificate())
                    //.setKeyPair(loader.getClientKeyPair())
                    .setEndpoint(endpoint)
                    .setIdentityProvider(clientProperties.getIdentityProvider())
                    .setRequestTimeout(uint(5000))
                    .setExecutor(executorService)
                    .build();

            clientMap.put(clientProperties.getEndpointUrl(), OpcUaClient.create(config));
        }
        return clientMap;
    }

    private synchronized ExecutorService createExecutor(String endPointUrl) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicLong threadNumber = new AtomicLong(0L);

            @Override
            public Thread newThread(@NonNull Runnable r) {
                Thread thread = new Thread(r, endPointUrl + "-" + threadNumber.getAndIncrement());
                thread.setDaemon(true);
                thread.setUncaughtExceptionHandler(
                        (t, e) ->
                                LoggerFactory.getLogger(Stack.class)
                                        .warn("Uncaught Exception on shared stack ExecutorService thread!", e)
                );
                return thread;
            }
        };
        return Executors.newCachedThreadPool(threadFactory);
    }

}
