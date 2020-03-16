package com.hhi.opcuaexample.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.BiConsumer;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@RequiredArgsConstructor
@Slf4j
@Component
public class OpcuaClientRunner {

    private final InfluxService influxService;

    NodeId[] nodeIds = {
            new NodeId(2,"Simulation Examples.Functions.Random1"),
            new NodeId(2,"Simulation Examples.Functions.Random2")
//            new NodeId(2,"Simulation Examples.Functions.Random3"),
//            new NodeId(2,"Simulation Examples.Functions.Random4"),
//            new NodeId(2,"Simulation Examples.Functions.Random5"),
//            new NodeId(2,"Simulation Examples.Functions.Random6"),
//            new NodeId(2,"Simulation Examples.Functions.Random7"),
//            new NodeId(2,"Simulation Examples.Functions.Random8")
            };

    @Async
    public void run(String endPointUrl,OpcUaClient client){

        //log.error("Error running client example: {}", t.getMessage(), t);
        //log.error("Error getting client: {}", t.getMessage(), t);

        try {
            // synchronous connect
            client.connect().get();
            log.info("=========endPointUrl=============::{}", endPointUrl);

            //연결이 끊어지고 다시 연결되었을 때 subscription을 다시 만들고 설정들을 다시 해줘야 한다.
            client.getSubscriptionManager().addSubscriptionListener(new UaSubscriptionManager.SubscriptionListener() {
                @Override
                public void onSubscriptionTransferFailed(UaSubscription subscription, StatusCode statusCode) {
                    subscribe(endPointUrl, client);
                }
            });

            subscribe(endPointUrl, client);
        }catch (Throwable t){
            log.error("Error running client : {}", t.getMessage(), t);
        }
    }

    private void subscribe(String endPointUrl, OpcUaClient client) {
        try {
            // create a subscription @ 1000ms
            UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();


            // insert influxDB
            subscription.addNotificationListener(new UaSubscription.NotificationListener() {
                @Override
                public void onDataChangeNotification(UaSubscription subscription, List<UaMonitoredItem> monitoredItems, List<DataValue> dataValues, DateTime publishTime) {
                    influxService.opcUaInsertList(monitoredItems, dataValues, endPointUrl);
                }
            });

            List<MonitoredItemCreateRequest> requestList = createRequestList(nodeIds, subscription);

            // when creating items in MonitoringMode.Reporting this callback is where each item needs to have its
            // value/event consumer hooked up. The alternative is to create the item in sampling mode, hook up the
            // consumer after the creation call completes, and then change the mode for all items to reporting.
            BiConsumer<UaMonitoredItem, Integer> onItemCreated = (item, id) -> item.setValueConsumer((uaMonitoredItem, value) -> {
            });

            List<UaMonitoredItem> items = subscription.createMonitoredItems(
                    TimestampsToReturn.Both,
                    requestList,
                    onItemCreated
            ).get();

            for (UaMonitoredItem item : items) {
                if (item.getStatusCode().isGood()) {
                    log.info("item created for nodeId={}", item.getReadValueId().getNodeId());
                } else {
                    log.warn(
                            "failed to create item for nodeId={} (status={})",
                            item.getReadValueId().getNodeId(), item.getStatusCode());
                }
            }
        }catch (Throwable ex){
            ex.getStackTrace();
        }
    }

    //node별 request생성 후 list저장
    private List<MonitoredItemCreateRequest> createRequestList(NodeId[] nodeIds,UaSubscription subscription){
        List<MonitoredItemCreateRequest> requestList = newArrayList();

        for(NodeId nodeId:nodeIds) {
            // subscribe to the Value attribute of the server's CurrentTime node
            ReadValueId readValueId = new ReadValueId(
                    nodeId,
                    AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE
            );

            UInteger clientHandle = subscription.nextClientHandle();

            MonitoringParameters parameters = new MonitoringParameters(
                    clientHandle,
                    1000.0,     // sampling interval
                    null,       // filter, null means use default
                    uint(1),   // queue size
                    true        // discard oldest
            );

            MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                    readValueId,
                    MonitoringMode.Reporting,
                    parameters
            );

            requestList.add(request);
        }

        return requestList;
    }

    private void onSubscriptionValue(UaMonitoredItem item, DataValue value, String endPointUrl) {

        log.info(
                "subscription value received: item={}, value={}",
                item.getReadValueId().getNodeId(), value.getValue());

    }
}
