package com.hhi.opcuaexample.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@RequiredArgsConstructor
@Slf4j
@Component
public class OpcuaClientRunner {

//    s

    NodeId[] nodeIds = {
            new NodeId(2,"Simulation Examples.Functions.Random1"),
            new NodeId(2,"Simulation Examples.Functions.Random2"),
            };

    @Async
    public void run(String endPointUrl,OpcUaClient client) {

        //CompletableFuture<OpcUaClient> future = new CompletableFuture<>();

        try {
//            future.whenCompleteAsync((c, ex) -> {
//                if (ex != null) {
//                    log.error("Error running example: {}", ex.getMessage(), ex);
//                }
//
//                try {
//                    client.disconnect().get();
//                    //Stack.releaseSharedResources();
//                } catch (InterruptedException | ExecutionException e) {
//                    log.error("Error disconnecting:", e.getMessage(), e);
//                }
//
//                try {
//                    Thread.sleep(1000);
//                    //System.exit(0);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            });

            try {
                subscription(endPointUrl,client);
                //future.get();
            } catch (Throwable t) {
                log.error("Error running client example: {}", t.getMessage(), t);
                //future.completeExceptionally(t);
            }
        } catch (Throwable t) {
            log.error("Error getting client: {}", t.getMessage(), t);

            //future.completeExceptionally(t);

//            try {
//                Thread.sleep(1000);
//                System.exit(0);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }

//        try {
//            Thread.sleep(999_999_999);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public void subscription(String endPointUrl,OpcUaClient client) throws Exception {
        // synchronous connect
        client.connect().get();
        log.info("=========endPointUrl=============::{}",endPointUrl);

        //연결이 끊어지고 다시 연결이 되는 것을 캐치하는 리스너 추가
        client.getSubscriptionManager().addSubscriptionListener(new UaSubscriptionManager.SubscriptionListener()  {
            @Override
            public void onSubscriptionTransferFailed(UaSubscription subscription, StatusCode statusCode) {
                try {
                    log.info("==============Subscription resumed====================");
                    subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

                    List<MonitoredItemCreateRequest> requestList = createRequestList(nodeIds,subscription);

                    // when creating items in MonitoringMode.Reporting this callback is where each item needs to have its
                    // value/event consumer hooked up. The alternative is to create the item in sampling mode, hook up the
                    // consumer after the creation call completes, and then change the mode for all items to reporting.
                    BiConsumer<UaMonitoredItem, Integer> onItemCreated = (item, id) -> item.setValueConsumer((uaMonitoredItem, value) -> {
                        onSubscriptionValue(uaMonitoredItem, value, endPointUrl);
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

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

        });

        // create a subscription @ 1000ms
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get();

        List<MonitoredItemCreateRequest> requestList = createRequestList(nodeIds,subscription);

        // when creating items in MonitoringMode.Reporting this callback is where each item needs to have its
        // value/event consumer hooked up. The alternative is to create the item in sampling mode, hook up the
        // consumer after the creation call completes, and then change the mode for all items to reporting.
        BiConsumer<UaMonitoredItem, Integer> onItemCreated = (item, id) -> item.setValueConsumer((uaMonitoredItem, value) -> {
            onSubscriptionValue(uaMonitoredItem, value, endPointUrl);
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
        // let the example run for 5 seconds then terminate
        //Thread.sleep(5000);
        //future.complete(client);
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

//        String fieldNamee = item.getReadValueId().getNodeId().getIdentifier().toString().replace(".","_").replace(" ","-");
//        batchPoints.point(Point.measurement("opcua_measurement")
//                .time(value.getServerTime().getJavaTime(), TimeUnit.MILLISECONDS)
//                .tag("ip", endPointUrl)
//                .field(fieldNamee, value.getValue().getValue())
//                .build());
//
//        if(batchPoints.getPoints().size() >= 100 ){
//
//            influxService.opcUaInsertList(batchPoints);
//
//            batchPoints = BatchPoints.database("HHI_test_database")
//                    .retentionPolicy("autogen").build();
//
//            batchPoints.getPoints().clear();
//
//        }
    }
}
