package com.hhi.opcuaexample.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
@Service
public class InfluxService {
    private final InfluxDB influxDB;

    @Async
    public void opcUaInsertList(List<UaMonitoredItem> uaMonitoredItemList, List<DataValue> dataValueList,String endPointUrl){
        try {
            //influxDB.enableGzip();
            //influxDB.setRetentionPolicy("autogen");
            BatchPoints batchPoints = BatchPoints.database("HHI_test_database")
                    .retentionPolicy("autogen").build();

            for(int i = 0; i<dataValueList.size(); i++){

                DataValue value = dataValueList.get(i);
                UaMonitoredItem item = uaMonitoredItemList.get(i);

                String fieldNamee = item.getReadValueId().getNodeId().getIdentifier().toString().replace(".","_").replace(" ","-");
                batchPoints.point(Point.measurement("opcua_measurement")
                        .time(value.getServerTime().getJavaTime(), TimeUnit.MILLISECONDS)
                        .tag("ip", endPointUrl)
                        .field(fieldNamee, value.getValue().getValue())
                        .build());
            }

            influxDB.write(batchPoints);
            influxDB.close();
            log.info("opcUaInsertList 성공 : {}", batchPoints.getPoints().size() + "건");

        }catch (Exception ex){
            log.error("opcUaInsertList : {}", ex.getMessage());
            ex.printStackTrace();
        }

    }

}
