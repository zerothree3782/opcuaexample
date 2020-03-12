package com.hhi.opcuaexample.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Slf4j
@Service
public class InfluxService {
    private final InfluxDB influxDB;

    @Async
    public void opcUaInsertList(BatchPoints batchPoints){
        try {
            influxDB.enableGzip();
            influxDB.setRetentionPolicy("autogen");
            influxDB.write(batchPoints);
            influxDB.close();
            log.info("opcUaInsertList 성공 : {}", batchPoints.getPoints().size() + "건");
        }catch (Exception ex){
            log.error("opcUaInsertList : {}", ex.getMessage());
            ex.printStackTrace();
        }

    }

}
