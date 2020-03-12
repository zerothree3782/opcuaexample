package com.hhi.opcuaexample.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "opcua")
public class OpcuaPropertiesList {
    private List<OpcuaProperties> list;
}
