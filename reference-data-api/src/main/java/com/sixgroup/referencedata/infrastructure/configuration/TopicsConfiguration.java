package com.sixgroup.referencedata.infrastructure.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "topics")
@Data
public class TopicsConfiguration {

    private String trades;
    private String enrichedTrades;
    private String isin;
}
