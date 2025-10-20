package com.xperia.xpense_consumer.consumer;

import com.xperia.xpense_consumer.service.MutualFundSchemeDetailService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MutualFundDetailMultiThreadedConsumer {

    @Value("${mutualFund.api.url}")
    private String mutualFundUrl;

    private final MutualFundSchemeDetailService schemeDetailService;
    private final Properties kafkaConsumerProperties;
    private final RestTemplate restTemplate;
    private static final int NUMBER_OF_CONSUMERS = 20;
    private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CONSUMERS);

    @Autowired
    public MutualFundDetailMultiThreadedConsumer(Properties kafkaConsumerProperties, RestTemplate restTemplate, MutualFundSchemeDetailService schemeDetailService){
        this.restTemplate = restTemplate;
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.schemeDetailService = schemeDetailService;
    }

    @PostConstruct
    public void start(){
        for (int i = 0; i < NUMBER_OF_CONSUMERS; i ++){
            MutualFundDetailConsumer consumer = new MutualFundDetailConsumer(kafkaConsumerProperties, restTemplate,
                    schemeDetailService, mutualFundUrl);
            executorService.submit(consumer);
        }
    }
}
