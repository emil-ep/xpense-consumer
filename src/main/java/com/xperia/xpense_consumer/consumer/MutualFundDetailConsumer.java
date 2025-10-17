package com.xperia.xpense_consumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.xperia.models.MutualFundDetailModel;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MutualFundDetailConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualFundDetailConsumer.class);
    private static final List<String> TOPICS_TO_CONSUME = Arrays.asList("scheme_detail");
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public MutualFundDetailConsumer(Properties kafkaConsumerProperties, RestTemplate restTemplate){
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.restTemplate = restTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void start(){
        consumer.subscribe(TOPICS_TO_CONSUME);
        executorService.submit(() -> {
            try{
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record: records){
                        String schemeCode = record.value();
                        String url = "https://api.mfapi.in/mf/" + schemeCode;
                        MutualFundDetailModel response = restTemplate.getForObject("https://api.mfapi.in/mf/118621", MutualFundDetailModel.class);
//                        MutualFundSchemeDetail  fundSchemeDetail = new MutualFundSchemeDetail(response.getSchemeCode(),
//                                response.getSchemeType(),
//                                response.getSchemeCategory(),
//                                response.getFundHouse(),
//                                response.getSchemeName(),
//                                this.objectMapper.valueToTree(response.getData())
//                        );
                        LOGGER.info("Received scheme : {}", schemeCode);
                    }
                }
            }catch (Exception ex){
                LOGGER.error("Error while consuming record from : {}",
                        TOPICS_TO_CONSUME
                        .stream()
                        .reduce((topic1, topic2) -> topic1.concat(",")));
            }finally {
                shutdown();
            }
        });
    }

    public void shutdown(){
        executorService.shutdown();
    }
}
