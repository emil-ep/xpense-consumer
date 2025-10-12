package com.xperia.xpense_consumer.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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

    public MutualFundDetailConsumer(Properties kafkaConsumerProperties){
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
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
