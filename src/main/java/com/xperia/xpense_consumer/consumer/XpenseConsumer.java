package com.xperia.xpense_consumer.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class XpenseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(XpenseConsumer.class);
    private final Properties consumerProperties;
    private KafkaConsumer<String, String> consumer;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    public XpenseConsumer(Properties kafkaConsumerProperties){
        this.consumerProperties = kafkaConsumerProperties;
    }

    @PostConstruct
    public void start(){
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("mf_scheme"));
        executorService.submit(() -> {
            try{
                while (running){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records){
                        LOGGER.info("Consumed message = topic : {} \n partition : {} \n offset : {} \n key: {} \n value : {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                }
            }catch (Exception ex){
                LOGGER.error("Exception occurred while consuming message ", ex);
            }finally {
                shutdown();
            }
        });
    }

    public void shutdown(){
        running = false;
        executorService.shutdown();
    }
}
