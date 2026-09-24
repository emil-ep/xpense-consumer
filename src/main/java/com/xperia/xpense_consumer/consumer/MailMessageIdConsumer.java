package com.xperia.xpense_consumer.consumer;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.xperia.models.XpenseKafkaTopics;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MailMessageIdConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MailMessageIdConsumer.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Properties consumerProperties;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    @Autowired
    public MailMessageIdConsumer(Properties kafkaConsumerProperties){
        this.consumerProperties = kafkaConsumerProperties;
    }


    @PostConstruct
    public void start(){
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singleton(XpenseKafkaTopics.MAIL_MESSAGE_ID.getName()));

        this.executorService.submit(() -> {
           try{
               while(running){
                   LOGGER.debug("Intentionally left blank");
               }
           }catch (Exception ex){
               LOGGER.error("Error while consuming message : {}", ex.getMessage());
           }finally {
               LOGGER.info("Shutting down MailMessageIdConsumer due to exception. Setting running to false");
               shutdown();
           }
        });
    }

    private void shutdown(){
        this.running = false;
        this.executorService.shutdown();
    }
}
