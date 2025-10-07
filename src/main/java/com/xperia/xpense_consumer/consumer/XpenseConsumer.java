package com.xperia.xpense_consumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xperia.xpense_consumer.models.MutualFundSchemeConsumerModel;
import com.xperia.xpense_consumer.models.entity.MutualFundScheme;
import com.xperia.xpense_consumer.service.MutualFundSchemeService;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class XpenseConsumer {

    @Autowired
    private MutualFundSchemeService mutualFundSchemeService;

    private static final Logger LOGGER = LoggerFactory.getLogger(XpenseConsumer.class);
    private final Properties consumerProperties;
    private KafkaConsumer<String, String> consumer;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;
    private final ObjectMapper objectMapper;
    private final static long BATCH_SIZE = 1000;


    public XpenseConsumer(Properties kafkaConsumerProperties){
        this.consumerProperties = kafkaConsumerProperties;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void start(){
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList("mf_scheme"));
        List<MutualFundScheme> list = new ArrayList<>();
        executorService.submit(() -> {
            try{
                while (running){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records){
                        try {
                            String json = record.value();
                            MutualFundSchemeConsumerModel model =
                                    objectMapper.readValue(json, MutualFundSchemeConsumerModel.class);
                            MutualFundScheme mfScheme = new MutualFundScheme(model.getSchemeCode(), model.getSchemeName(),
                                    model.getIsinGrowth(), model.getIsinDivReinvestment());
                            list.add(mfScheme);
                            if (list.size() >= BATCH_SIZE){
                                mutualFundSchemeService.saveAllSchemes(list);
                                list.clear();
                            }
                        } catch (Exception e) {
                            LOGGER.error("Failed to parse record : {}", record.value(), e);
                        }
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
