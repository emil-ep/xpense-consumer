package com.xperia.xpense_consumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;
import org.xperia.entities.mf.MutualFundSchemeDetail;
import org.xperia.models.MutualFundDetailModel;
import org.xperia.service.MutualFundSchemeDetailService;
import org.xperia.util.MutualFundUtil;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MutualFundDetailConsumer implements Runnable{

    private final MutualFundSchemeDetailService schemeDetailService;
    private final KafkaConsumer<String, String> consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(MutualFundDetailConsumer.class);
    private static final List<String> TOPICS_TO_CONSUME = Arrays.asList("scheme_detail");
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final List<MutualFundSchemeDetail> schemeDetails;
    private static final int SCHEME_SAVE_THRESHOLD = 500;
    private static final long FLUSH_INTERVAL_MS = 20000;
    private String url;

    public MutualFundDetailConsumer(Properties kafkaConsumerProperties, RestTemplate restTemplate,
                                    MutualFundSchemeDetailService schemeDetailService, String url){
        this.url = url;
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.restTemplate = restTemplate;
        this.objectMapper = new ObjectMapper();
        this.schemeDetailService = schemeDetailService;
        this.schemeDetails = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::flushAfterTTL, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        consumer.subscribe(TOPICS_TO_CONSUME);
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record: records){
                    String schemeCode = record.value();
                    String url = this.url + schemeCode;
                    MutualFundDetailModel response = restTemplate.getForObject(url, MutualFundDetailModel.class);
                    Map<String, Double> growthMap = MutualFundUtil.findGrowth(response.getData());
                    MutualFundSchemeDetail fundSchemeDetail = new MutualFundSchemeDetail(response.getMeta().getSchemeCode(),
                            response.getMeta().getSchemeType(),
                            response.getMeta().getSchemeCategory(),
                            response.getMeta().getFundHouse(),
                            response.getMeta().getSchemeName(),
                            this.objectMapper.valueToTree(response.getData()),
                            growthMap.getOrDefault("growth", 0.0),
                            growthMap.getOrDefault("growthPercent", 0.0)
                    );
                    createSchemeDetailBatch(fundSchemeDetail);
                    LOGGER.info("Received scheme : {}", schemeCode);
                }
            }
        }catch (Exception ex){
            LOGGER.error("Error while consuming record from : {}",
                    TOPICS_TO_CONSUME
                            .stream()
                            .reduce((topic1, topic2) -> topic1.concat(",")));
        }
    }

    private void createSchemeDetailBatch(MutualFundSchemeDetail schemeDetail){
        synchronized (schemeDetails){
            this.schemeDetails.add(schemeDetail);
            if (this.schemeDetails.size() >=  SCHEME_SAVE_THRESHOLD){
                flush();
            }
        }
    }

    private synchronized void flush() {
        if (this.schemeDetails.isEmpty()) return;

        List<MutualFundSchemeDetail> schemeToSave = new ArrayList<>(schemeDetails);
        schemeDetails.clear();

        try{
            this.schemeDetailService.saveAll(schemeToSave);
            LOGGER.info("saved a batch of schemeDetails, size : {}", schemeToSave.size());
        } catch (Exception ex){
            LOGGER.error("Failed to save a batch of schemeDetails ", ex);
        }
    }

    private synchronized void flushAfterTTL(){
        synchronized (this.schemeDetails){
            if (!this.schemeDetails.isEmpty()){
                flush();
            }
        }
    }
}
