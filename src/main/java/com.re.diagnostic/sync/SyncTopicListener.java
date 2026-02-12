package com.re.diagnostic.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.re.diagnostic.rules.RuleCache;
import com.re.diagnostic.rules.RuleLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SyncTopicListener implements Runnable {

    private static final Logger logger = LogManager.getLogger(SyncTopicListener.class);
    private final KafkaConsumer<String, String> consumer;
    private final RuleLoader ruleLoader;
    private final RuleCache ruleCache;
    private final ObjectMapper mapper = new ObjectMapper();

    public SyncTopicListener(Properties props, String syncTopic, RuleLoader ruleLoader, RuleCache ruleCache) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(syncTopic));
        this.ruleLoader = ruleLoader;
        this.ruleCache = ruleCache;

    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    JsonNode msg = mapper.readTree(record.value());
                    logger.info("Rule sync record received | topic={} | partition={} | offset={}",
                            record.topic(), record.partition(), record.offset());

                    logger.info("Rule sync payload: {}", msg.toString());
                    String action = msg.get("action").asText();
                    JsonNode dtcCodes = msg.get("dtc_code");
                    logger.info("Processing rule sync action={} dtcCodes={}", action, dtcCodes);
                    for (JsonNode codeNode : dtcCodes) {
                        String dtcCode = codeNode.asText();
                        if ("add".equalsIgnoreCase(action) || "update".equalsIgnoreCase(action)) {
                            ruleLoader.reloadRule(dtcCode);
                            logger.info("Rule added/updated successfully | dtcCode={}", dtcCode);
                        } else if ("delete".equalsIgnoreCase(action)) {
                            ruleCache.remove(dtcCode);
                            logger.info("Rule removed successfully | dtcCode={}", dtcCode);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error processing rule sync record | value={}", record.value(), e);
                }
            }
        }
    }
}