package com.re.diagnostic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.re.diagnostic.db.DtcRepository;
import com.re.diagnostic.db.DtcStateCache;
import com.re.diagnostic.rules.MatchedProperty;
import com.re.diagnostic.rules.RuleEvaluator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DiagnosticEngineService {

    private static final Logger logger = LogManager.getLogger(DiagnosticEngineService.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final RuleEvaluator ruleEvaluator;
    private final DtcRepository dtcRepository;
    private final DtcStateCache dtcStateCache;
    private final KafkaProducer<String, String> producer;
    private final String outputTopic;
    private final String notificationOutputTopic;

    private static final String SYSTEM_ID = "system_id";

    public DiagnosticEngineService(RuleEvaluator ruleEvaluator, DtcRepository dtcRepository,
            DtcStateCache dtcStateCache,
            KafkaProducer<String, String> producer, String outputTopic, String notificationOutputTopic) {
        this.ruleEvaluator = ruleEvaluator;
        this.dtcRepository = dtcRepository;
        this.dtcStateCache = dtcStateCache;
        this.producer = producer;
        this.outputTopic = outputTopic;
        this.notificationOutputTopic = notificationOutputTopic;
    }

    public void process(String telemetryJson) {
        long startNs = System.nanoTime();
        logger.debug("Processing message start");

        try {
            JsonNode telemetryRoot;
            try {
                telemetryRoot = mapper.readTree(telemetryJson);
            } catch (JsonProcessingException e) {
                logger.error("Invalid telemetry JSON, skipping: {}", telemetryJson, e);
                return;
            }

            String systemId = extractMetadataValues(telemetryRoot, SYSTEM_ID);
            if (systemId == null) {
                logger.warn("Missing systemId in telemetry, skipping: {}", telemetryJson);
                return;
            }

            List<MatchedProperty> evaluationResults;
            try {
                evaluationResults = ruleEvaluator.evaluate(telemetryRoot);
            } catch (Exception e) {
                logger.error("Rule evaluation failed for systemId={}, skipping message", systemId, e);
                return;
            }

            if (evaluationResults.isEmpty()) {
                logger.debug("No rules matched for systemId={}", systemId);
            } else {
                logger.info("Matched {} rules for systemId={}", evaluationResults.size(), systemId);
            }

            // Pre-warm cache: 1 batch SQL query instead of N individual cache-miss queries
            if (!evaluationResults.isEmpty()) {
                Set<Long> dtcIdsToCheck = new HashSet<>();
                for (MatchedProperty result : evaluationResults) {
                    dtcIdsToCheck.add(result.getDtcId());
                }
                dtcStateCache.preWarm(systemId, dtcIdsToCheck);
            }

            for (MatchedProperty result : evaluationResults) {
                Long dtcId = result.getDtcId();
                String dtcCode = result.getDtcCode();
                boolean matched = result.isMatched();
                try {
                    boolean openExists = dtcStateCache.isOpen(systemId, dtcId);

                    // RULE MATCHED → OPEN
                    if (matched && !openExists) {
                        logger.info("Opening DTC | dtcCode={} | systemId={} | severity={} | version={}",
                                dtcCode, systemId, result.getSeverity(), result.getVersion());

                        dtcRepository.saveOccurrence(dtcId, dtcCode, systemId, result.getSeverity(), "OPEN",
                                result.getVersion(), result.getEcuType(), telemetryJson);
                        dtcStateCache.markOpen(systemId, dtcId);
                        publishDtcEvent(result, systemId, "OPEN");
                    }
                    // RULE NOT MATCHED → CLOSE
                    else if (!matched && openExists) {
                        logger.info("Closing DTC | dtcCode={} | systemId={}", dtcCode, systemId);

                        dtcRepository.closeOpenOccurrence(dtcId, systemId);
                        dtcStateCache.markClosed(systemId, dtcId);
                        publishDtcEvent(result, systemId, "CLOSED");
                    }
                    // No state change
                    else {
                        logger.debug("No DTC state change | dtcCode={} | systemId={} | matched={} | openExists={}",
                                dtcCode, systemId, matched, openExists);
                    }
                } catch (Exception e) {
                    logger.error("Failed processing DTC lifecycle | dtcCode={} | systemId={}", dtcCode, systemId, e);
                }
            }
        } finally {
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            logger.debug("Processing message end. Time taken: {} ms", elapsedMs);
        }
    }

    private void publishDtcEvent(MatchedProperty result, String systemId, String status) {

        ObjectNode payload = mapper.createObjectNode();
        payload.put("dtcId", result.getDtcId());
        payload.put("dtcCode", result.getDtcCode());
        payload.put("description", result.getDescription());
        payload.put("systemId", systemId);
        payload.put("severity", result.getSeverity());
        payload.put("status", status);
        payload.put("ruleVersion", result.getVersion());
		payload.put("ecuType", result.getEcuType());

        String now = Instant.now().toString();
        payload.put("eventTime", now);

        if ("CLOSED".equals(status)) {
            payload.put("clearedAt", now);
        }

        try {
            String message = mapper.writeValueAsString(payload);

            producer.send(new ProducerRecord<>(outputTopic, systemId, message), (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Kafka publish failed | dtcCode={} | status={}", result.getDtcCode(), status,
                            exception);
                } else {
                    logger.info("DTC event published | dtcCode={} | status={} | offset={}", result.getDtcCode(), status,
                            metadata.offset());
                }
            });

            publishAlertEvent(result, systemId, status);

        } catch (Exception e) {
            logger.error("Failed to publish DTC event | dtcCode={}", result.getDtcCode(), e);
        }
    }

    private void publishAlertEvent(MatchedProperty result, String systemId, String status) {
        long currentTimestamp = System.currentTimeMillis();

        ObjectNode rootNode = mapper.createObjectNode();

        ObjectNode bodyNode = mapper.createObjectNode();
        ObjectNode metaNode = mapper.createObjectNode();
        metaNode.put("guid", "NA");
        metaNode.put("system_id", systemId);
        bodyNode.set("meta", metaNode);

        ArrayNode dataNode = mapper.createArrayNode();

        // message type
        ObjectNode messageNode = mapper.createObjectNode();
        messageNode.put("type", "message");
        ArrayNode messageAttributes = mapper.createArrayNode();

        ObjectNode dtcCodeAttribute = mapper.createObjectNode();
        dtcCodeAttribute.put("key", "dtcCode");
        dtcCodeAttribute.put("datatype", "string");
        dtcCodeAttribute.put("unit", "NA");
        ArrayNode dtcCodeValue = mapper.createArrayNode();
        dtcCodeValue.add(result.getDtcCode());
        dtcCodeAttribute.set("value", dtcCodeValue);
        messageAttributes.add(dtcCodeAttribute);

        ObjectNode descriptionAttribute = mapper.createObjectNode();
        descriptionAttribute.put("key", "description");
        descriptionAttribute.put("datatype", "string");
        descriptionAttribute.put("unit", "NA");
        ArrayNode descriptionValue = mapper.createArrayNode();
        descriptionValue.add(result.getDescription());
        descriptionAttribute.set("value", descriptionValue);
        messageAttributes.add(descriptionAttribute);

        ObjectNode ecuTypeAttribute = mapper.createObjectNode();
        ecuTypeAttribute.put("key", "ecuType");
        ecuTypeAttribute.put("datatype", "string");
        ecuTypeAttribute.put("unit", "NA");
        ArrayNode ecuTypeValue = mapper.createArrayNode();
        ecuTypeAttribute.set("value", ecuTypeValue);
        messageAttributes.add(ecuTypeAttribute);

        messageNode.set("attributes", messageAttributes);
        dataNode.add(messageNode);

        // observability type
        ObjectNode observabilityNode = mapper.createObjectNode();
        observabilityNode.put("type", "observability");
        ArrayNode observabilityAttributes = mapper.createArrayNode();

        ObjectNode eventTimeAttribute = mapper.createObjectNode();
        eventTimeAttribute.put("key", "eventTime");
        eventTimeAttribute.put("datatype", "long");
        eventTimeAttribute.put("unit", "epochmilliseconds");
        eventTimeAttribute.put("value", currentTimestamp);
        observabilityAttributes.add(eventTimeAttribute);

        if ("CLOSED".equals(status)) {
            ObjectNode clearedAtAttribute = mapper.createObjectNode();
            clearedAtAttribute.put("key", "clearedAt");
            clearedAtAttribute.put("datatype", "long");
            clearedAtAttribute.put("unit", "epochmilliseconds");
            clearedAtAttribute.put("value", currentTimestamp);
            observabilityAttributes.add(clearedAtAttribute);
        }

        observabilityNode.set("attributes", observabilityAttributes);
        dataNode.add(observabilityNode);

        // channel type
        ObjectNode channelNode = mapper.createObjectNode();
        channelNode.put("type", "channel");
        ArrayNode channelAttributes = mapper.createArrayNode();

        ObjectNode modeAttribute = mapper.createObjectNode();
        modeAttribute.put("key", "mode");
        modeAttribute.put("datatype", "array");
        modeAttribute.put("unit", "NA");
        ArrayNode modeValue = mapper.createArrayNode();
        modeValue.add("EMAIL");
        modeValue.add("PUSH");
        modeAttribute.set("value", modeValue);
        channelAttributes.add(modeAttribute);

        channelNode.set("attributes", channelAttributes);
        dataNode.add(channelNode);

        // severity type
        ObjectNode severityNode = mapper.createObjectNode();
        severityNode.put("type", "severity");
        ArrayNode severityAttributes = mapper.createArrayNode();

        ObjectNode severityAttribute = mapper.createObjectNode();
        severityAttribute.put("key", "severity");
        severityAttribute.put("datatype", "String"); // Based on example "String"
        severityAttribute.put("unit", "NA");
        severityAttribute.put("value", result.getSeverity());
        severityAttributes.add(severityAttribute);

        severityNode.set("attributes", severityAttributes);
        dataNode.add(severityNode);

        bodyNode.set("data", dataNode);
        rootNode.set("body", bodyNode);

        rootNode.put("timestamp", currentTimestamp);
        rootNode.put("version", "");
        rootNode.put("event_category", "NOTIFICATION");
        rootNode.put("event_type", "DTC_ALERT");
        rootNode.put("encoding_type", 0);
        rootNode.put("correlation_id", "");
        rootNode.put("event_id", "");

        try {
            String alertMessage = mapper.writeValueAsString(rootNode);

            producer.send(new ProducerRecord<>(notificationOutputTopic, systemId, alertMessage),
                    (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Kafka publish to notification topic failed | dtcCode={} | status={}",
                                    result.getDtcCode(), status,
                                    exception);
                        } else {
                            logger.info("Alert event published | dtcCode={} | status={} | offset={}",
                                    result.getDtcCode(), status,
                                    metadata.offset());
                        }
                    });

        } catch (Exception e) {
            logger.error("Failed to publish Alert event | dtcCode={}", result.getDtcCode(), e);
        }
    }

    private static String extractMetadataValues(JsonNode root, String field) {
        return root.path("meta").path(field).asText(null);
    }

}
