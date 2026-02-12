package com.re.diagnostic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.re.diagnostic.db.DtcRepository;
import com.re.diagnostic.rules.MatchedProperty;
import com.re.diagnostic.rules.RuleEvaluator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.time.Instant;
import java.util.List;

public class DiagnosticEngineService {

    private static final Logger logger = LogManager.getLogger(DiagnosticEngineService.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final RuleEvaluator ruleEvaluator;
    private final DtcRepository dtcRepository;
    private final KafkaProducer<String, String> producer;
    private final String outputTopic;

    private static final String SYSTEM_ID = "system_id";

    public DiagnosticEngineService(RuleEvaluator ruleEvaluator, DtcRepository dtcRepository,
                                   KafkaProducer<String, String> producer, String outputTopic) {
        this.ruleEvaluator = ruleEvaluator;
        this.dtcRepository = dtcRepository;
        this.producer = producer;
        this.outputTopic = outputTopic;
    }

    public void process(String telemetryJson) {

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

        for (MatchedProperty result : evaluationResults) {
            Long dtcId = result.getDtcId();
            String dtcCode = result.getDtcCode();
            boolean matched = result.isMatched();
            try {
                boolean openExists = dtcRepository.existsOpenDtc(dtcId, systemId);

                // RULE MATCHED → OPEN
                if (matched && !openExists) {
                    logger.info("Opening DTC | dtcCode={} | systemId={} | severity={} | version={}",
                            dtcCode, systemId, result.getSeverity(), result.getVersion());

                    dtcRepository.saveOccurrence(dtcId, dtcCode, systemId, result.getSeverity(), "OPEN",
                            result.getVersion(), telemetryJson);
                    publishDtcEvent(result, systemId, "OPEN");
                }
                // RULE NOT MATCHED → CLOSE
                else if (!matched && openExists) {
                    logger.info("Closing DTC | dtcCode={} | systemId={}", dtcCode, systemId);

                    dtcRepository.closeOpenOccurrence(dtcId, systemId);
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

        String now = Instant.now().toString();
        payload.put("eventTime", now);

        if ("CLOSED".equals(status)) {
            payload.put("clearedAt", now);
        }

        try {
            String message = mapper.writeValueAsString(payload);

            producer.send(new ProducerRecord<>(outputTopic, systemId, message), (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Kafka publish failed | dtcCode={} | status={}", result.getDtcCode(), status, exception);
                } else {
                    logger.info("DTC event published | dtcCode={} | status={} | offset={}", result.getDtcCode(), status, metadata.offset());
                }
            });

        } catch (Exception e) {
            logger.error("Failed to publish DTC event | dtcCode={}", result.getDtcCode(), e);
        }
    }

    private static String extractMetadataValues(JsonNode root, String field) {
        return root.path("meta").path(field).asText(null);
    }

}
