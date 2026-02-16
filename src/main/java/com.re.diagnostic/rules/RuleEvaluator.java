package com.re.diagnostic.rules;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

public class RuleEvaluator {

    private static final Logger logger = LogManager.getLogger(RuleEvaluator.class);
    private final RuleCache ruleCache;

    public RuleEvaluator(RuleCache ruleCache) {
        this.ruleCache = ruleCache;
    }

    public List<MatchedProperty> evaluate(JsonNode telemetryRoot) {

        List<MatchedProperty> results = new ArrayList<>();

        try {
            Map<Long, Double> telemetryValues = extractTelemetryValues(telemetryRoot);

            if (telemetryValues.isEmpty()) {
                return results;
            }
            Set<String> relevantDtcCodes = new HashSet<>();
            for (Long propertyId : telemetryValues.keySet()) {
                relevantDtcCodes.addAll(ruleCache.getDtcsByPropertyId(propertyId));
            }
            for (String dtcCode : relevantDtcCodes) {

                RuleDefinition rule = ruleCache.getByDtc(dtcCode);
                if (rule == null)
                    continue;

                boolean matched = false;
                try {
                    matched = evaluateRuleNode(rule.getRuleNode(), telemetryValues);
                } catch (Exception e) {
                    logger.error("Rule evaluation failed for dtcCode={}", dtcCode, e);
                }
                results.add(new MatchedProperty(
                        rule.getDtcId(),
                        rule.getDtcCode(),
                        rule.getDescription(),
                        matched,
                        rule.getSeverity(),
                        rule.getEcuType(),
                        rule.getVersion()));
            }

            return results;

        } catch (Exception e) {
            logger.error("Rule evaluation failed for telemetry: {}", telemetryRoot.toString(), e);
            throw new RuntimeException("Rule evaluation failed", e);
        }
    }

    private boolean evaluateRuleNode(JsonNode node, Map<Long, Double> telemetry) {

        if (node.has("logic") && node.has("conditions")) {

            String logic = node.get("logic").asText().toUpperCase();
            JsonNode conditions = node.get("conditions");

            if ("AND".equals(logic)) {
                for (JsonNode condition : conditions) {
                    if (!evaluateRuleNode(condition, telemetry)) {
                        return false;
                    }
                }
                return true;
            }

            if ("OR".equals(logic)) {
                for (JsonNode condition : conditions) {
                    if (evaluateRuleNode(condition, telemetry)) {
                        return true;
                    }
                }
                return false;
            }
        }

        return evaluateCondition(node, telemetry);
    }

    private boolean evaluateCondition(JsonNode condition, Map<Long, Double> telemetry) {

        String parameter = condition.get("parameter").asText();
        String operator = condition.get("operator").asText();
        double expectedValue = condition.get("value").asDouble();

        Long propertyId = Long.parseLong(parameter);
        if (propertyId == null) {
            return false;
        }

        Double actualValue = telemetry.get(propertyId);
        if (actualValue == null) {
            return false;
        }

        return compareNumbers(actualValue, expectedValue, operator);
    }

    private Map<Long, Double> extractTelemetryValues(JsonNode root) {

        Map<Long, Double> values = new HashMap<>();

        JsonNode telemetryArray = root.path("telemetry");
        if (!telemetryArray.isArray()) {
            return values;
        }

        for (JsonNode telemetry : telemetryArray) {
            JsonNode dataArray = telemetry.path("data");
            if (!dataArray.isArray())
                continue;

            for (JsonNode dataNode : dataArray) {
                long propertyId = dataNode.path("id").asLong();
                JsonNode valueNode = dataNode.path("value");

                if (valueNode.isArray() && !valueNode.isEmpty()) {
                    values.put(propertyId, valueNode.get(0).asDouble());
                }
            }
        }
        return values;
    }

    private boolean compareNumbers(double actual, double expected, String operator) {
        return switch (operator) {
            case ">" -> actual > expected;
            case "<" -> actual < expected;
            case ">=" -> actual >= expected;
            case "<=" -> actual <= expected;
            case "==" -> actual == expected;
            case "!=" -> actual != expected;
            default -> false;
        };
    }

}
