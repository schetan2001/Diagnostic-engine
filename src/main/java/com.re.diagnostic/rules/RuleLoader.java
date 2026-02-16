package com.re.diagnostic.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.re.diagnostic.db.PostgresService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

public class RuleLoader {

    private static final Logger logger = LogManager.getLogger(RuleLoader.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final PostgresService postgresService;
    private final RuleCache ruleCache;

    public RuleLoader(PostgresService postgresService, RuleCache ruleCache) {
        this.postgresService = postgresService;
        this.ruleCache = ruleCache;
    }

    public void loadAllRules() {

        String sql = """
                SELECT m.id, m.dtc_code, m.severity, m.description, m.ecu_type,
                       r.rule_conditions, r.version
                FROM dtc_master m
                JOIN dtc_rule_conditions r
                  ON m.id = r.dtc_id
                WHERE r.is_latest = true
                """;

        try (Connection con = postgresService.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            int loadedCount = 0;
            while (rs.next()) {
                try {
                    RuleDefinition rule = mapRule(rs);
                    ruleCache.put(rule);
                    loadedCount++;
                } catch (Exception e) {
                    logger.error("Failed to map rule from DB row, skipping", e);
                }
            }
            logger.info("Loaded {} rules into cache", loadedCount);

        } catch (Exception e) {
            logger.error("Failed to load rules from DB", e);
            throw new RuntimeException("Failed to load rules", e);
        }
    }

    public void reloadRule(String dtcCode) {

        String sql = """
                SELECT m.id, m.dtc_code, m.severity, m.description, m.ecu_type,
                       r.rule_conditions, r.version
                FROM dtc_master m
                JOIN dtc_rule_conditions r
                  ON m.id = r.dtc_id
                WHERE r.is_latest = true
                  AND m.dtc_code = ?
                """;

        try (Connection con = postgresService.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setString(1, dtcCode);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    try {
                        RuleDefinition rule = mapRule(rs);
                        ruleCache.put(rule);
                        logger.info("Reloaded rule into cache: dtcCode={}", dtcCode);
                    } catch (Exception e) {
                        logger.error("Failed to map rule for dtcCode={}, skipping", dtcCode, e);
                    }
                } else {
                    ruleCache.remove(dtcCode);
                    logger.info("Removed rule from cache: dtcCode={}", dtcCode);
                }
            }

        } catch (Exception e) {
            logger.error("Failed to reload rule for dtcCode={}", dtcCode, e);
            throw new RuntimeException("Failed to reload rule for dtcCode=" + dtcCode, e);
        }
    }

    private RuleDefinition mapRule(ResultSet rs) throws Exception {

        String ruleJson = rs.getString("rule_conditions");
        JsonNode ruleNode = mapper.readTree(ruleJson);

        Set<Long> propertyIds = new HashSet<>();
        extractPropertyIds(ruleNode, propertyIds);

        return new RuleDefinition(
                rs.getLong("id"),
                rs.getString("dtc_code"),
                rs.getString("severity"),
                rs.getString("description"),
                rs.getString("ecu_type"),
                rs.getInt("version"),
                propertyIds,
                ruleJson,
                ruleNode);
    }

    private void extractPropertyIds(JsonNode node, Set<Long> propertyIds) {

        if (node == null || node.isNull()) {
            return;
        }

        if (node.has("conditions") && node.get("conditions").isArray()) {
            for (JsonNode condition : node.get("conditions")) {
                extractPropertyIds(condition, propertyIds);
            }
            return;
        }

        if (node.has("parameter") && node.get("parameter").isTextual()) {
            try {
                long propertyId = Long.parseLong(node.get("parameter").asText());
                propertyIds.add(propertyId);
            } catch (NumberFormatException e) {
                logger.warn("Invalid parameter id: {}", node.get("parameter").asText());
            }
        }
    }

}
