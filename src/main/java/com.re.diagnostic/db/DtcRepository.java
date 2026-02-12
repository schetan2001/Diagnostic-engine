package com.re.diagnostic.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DtcRepository {

    private final PostgresService postgresService;

    public DtcRepository(PostgresService postgresService) {
        this.postgresService = postgresService;
    }

    public boolean existsOpenDtc(Long dtcId, String systemId) {

        String sql = """
                    SELECT 1
                    FROM dtc_occurrences
                    WHERE dtc_id = ?
                      AND system_id = ?
                      AND status = 'OPEN'
                    LIMIT 1
                """;

        try (Connection con = postgresService.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setLong(1, dtcId);
            ps.setString(2, systemId);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to check open DTC", e);
        }
    }

    public void saveOccurrence(Long dtcId, String dtcCode, String systemId, String severity, String status,
                               Integer ruleVersion, String canDataJson) {

        String sql = """
                    INSERT INTO dtc_occurrences
                    (dtc_id, dtc_code, system_id, severity,
                     status, rule_version, first_triggered_at,
                     last_triggered_at, can_data, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW(), ?::jsonb, 1)
                """;

        try (Connection con = postgresService.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setLong(1, dtcId);
            ps.setString(2, dtcCode);
            ps.setString(3, systemId);
            ps.setString(4, severity);
            ps.setString(5, status);
            ps.setInt(6, ruleVersion);
            ps.setString(7, canDataJson);

            ps.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to save DTC occurrence", e);
        }
    }

    public void closeOpenOccurrence(Long dtcId, String systemId) {

        String sql = """
                    UPDATE dtc_occurrences
                    SET status = 'CLOSED',
                        cleared_at = NOW()
                    WHERE dtc_id = ?
                      AND system_id = ?
                      AND status = 'OPEN'
                """;

        try (Connection con = postgresService.getConnection(); PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setLong(1, dtcId);
            ps.setString(2, systemId);

            ps.executeUpdate();

        } catch (Exception e) {
            throw new RuntimeException("Failed to close DTC occurrence", e);
        }
    }

}
