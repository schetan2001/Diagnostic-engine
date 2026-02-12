package com.re.diagnostic.rules;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import java.util.Set;

@Getter
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class RuleDefinition {

    @EqualsAndHashCode.Include
    @ToString.Include
    private final Long dtcId;

    @EqualsAndHashCode.Include
    @ToString.Include
    private final Integer version;

    private final String dtcCode;
    private final String severity;
    private final String description;
    private final Set<Long> propertyIds;
    private final String ruleConditionsJson;
    private final JsonNode ruleNode;


    public RuleDefinition(Long dtcId, String dtcCode, String severity,String description, Integer version, Set<Long> propertyIds,
                          String ruleConditionsJson, JsonNode ruleNode) {

        if (dtcId == null) throw new IllegalArgumentException("dtcId cannot be null");
        if (dtcCode == null || dtcCode.isBlank()) throw new IllegalArgumentException("dtcCode cannot be null or blank");
        if (version == null || version <= 0) throw new IllegalArgumentException("version must be > 0");

        this.dtcId = dtcId;
        this.dtcCode = dtcCode;
        this.severity = severity;
        this.description=description;
        this.version = version;
        this.propertyIds = propertyIds == null ? Set.of() : Set.copyOf(propertyIds);
        this.ruleConditionsJson = ruleConditionsJson;
        this.ruleNode = ruleNode;
    }

}
