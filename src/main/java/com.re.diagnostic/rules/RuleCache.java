package com.re.diagnostic.rules;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RuleCache {

    private final Map<String, RuleDefinition> rulesByDtc = new ConcurrentHashMap<>();
    private final Map<Long, Set<String>> dtcsByPropertyId = new ConcurrentHashMap<>();

    public synchronized void put(RuleDefinition rule) {

        RuleDefinition existing = rulesByDtc.get(rule.getDtcCode());
        if (existing != null && existing.getPropertyIds() != null) {
            for (Long propertyId : existing.getPropertyIds()) {
                Set<String> dtcs = dtcsByPropertyId.get(propertyId);
                if (dtcs != null) {
                    dtcs.remove(rule.getDtcCode());
                    if (dtcs.isEmpty()) {
                        dtcsByPropertyId.remove(propertyId);
                    }
                }
            }
        }

        rulesByDtc.put(rule.getDtcCode(), rule);

        if (rule.getPropertyIds() != null) {
            for (Long propertyId : rule.getPropertyIds()) {
                dtcsByPropertyId
                        .computeIfAbsent(propertyId, k -> ConcurrentHashMap.newKeySet())
                        .add(rule.getDtcCode());
            }
        }
    }

    public RuleDefinition getByDtc(String dtcCode) {
        return rulesByDtc.get(dtcCode);
    }

    public synchronized void remove(String dtcCode) {
        RuleDefinition rule = rulesByDtc.remove(dtcCode);
        if (rule != null && rule.getPropertyIds() != null) {
            for (Long propertyId : rule.getPropertyIds()) {
                Set<String> dtcs = dtcsByPropertyId.get(propertyId);
                if (dtcs != null) {
                    dtcs.remove(dtcCode);
                    if (dtcs.isEmpty()) {
                        dtcsByPropertyId.remove(propertyId);
                    }
                }
            }
        }
    }

    public boolean contains(String dtcCode) {
        return rulesByDtc.containsKey(dtcCode);
    }

    public int size() {
        return rulesByDtc.size();
    }

    public Collection<RuleDefinition> getAll() {
        return rulesByDtc.values();
    }

    public Set<String> getDtcsByPropertyId(long propertyId) {
        return dtcsByPropertyId.getOrDefault(propertyId, Set.of());
    }
}
