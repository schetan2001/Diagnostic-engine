package com.re.diagnostic.db;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Set;

/**
 * In-memory cache for tracking open DTC states.
 * Uses Caffeine LoadingCache to safely lazy-load open DTCs and prevent OOM.
 * Key format: "systemId::dtcId"
 */
public class DtcStateCache {

    private static final Logger logger = LogManager.getLogger(DtcStateCache.class);

    private final LoadingCache<String, Boolean> cache;
    private final DtcRepository dtcRepository;

    public DtcStateCache(DtcRepository dtcRepository) {
        this.dtcRepository = dtcRepository;
        this.cache = Caffeine.newBuilder()
                .maximumSize(500_000)
                .expireAfterAccess(Duration.ofHours(24))
                .build(this::loadStateFromDatabase);
    }

    private boolean loadStateFromDatabase(String key) {
        String[] parts = key.split("::");
        String systemId = parts[0];
        Long dtcId = Long.parseLong(parts[1]);
        
        logger.debug("Cache Miss: Querying Postgres for systemId={}, dtcId={}", systemId, dtcId);
        return dtcRepository.existsOpenDtc(dtcId, systemId);
    }

    public boolean isOpen(String systemId, Long dtcId) {
        return Boolean.TRUE.equals(cache.get(buildKey(systemId, dtcId)));
    }

    public void markOpen(String systemId, Long dtcId) {
        cache.put(buildKey(systemId, dtcId), true);
    }

    public void markClosed(String systemId, Long dtcId) {
        cache.put(buildKey(systemId, dtcId), false);
    }

    public long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    /**
     * Pre-warm the cache for a systemId by batch-querying all open DTCs in one DB call.
     * This avoids N individual cache-miss DB round-trips during processing.
     */
    public void preWarm(String systemId, Set<Long> dtcIdsToCheck) {
        boolean anyMissing = false;
        for (Long dtcId : dtcIdsToCheck) {
            if (cache.getIfPresent(buildKey(systemId, dtcId)) == null) {
                anyMissing = true;
                break;
            }
        }

        if (!anyMissing) {
            return;
        }

        try {
            Set<Long> openDtcIds = dtcRepository.findOpenDtcIds(systemId);

            for (Long dtcId : dtcIdsToCheck) {
                String key = buildKey(systemId, dtcId);
                if (cache.getIfPresent(key) == null) {
                    cache.put(key, openDtcIds.contains(dtcId));
                }
            }
            logger.debug("Pre-warmed cache for systemId={}, checked={}, open={}", systemId, dtcIdsToCheck.size(), openDtcIds.size());
        } catch (Exception e) {
            logger.warn("Cache pre-warm failed for systemId={}, falling back to individual lookups", systemId, e);
        }
    }

    private static String buildKey(String systemId, Long dtcId) {
        return systemId + "::" + dtcId;
    }
}
