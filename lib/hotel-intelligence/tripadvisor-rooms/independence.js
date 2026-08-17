/**
 * Source-independence scoring for room-count corroboration.
 * Matching values alone do NOT imply VERIFIED_MULTI_SOURCE.
 */

export const SOURCE_INDEPENDENCE_VERSION = "source-independence-v1";

/** Upstream / distribution clusters that often share inventory facts. */
export const UPSTREAM_CLUSTERS = Object.freeze({
  OTA_CONSUMER: "ota_consumer",
  HBX_CONTENT: "hbx_content",
  OFFICIAL: "official",
  TOURISM_GOV: "tourism_gov",
  PRESS_NEWS: "press_news",
  RESEARCH_WEB: "research_web",
  UNKNOWN: "unknown",
});

const OTA_HOST_RE =
  /\b(tripadvisor|booking\.com|expedia|hotels\.com|agoda|trivago|kayak|google\.(com|co)|maps\.google|travel\.google)\b/i;

/**
 * @param {object} obs
 */
export function assignUpstreamCluster(obs) {
  const provider = String(obs?.provider || obs?.source_provider || "").toLowerCase();
  const url = String(obs?.url || "").toLowerCase();
  const cat = String(obs?.source_category || "");

  if (provider === "hotelbeds" || provider === "hbx") {
    return {
      cluster: UPSTREAM_CLUSTERS.HBX_CONTENT,
      upstream_source_if_known: "hotelbeds_content_api",
      independence_confidence: 0.55,
    };
  }
  if (provider === "tripadvisor" || provider === "tripadvisor_apify" || /tripadvisor\./i.test(url)) {
    return {
      cluster: UPSTREAM_CLUSTERS.OTA_CONSUMER,
      upstream_source_if_known: "tripadvisor",
      independence_confidence: 0.4,
    };
  }
  if (OTA_HOST_RE.test(url) || /Google Hotels|OTA/i.test(cat)) {
    return {
      cluster: UPSTREAM_CLUSTERS.OTA_CONSUMER,
      upstream_source_if_known: "ota_or_google_hotels_uncertain",
      independence_confidence: 0.35,
    };
  }
  if (
    /Official Hotel|Official Brand|Official Owner|Official Operator/i.test(cat) ||
    provider === "official_site"
  ) {
    return {
      cluster: UPSTREAM_CLUSTERS.OFFICIAL,
      upstream_source_if_known: null,
      independence_confidence: 0.95,
    };
  }
  if (/Tourism Authority|Convention Bureau|Destination Marketing/i.test(cat)) {
    return {
      cluster: UPSTREAM_CLUSTERS.TOURISM_GOV,
      upstream_source_if_known: null,
      independence_confidence: 0.85,
    };
  }
  if (/Historic Press Release|News/i.test(cat)) {
    return {
      cluster: UPSTREAM_CLUSTERS.PRESS_NEWS,
      upstream_source_if_known: null,
      independence_confidence: 0.7,
    };
  }
  if (provider === "room_count_research") {
    // Inherit from URL classification when possible
    if (OTA_HOST_RE.test(url)) {
      return {
        cluster: UPSTREAM_CLUSTERS.OTA_CONSUMER,
        upstream_source_if_known: "search_hit_ota",
        independence_confidence: 0.35,
      };
    }
    return {
      cluster: UPSTREAM_CLUSTERS.RESEARCH_WEB,
      upstream_source_if_known: null,
      independence_confidence: 0.6,
    };
  }
  return {
    cluster: UPSTREAM_CLUSTERS.UNKNOWN,
    upstream_source_if_known: null,
    independence_confidence: 0.5,
  };
}

/**
 * Annotate observations with independence metadata.
 * @param {object[]} observations
 */
export function annotateIndependence(observations) {
  return (observations || []).map((o) => {
    const meta = assignUpstreamCluster(o);
    return {
      ...o,
      source_provider: o.source_provider || o.provider || null,
      upstream_source_if_known:
        o.upstream_source_if_known ?? meta.upstream_source_if_known,
      independence_cluster: meta.cluster,
      independence_confidence:
        o.independence_confidence ?? meta.independence_confidence,
    };
  });
}

/**
 * Can two agreeing observations count as independent multi-source verification?
 * @param {object[]} agreeing
 */
export function assessMultiSourceIndependence(agreeing) {
  const rows = annotateIndependence(agreeing);
  const clusters = new Set(rows.map((r) => r.independence_cluster));
  const domains = new Set(
    rows
      .map((r) => {
        try {
          return new URL(r.url).hostname.replace(/^www\./, "");
        } catch {
          return r.source_domain || r.provider || null;
        }
      })
      .filter(Boolean)
  );

  // Never promote on OTA-only agreement
  const onlyOta =
    clusters.size === 1 && clusters.has(UPSTREAM_CLUSTERS.OTA_CONSUMER);
  if (onlyOta) {
    return {
      independent: false,
      status_hint: "SOURCE_INDEPENDENCE_UNCERTAIN",
      reason: "ota_consumer_cluster_only",
      clusters: [...clusters],
      independence_confidence: 0.3,
    };
  }

  // Tripadvisor + Hotelbeds alone → shared-feed risk
  const hasOta = clusters.has(UPSTREAM_CLUSTERS.OTA_CONSUMER);
  const hasHbx = clusters.has(UPSTREAM_CLUSTERS.HBX_CONTENT);
  const hasOfficial = clusters.has(UPSTREAM_CLUSTERS.OFFICIAL);
  const hasTourism = clusters.has(UPSTREAM_CLUSTERS.TOURISM_GOV);
  const hasPress = clusters.has(UPSTREAM_CLUSTERS.PRESS_NEWS);
  const hasResearch = clusters.has(UPSTREAM_CLUSTERS.RESEARCH_WEB);

  if (hasOta && hasHbx && !hasOfficial && !hasTourism && !hasPress) {
    return {
      independent: false,
      status_hint: "SOURCE_INDEPENDENCE_UNCERTAIN",
      reason: "tripadvisor_hotelbeds_possible_shared_upstream",
      clusters: [...clusters],
      independence_confidence: 0.4,
    };
  }

  // Same domain twice ≠ independent
  if (domains.size < 2 && rows.length >= 2 && !hasOfficial) {
    return {
      independent: false,
      status_hint: "SOURCE_INDEPENDENCE_UNCERTAIN",
      reason: "same_domain_or_provider",
      clusters: [...clusters],
      independence_confidence: 0.45,
    };
  }

  // Official + anything else high-trust, or tourism + press, or official alone handled elsewhere
  if (hasOfficial && (hasTourism || hasPress || hasResearch || hasHbx || rows.length >= 1)) {
    // Multi requires *two* independent confirming sources besides Tripadvisor candidate.
    // agreeing[] should already exclude Tripadvisor itself.
    const nonOta = rows.filter((r) => r.independence_cluster !== UPSTREAM_CLUSTERS.OTA_CONSUMER);
    const nonOtaClusters = new Set(nonOta.map((r) => r.independence_cluster));
    if (nonOta.length >= 2 && nonOtaClusters.size >= 2) {
      return {
        independent: true,
        status_hint: "VERIFIED_MULTI_SOURCE",
        reason: "distinct_non_ota_clusters",
        clusters: [...clusters],
        independence_confidence: 0.88,
      };
    }
    if (hasOfficial && nonOta.length >= 2 && domains.size >= 2) {
      return {
        independent: true,
        status_hint: "VERIFIED_MULTI_SOURCE",
        reason: "official_plus_second_domain",
        clusters: [...clusters],
        independence_confidence: 0.85,
      };
    }
  }

  if ((hasTourism || hasPress) && hasResearch && domains.size >= 2) {
    return {
      independent: true,
      status_hint: "VERIFIED_MULTI_SOURCE",
      reason: "tourism_or_press_plus_research",
      clusters: [...clusters],
      independence_confidence: 0.8,
    };
  }

  if (rows.length >= 2 && domains.size >= 2 && !onlyOta) {
    // Ambiguous — do not auto-multi
    return {
      independent: false,
      status_hint: "SOURCE_INDEPENDENCE_UNCERTAIN",
      reason: "agreement_without_clear_independence",
      clusters: [...clusters],
      independence_confidence: 0.5,
    };
  }

  return {
    independent: false,
    status_hint: "CANDIDATE_SINGLE_SOURCE",
    reason: "insufficient_independent_corroboration",
    clusters: [...clusters],
    independence_confidence: 0.55,
  };
}
