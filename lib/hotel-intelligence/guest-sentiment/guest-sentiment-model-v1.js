/**
 * Guest Sentiment & Reputation — canonical hotel observation model (v1).
 * Does not average incompatible platform scales.
 * Does not fabricate trends from a single snapshot.
 */

import {
  GUEST_THEME_TAXONOMY_V1,
  GUEST_THEME_BY_ID,
  mapPlatformCategoryToTheme,
  MIN_SAMPLE_FOR_PERCENT,
} from "./guest-theme-taxonomy-v1.js";

export const GUEST_SENTIMENT_MODEL_V1 = "guest-sentiment-model-v1";

export const SENTIMENT_PLATFORMS = Object.freeze({
  GOOGLE: { id: "GOOGLE", label: "Google Reviews", scaleMax: 5 },
  TRIPADVISOR: { id: "TRIPADVISOR", label: "Tripadvisor", scaleMax: 5 },
  BOOKING: { id: "BOOKING", label: "Booking.com", scaleMax: 10 },
  EXPEDIA: { id: "EXPEDIA", label: "Expedia", scaleMax: 10 },
  HOTELS_COM: { id: "HOTELS_COM", label: "Hotels.com", scaleMax: 10 },
  AGODA: { id: "AGODA", label: "Agoda", scaleMax: 10 },
  TRIVAGO: { id: "TRIVAGO", label: "Trivago", scaleMax: 10 },
});

/**
 * Canonical seven-platform reputation overview (HE-QL-029).
 * Populated platforms render native scores; others stay visible as not connected /
 * data unavailable — never invent 0 ratings or review counts.
 * Google / Hotels.com / Agoda / Trivago are catalog candidates until source access
 * and rights are confirmed — do not present as integrated feeds.
 */
export const REPUTATION_OVERVIEW_PLATFORMS_V1 = Object.freeze([
  { id: "TRIPADVISOR", label: "Tripadvisor", scaleMax: 5, integration: "SUPPORTED" },
  { id: "BOOKING", label: "Booking.com", scaleMax: 10, integration: "SUPPORTED" },
  { id: "EXPEDIA", label: "Expedia", scaleMax: 10, integration: "SUPPORTED" },
  { id: "GOOGLE", label: "Google Reviews", scaleMax: 5, integration: "CATALOG_CANDIDATE" },
  { id: "HOTELS_COM", label: "Hotels.com", scaleMax: 10, integration: "CATALOG_CANDIDATE" },
  { id: "AGODA", label: "Agoda", scaleMax: 10, integration: "CATALOG_CANDIDATE" },
  { id: "TRIVAGO", label: "Trivago", scaleMax: 10, integration: "CATALOG_CANDIDATE" },
]);

export function buildReputationPlatformOverview(availablePlatforms = []) {
  const byId = new Map(
    (availablePlatforms || []).map((p) => [String(p.platform || "").toUpperCase(), p])
  );
  return REPUTATION_OVERVIEW_PLATFORMS_V1.map((meta) => {
    const hit = byId.get(meta.id);
    if (hit && (hit.rating != null || hit.listingUrl)) {
      return {
        ...hit,
        platform: meta.id,
        platformLabel: hit.platformLabel || meta.label,
        available: true,
        status: "AVAILABLE",
        statusLabel: null,
        integration: meta.integration,
      };
    }
    const notConnected = meta.integration === "CATALOG_CANDIDATE";
    return {
      platform: meta.id,
      platformLabel: meta.label,
      scaleMax: meta.scaleMax,
      available: false,
      status: notConnected ? "NOT_CONNECTED" : "DATA_UNAVAILABLE",
      statusLabel: notConnected ? "Not connected" : "Data unavailable",
      integration: meta.integration,
      rating: null,
      ratingDisplay: null,
      reviewCount: null,
      observedAt: null,
      listingUrl: null,
      categoryScores: [],
      rightsStatus: null,
    };
  });
}

export const RIGHTS_STATUS = Object.freeze({
  RIGHTS_REVIEW_REQUIRED: "RIGHTS_REVIEW_REQUIRED",
  LICENSED_OK: "LICENSED_OK",
  PUBLIC_METADATA_ONLY: "PUBLIC_METADATA_ONLY",
  FORBIDDEN: "FORBIDDEN",
});

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

/**
 * Normalize one platform observation.
 */
export function normalizePlatformObservation(raw = {}) {
  const platformKey = String(raw.platform || "").toUpperCase();
  const meta = SENTIMENT_PLATFORMS[platformKey] || {
    id: platformKey || "UNKNOWN",
    label: text(raw.platformLabel) || platformKey || "Unknown",
    scaleMax: num(raw.scaleMax) || null,
  };
  const rating = num(raw.rating);
  const reviewCount = num(raw.reviewCount);
  const scaleMax = num(raw.scaleMax) || meta.scaleMax;
  const categoryScores = Array.isArray(raw.categoryScores)
    ? raw.categoryScores
        .map((c) => ({
          label: text(c.label) || "Category",
          score: num(c.score),
          scaleMax: num(c.scaleMax) || scaleMax,
          themeId: c.themeId || mapPlatformCategoryToTheme(c.label),
        }))
        .filter((c) => c.score != null)
    : [];

  return {
    platform: meta.id,
    platformLabel: meta.label,
    platformPropertyId: text(raw.platformPropertyId),
    listingUrl: text(raw.listingUrl),
    rating,
    scaleMax,
    reviewCount,
    observedAt: text(raw.observedAt) || null,
    samplePeriod: text(raw.samplePeriod) || null,
    collectionMethod: text(raw.collectionMethod) || null,
    rightsStatus: text(raw.rightsStatus) || RIGHTS_STATUS.RIGHTS_REVIEW_REQUIRED,
    provenance: text(raw.provenance) || null,
    categoryScores,
    languageNotes: text(raw.languageNotes) || null,
  };
}

/**
 * Build HE presentation packet from staged/Airtable observations.
 * Never invents ratings. Never averages across platforms.
 */
export function buildGuestSentimentView(hotel, packet = {}, opts = {}) {
  const hotelId =
    text(hotel?.id) || text(hotel?.recordId) || text(packet.hpcId) || null;
  const hotelName = text(hotel?.name) || text(packet.hotelName) || "Hotel";
  const liveOk = packet.live_ui_authorized === true;
  // Packet flag alone must never open live customer UI — caller must opt in.
  const founderPreview =
    opts.founderPreview === true && packet.founder_preview_render === true;
  const observations = (packet.observations || [])
    .map(normalizePlatformObservation)
    .filter((o) => o.platform && (o.rating != null || o.listingUrl));

  // Founder preview may render with zero observations (seven dimmed cards).
  // Live customer UI still requires live_ui_authorized.
  const authorizeRender = liveOk || founderPreview;
  if (!authorizeRender) {
    return {
      version: GUEST_SENTIMENT_MODEL_V1,
      taxonomyVersion: GUEST_THEME_TAXONOMY_V1,
      hotelId,
      hotelName,
      visible: false,
      platforms: [],
      platformOverview: [],
      overviewPlatformCount: REPUTATION_OVERVIEW_PLATFORMS_V1.length,
      themes: [],
      insights: null,
      trends: { available: false, reason: "insufficient_history" },
      limitations: packet.limitations || [],
      rightsChip: null,
      canonicalSource: "HE_GUEST_SENTIMENT_STAGED_OR_AIRTABLE",
      dashboardVariant: "guest-reputation-experience-v2",
    };
  }

  if (!observations.length) {
    const emptyOverview = buildReputationPlatformOverview([]);
    return {
      version: GUEST_SENTIMENT_MODEL_V1,
      taxonomyVersion: GUEST_THEME_TAXONOMY_V1,
      hotelId,
      hotelName,
      visible: true,
      proposedPreview: !liveOk && founderPreview,
      platforms: [],
      platformOverview: emptyOverview,
      overviewPlatformCount: REPUTATION_OVERVIEW_PLATFORMS_V1.length,
      themes: [],
      categoryPerformance: {
        available: false,
        sourceClass: "PLATFORM_PUBLISHED_CATEGORY_SCORES",
        note: "Platform-published category scores — not review-theme frequency counts.",
        defaultPlatform: null,
        platforms: [],
      },
      executiveInsights: buildExecutiveInsightCards({
        hotelName,
        platforms: [],
        strengths: [],
        concerns: [],
        themes: [],
        trends: { available: false },
        followUps: packet.insights?.followUps || [],
        packetInsights: packet.insights,
      }),
      reviewThemes: { available: false, items: [] },
      insights: packet.insights || null,
      trends: { available: false, reason: "insufficient_history" },
      limitations: packet.limitations || [],
      rightsChip: liveOk
        ? null
        : "Proposed preview — public listing metadata. Rights review required. Not Airtable-persisted. Not a licensed review feed.",
      canonicalSource: packet.canonicalSource || "HE_GUEST_SENTIMENT_STAGED_OR_AIRTABLE",
      observedAt: packet.observedAt || null,
      dashboardVariant: "guest-reputation-experience-v2",
    };
  }

  const platforms = observations.map((o) => ({
    platform: o.platform,
    platformLabel: o.platformLabel,
    rating: o.rating,
    scaleMax: o.scaleMax,
    ratingDisplay:
      o.rating != null && o.scaleMax != null
        ? `${Number(o.rating).toFixed(1)} / ${o.scaleMax}`
        : o.rating != null
          ? Number(o.rating).toFixed(1)
          : null,
    reviewCount: o.reviewCount,
    observedAt: o.observedAt,
    listingUrl: o.listingUrl,
    rightsStatus: o.rightsStatus,
    categoryScores: (o.categoryScores || []).map((c) => ({
      label: c.label,
      score: c.score,
      scaleMax: c.scaleMax,
      themeId: c.themeId || null,
      scoreDisplay:
        c.score != null && c.scaleMax != null
          ? `${Number(c.score).toFixed(1)} / ${c.scaleMax}`
          : c.score != null
            ? String(c.score)
            : null,
      pct: c.scaleMax ? Math.round((c.score / c.scaleMax) * 1000) / 10 : null,
    })),
  }));

  const themeMap = new Map();
  for (const o of observations) {
    for (const c of o.categoryScores) {
      const themeId = c.themeId;
      if (!themeId || !GUEST_THEME_BY_ID[themeId]) continue;
      if (!themeMap.has(themeId)) {
        themeMap.set(themeId, {
          themeId,
          label: GUEST_THEME_BY_ID[themeId].label,
          scores: [],
          platforms: new Set(),
        });
      }
      const row = themeMap.get(themeId);
      row.scores.push({
        platform: o.platformLabel,
        score: c.score,
        scaleMax: c.scaleMax,
        normalized: c.scaleMax ? c.score / c.scaleMax : null,
      });
      row.platforms.add(o.platformLabel);
    }
  }

  const themes = [...themeMap.values()]
    .map((t) => {
      const norms = t.scores.map((s) => s.normalized).filter((n) => n != null);
      const avgNorm = norms.length
        ? norms.reduce((a, b) => a + b, 0) / norms.length
        : null;
      const polarity =
        avgNorm == null ? "mixed" : avgNorm >= 0.8 ? "strength" : avgNorm <= 0.65 ? "concern" : "mixed";
      return {
        themeId: t.themeId,
        label: t.label,
        polarity,
        platformCoverage: [...t.platforms],
        evidence: t.scores.map((s) => ({
          platform: s.platform,
          scoreDisplay: `${s.score} / ${s.scaleMax}`,
        })),
        // Never emit fake percentages on tiny samples
        percentClaimAllowed: (packet.totalReviewSample || 0) >= MIN_SAMPLE_FOR_PERCENT,
      };
    })
    .sort((a, b) => a.label.localeCompare(b.label));

  const strengths = themes.filter((t) => t.polarity === "strength").slice(0, 4);
  const concerns = themes.filter((t) => t.polarity === "concern").slice(0, 4);

  const followUps = Array.isArray(packet.insights?.followUps)
    ? packet.insights.followUps
    : defaultFollowUps(concerns);

  const insights = {
    executiveSummary:
      text(packet.insights?.executiveSummary) ||
      buildDefaultInsight(hotelName, platforms, strengths, concerns),
    strengths: strengths.map((t) => t.label),
    concerns: concerns.map((t) => t.label),
    followUps,
    evidenceClass: "PLATFORM_CATEGORY_SCORES",
    interpretationNote:
      "Category scores are platform-published aggregates — not verified causes of operational performance. High category scores are not the same as frequently mentioned guest-review themes.",
  };

  const categoryPlatforms = platforms.filter(
    (p) => Array.isArray(p.categoryScores) && p.categoryScores.length > 0
  );
  const defaultCategoryPlatform =
    categoryPlatforms.find((p) => p.platform === "BOOKING")?.platform ||
    categoryPlatforms[0]?.platform ||
    null;

  const history = Array.isArray(packet.history) ? packet.history : [];
  const trends = {
    available: history.length >= 2,
    reason: history.length >= 2 ? null : "insufficient_history",
    windows: ["30d", "90d", "180d", "365d"],
    series: history.length >= 2 ? packet.trendWindows || null : null,
  };

  // Review-level theme analysis requires licensed review text — never invent from category scores.
  const reviewThemesAvailable =
    packet.reviewThemesAvailable === true &&
    Array.isArray(packet.reviewThemes) &&
    packet.reviewThemes.length > 0;
  const reviewThemes = {
    available: reviewThemesAvailable,
    reason: reviewThemesAvailable ? null : "no_licensed_review_text",
    items: reviewThemesAvailable ? packet.reviewThemes : [],
  };

  const executiveInsights = buildExecutiveInsightCards({
    hotelName,
    platforms,
    strengths,
    concerns,
    themes,
    trends,
    followUps,
    packetInsights: packet.insights,
  });

  const platformOverview = buildReputationPlatformOverview(platforms);

  return {
    version: GUEST_SENTIMENT_MODEL_V1,
    taxonomyVersion: GUEST_THEME_TAXONOMY_V1,
    hotelId,
    hotelName,
    visible: true,
    proposedPreview: !liveOk && founderPreview,
    platforms,
    platformOverview,
    overviewPlatformCount: REPUTATION_OVERVIEW_PLATFORMS_V1.length,
    themes,
    categoryPerformance: {
      available: categoryPlatforms.length > 0,
      sourceClass: "PLATFORM_PUBLISHED_CATEGORY_SCORES",
      note: "Platform-published category scores — not review-theme frequency counts.",
      defaultPlatform: defaultCategoryPlatform,
      platforms: categoryPlatforms.map((p) => ({
        platform: p.platform,
        platformLabel: p.platformLabel,
        scaleMax: p.scaleMax,
        categories: p.categoryScores,
      })),
    },
    executiveInsights,
    reviewThemes,
    insights,
    trends,
    limitations: packet.limitations || [],
    rightsChip: liveOk
      ? null
      : "Proposed preview — public listing metadata. Rights review required. Not Airtable-persisted. Not a licensed review feed.",
    canonicalSource: packet.canonicalSource || "STAGED_PUBLIC_LISTING_METADATA",
    observedAt: packet.observedAt || null,
    dashboardVariant: "guest-reputation-experience-v2",
  };
}

function metricFromThemes(themeRows, limit = 3) {
  return (themeRows || [])
    .flatMap((t) =>
      (t.evidence || []).map((e) => `${e.platform} ${t.label} ${e.scoreDisplay}`)
    )
    .slice(0, limit)
    .join(" · ");
}

/**
 * Compact executive assessment (HE-QL-029): four scannable cards.
 * Relative Strengths · Areas to Investigate · Recommended Management Follow-Up · Trend & Data Confidence
 */
function buildExecutiveInsightCards({
  hotelName,
  platforms,
  strengths,
  concerns,
  themes,
  trends,
  followUps,
  packetInsights,
}) {
  const custom = Array.isArray(packetInsights?.executiveCards)
    ? packetInsights.executiveCards.filter(
        (c) => c && text(c.title) && text(c.finding)
      )
    : [];
  if (custom.length) {
    return custom.slice(0, 4).map((c) => ({
      id: text(c.id) || text(c.title).toLowerCase().replace(/\s+/g, "_"),
      role: text(c.role) || null,
      title: text(c.title),
      finding: text(c.finding),
      metric: text(c.metric) || text(c.evidence) || null,
      evidence: text(c.evidence) || null,
      implication: text(c.implication) || null,
      confidence: text(c.confidence) || null,
    }));
  }

  const cards = [];

  // Prefer softest relative category for investigate when no concern-band themes
  let investigateThemes = concerns.slice(0, 3);
  if (!investigateThemes.length && Array.isArray(themes) && themes.length) {
    const softest = themes
      .map((t) => {
        const norms = (t.evidence || [])
          .map((e) => {
            const m = String(e.scoreDisplay || "").match(/^([\d.]+)\s*\/\s*([\d.]+)/);
            if (!m) return null;
            return Number(m[1]) / Number(m[2]);
          })
          .filter((n) => n != null);
        const avg = norms.length ? norms.reduce((a, b) => a + b, 0) / norms.length : 1;
        return { ...t, _avg: avg };
      })
      .sort((a, b) => a._avg - b._avg);
    if (softest.length && softest[0]._avg < 0.85) {
      investigateThemes = [softest[0]];
    }
  }

  cards.push({
    id: "relative_strengths",
    role: "RELATIVE_STRENGTHS",
    title: "Relative Strengths",
    finding: strengths.length
      ? `Stronger published category scores for ${strengths.map((t) => t.label).join(", ")}.`
      : platforms.length
        ? `${hotelName} listing scores are mid-to-upper on available platforms; no category cleared the directional strength band.`
        : "No platform category strengths identified yet.",
    metric: metricFromThemes(strengths) ||
      platforms
        .filter((p) => p.ratingDisplay)
        .map((p) => `${p.platformLabel} ${p.ratingDisplay}`)
        .slice(0, 3)
        .join(" · ") ||
      null,
    evidence: metricFromThemes(strengths, 6) || null,
    implication: strengths.length
      ? "Protect these listing signals in guest messaging — not proof of ops excellence."
      : null,
    confidence: null,
  });

  cards.push({
    id: "areas_to_investigate",
    role: "AREAS_TO_INVESTIGATE",
    title: "Areas to Investigate",
    finding: investigateThemes.length
      ? `Softer relative category scores for ${investigateThemes
          .map((t) => t.label)
          .join(", ")} — investigate; not proof of recurring guest complaints.`
      : "No category scores fell into a relative soft band in the permitted sample.",
    metric: metricFromThemes(investigateThemes) || null,
    evidence: metricFromThemes(investigateThemes, 6) || null,
    implication: investigateThemes.length
      ? "Compare with internal KPIs before CapEx or service conclusions."
      : "Re-check after the next observation window.",
    confidence: null,
  });

  const follow = (followUps || []).filter(Boolean).slice(0, 2);
  cards.push({
    id: "recommended_follow_up",
    role: "RECOMMENDED_FOLLOW_UP",
    title: "Recommended Management Follow-Up",
    finding: follow.length
      ? follow[0]
      : "Confirm platform property IDs against the canonical HPC record before any paid refresh.",
    metric: follow.length > 1 ? follow[1] : null,
    evidence: null,
    implication: follow.length > 1 ? null : "Keep rights/licensing review separate from listing scores.",
    confidence: null,
  });

  const availableCount = platforms.filter((p) => p.rating != null).length;
  cards.push({
    id: "trend_data_confidence",
    role: "TREND_DATA_CONFIDENCE",
    title: "Trend & Data Confidence",
    finding: trends?.available
      ? "Historical observation windows support trend comparison."
      : "Trend analysis unavailable — single snapshot only (no 30/90/180/365 series).",
    metric: `${availableCount} platform${availableCount === 1 ? "" : "s"} with native ratings · scales not averaged`,
    evidence: trends?.available
      ? null
      : "Do not project renovation or service-recovery trends from one observation.",
    implication: null,
    confidence: "Directional — platform aggregates; rights review still required for live UI",
  });

  return cards;
}

function buildDefaultInsight(hotelName, platforms, strengths, concerns) {
  const plat = platforms.map((p) => p.platformLabel).join(", ");
  const s = strengths.length
    ? `Relative category strengths include ${strengths.map((t) => t.label).join("; ")}.`
    : "No clear category strengths above the directional threshold.";
  const c = concerns.length
    ? `Relative category concerns include ${concerns.map((t) => t.label).join("; ")}.`
    : "No category scores fell into the directional concern band.";
  return `${hotelName} has platform listing metadata from ${plat || "available sources"}. ${s} ${c} Do not treat these scores as ADR/RevPAR or CapEx proof.`;
}

function defaultFollowUps(concerns) {
  const out = [
    "Confirm which platform property IDs match the canonical HPC record before any paid refresh.",
  ];
  if (concerns.some((t) => t.themeId === "maintenance" || t.themeId === "guestroom_condition")) {
    out.push("Ask operations for recent work-order / room-refresh cadence tied to guestroom condition themes.");
  }
  if (concerns.some((t) => t.themeId === "location_access")) {
    out.push("Validate shuttle / Metro / parking guest guidance versus listing expectations.");
  }
  return out;
}

/**
 * Proposed Airtable tables (dry-run design only).
 */
export function proposedAirtableSentimentSchema() {
  return {
    applyAuthorized: false,
    classification: "PERSISTENCE_DESIGNED",
    tables: [
      {
        name: "HI Hotel Reputation Observations",
        purpose: "Point-in-time platform rating snapshots keyed to HPC",
        fields: [
          "HPC Hotel ID",
          "Platform",
          "Platform Property ID",
          "Listing URL",
          "Native Rating",
          "Rating Scale Max",
          "Review Count",
          "Observed At",
          "Sample Period",
          "Collection Method",
          "Rights Status",
          "Provenance Notes",
          "Dedupe Key",
        ],
      },
      {
        name: "HI Hotel Reputation Theme Scores",
        purpose: "Platform category / theme scores linked to observations",
        fields: [
          "Observation",
          "Theme ID",
          "Theme Label",
          "Score",
          "Scale Max",
          "Polarity Hint",
          "Evidence Note",
        ],
      },
    ],
    notes: [
      "Do not store full review bodies unless licensed.",
      "Reuse HPC identity; do not invent parallel hotel keys.",
      "Historical rows retained — never overwrite prior Observed At snapshots.",
    ],
  };
}
