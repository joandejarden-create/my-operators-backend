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
  GOOGLE: { id: "GOOGLE", label: "Google", scaleMax: 5 },
  TRIPADVISOR: { id: "TRIPADVISOR", label: "Tripadvisor", scaleMax: 5 },
  BOOKING: { id: "BOOKING", label: "Booking.com", scaleMax: 10 },
  EXPEDIA: { id: "EXPEDIA", label: "Expedia", scaleMax: 10 },
});

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

  const authorizeRender = liveOk || (founderPreview && observations.length > 0);
  if (!authorizeRender || !observations.length) {
    return {
      version: GUEST_SENTIMENT_MODEL_V1,
      taxonomyVersion: GUEST_THEME_TAXONOMY_V1,
      hotelId,
      hotelName,
      visible: false,
      platforms: [],
      themes: [],
      insights: null,
      trends: { available: false, reason: "insufficient_history" },
      limitations: packet.limitations || [],
      rightsChip: null,
      canonicalSource: "HE_GUEST_SENTIMENT_STAGED_OR_AIRTABLE",
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
    trends,
    followUps,
    packetInsights: packet.insights,
  });

  return {
    version: GUEST_SENTIMENT_MODEL_V1,
    taxonomyVersion: GUEST_THEME_TAXONOMY_V1,
    hotelId,
    hotelName,
    visible: true,
    proposedPreview: !liveOk && founderPreview,
    platforms,
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
    dashboardVariant: "guest-reputation-experience-v1",
  };
}

function buildExecutiveInsightCards({
  hotelName,
  platforms,
  strengths,
  concerns,
  trends,
  followUps,
  packetInsights,
}) {
  const cards = [];
  const custom = Array.isArray(packetInsights?.executiveCards)
    ? packetInsights.executiveCards.filter(
        (c) => c && text(c.title) && text(c.finding)
      )
    : [];
  if (custom.length) {
    return custom.slice(0, 5).map((c) => ({
      id: text(c.id) || text(c.title).toLowerCase().replace(/\s+/g, "_"),
      title: text(c.title),
      finding: text(c.finding),
      evidence: text(c.evidence) || null,
      implication: text(c.implication) || null,
      confidence: text(c.confidence) || "Limited — platform aggregates only",
    }));
  }

  if (strengths.length) {
    const ev = strengths
      .flatMap((t) =>
        (t.evidence || []).map((e) => `${e.platform} ${e.scoreDisplay}`)
      )
      .slice(0, 4)
      .join("; ");
    cards.push({
      id: "strengths",
      title: "Strengths",
      finding: `${hotelName} shows relatively firm platform category scores for ${strengths
        .map((t) => t.label)
        .join(", ")}.`,
      evidence: ev || null,
      implication:
        "Owners can treat these as listing-signal strengths to protect in guest messaging — not as proof of operational excellence.",
      confidence: "Directional — category aggregates, not review-theme frequency",
    });
  }

  if (concerns.length) {
    const ev = concerns
      .flatMap((t) =>
        (t.evidence || []).map((e) => `${e.platform} ${e.scoreDisplay}`)
      )
      .slice(0, 4)
      .join("; ");
    cards.push({
      id: "areas_to_investigate",
      title: "Areas to Investigate",
      finding: `Softer relative category scores appear for ${concerns
        .map((t) => t.label)
        .join(", ")}.`,
      evidence: ev || null,
      implication:
        "Worth an ops/commercial check against internal KPIs — do not infer CapEx or service failure from listing categories alone.",
      confidence: "Directional — investigate before acting",
    });
  } else if (platforms.length) {
    cards.push({
      id: "areas_to_investigate",
      title: "Areas to Investigate",
      finding:
        "No category scores fell into the directional concern band in the permitted sample.",
      evidence: platforms
        .map((p) => `${p.platformLabel} ${p.ratingDisplay}`)
        .join("; "),
      implication:
        "Continue monitoring once a second observation window exists; absence of a soft category is not a guarantee of guest satisfaction.",
      confidence: "Limited sample of published categories",
    });
  }

  const ratingLine = platforms
    .filter((p) => p.ratingDisplay)
    .map((p) => `${p.platformLabel} ${p.ratingDisplay}`)
    .join("; ");
  if (ratingLine) {
    cards.push({
      id: "operational_implications",
      title: "Operational Implications",
      finding:
        "Native platform ratings differ in scale and review population — they are not interchangeable benchmarks.",
      evidence: ratingLine,
      implication:
        "Use each platform’s native score with its own denominator when briefing owners; never quote a blended reputation index.",
      confidence: "High for scale isolation; low for causal ops claims",
    });
  }

  cards.push({
    id: "data_limitations",
    title: "Data Limitations",
    finding: trends?.available
      ? "Historical observation windows are available for trend comparison."
      : "Trend windows (30/90/180/365-day) are not available from a single snapshot.",
    evidence: trends?.available
      ? null
      : "Insufficient historical observations — do not manufacture series from one scrape.",
    implication: followUps?.length
      ? followUps[0]
      : "Confirm platform property IDs against HPC before any paid refresh.",
    confidence: "Governance — rights and licensing still required for live UI",
  });

  return cards.slice(0, 5);
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
