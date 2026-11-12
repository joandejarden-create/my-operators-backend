/**
 * Normalize hunter discoveries into a shared commercial screening shape.
 * Does not invent buyers, dates, or RFPs.
 */

import {
  DISCOVERY_EVIDENCE_CLASS,
  resolveDiscoveryEvidenceClass,
  stampDiscoveryEvidenceClass,
} from "../shared/discovery-evidence-class-v1.js";

export const NORMALIZE_DISCOVERY_V1 = "gdi-nextgen-normalize-discovery-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function httpUrls(list = []) {
  return [
    ...new Set(
      (list || [])
        .map((u) => text(u))
        .filter((u) => /^https?:\/\//i.test(u))
    ),
  ];
}

/**
 * Canonical discovery row for screening / dedupe / depth / ORS handoff.
 */
export function normalizeHunterDiscovery(raw = {}, meta = {}) {
  const evidenceUrls = httpUrls([
    ...(raw.evidenceUrls || []),
    raw.sourceUrl,
    raw.url,
    ...(raw.supportingSources || []).map((s) => s?.url || s?.sourceUrl),
  ]);
  const organizationName =
    text(
      raw.organizationName ||
        raw.organizingEntity ||
        raw.companyName ||
        raw.orgName
    ) || null;
  const activityTitle =
    text(
      raw.activityTitle ||
        raw.title ||
        raw.eventName ||
        raw.programmeName ||
        raw.demandTrigger
    ) || null;
  const cls = resolveDiscoveryEvidenceClass(
    raw,
    meta.discoveryEvidenceClass || DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
  );

  const row = stampDiscoveryEvidenceClass(
    {
      schema: NORMALIZE_DISCOVERY_V1,
      sourceHunter: meta.hunterId || raw.sourceHunter || null,
      hotelId: meta.hotelId || raw.hotelId || null,
      organizationName,
      domain: text(raw.domain) || null,
      externalOrgId: raw.externalOrgId || raw.noticeId || null,
      activityTitle,
      activityDate: raw.activityDate || raw.eventStartDate || null,
      activityDateVerified: raw.activityDateVerified === true,
      activityDateKind: raw.activityDateKind || null,
      geographicScope:
        text(raw.geographicScope || raw.geography || raw.city || raw.territory) ||
        null,
      demandTrigger: text(raw.demandTrigger || activityTitle) || null,
      pursuitThesis:
        text(
          raw.pursuitThesis ||
            raw.hotelOpportunityThesis ||
            raw.whyMyHotel ||
            raw.defaultThesis
        ) || null,
      evidenceUrls,
      sourceUrl: evidenceUrls[0] || null,
      sourceProvenance: {
        provider: raw.provider || meta.provider || "public",
        researchMethodId: raw.researchMethodId || meta.researchMethodId || null,
        sourceAuthority: raw.sourceAuthority || null,
        retrievedAt: raw.retrievedAt || meta.retrievedAt || null,
        researchFreshness:
          raw.researchFreshness ||
          (cls === DISCOVERY_EVIDENCE_CLASS.LIVE_PUBLIC_DISCOVERY
            ? "FRESH_RUN"
            : cls === DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
              ? "HISTORICAL_VERIFIED"
              : "FIXTURE_OR_UNKNOWN"),
      },
      contactName: text(raw.contactName) || null,
      contactRole: text(raw.contactRole) || null,
      contactEmail: text(raw.contactEmail) || null,
      buyerVerified: raw.buyerVerified === true,
      intermediaryVerified: raw.intermediaryVerified === true,
      hasCredibleContact: Boolean(
        (raw.contactName && raw.contactRole) ||
          raw.buyerVerified === true ||
          raw.intermediaryVerified === true
      ),
      hasRealisticHotelThesis: Boolean(
        text(raw.pursuitThesis || raw.hotelOpportunityThesis || raw.whyMyHotel)
      ),
      confidence: Number(raw.confidence ?? 0.5),
      hotelDemandRelevance: Number(raw.hotelDemandRelevance ?? 0.5),
      recordKind: raw.recordKind || "DEMAND_TRIGGER",
      uncertainties: Array.isArray(raw.uncertainties)
        ? raw.uncertainties.map(text).filter(Boolean)
        : [],
      unresolvedEvidenceGaps: Array.isArray(raw.unresolvedEvidenceGaps)
        ? raw.unresolvedEvidenceGaps.map(text).filter(Boolean)
        : [],
      language: raw.language || meta.language || "en",
      countryCode: raw.countryCode || meta.countryCode || null,
      exclude: raw.exclude === true,
      rawHints: {
        noticeId: raw.noticeId || null,
        segmentId: raw.segmentId || meta.segmentId || null,
      },
    },
    cls
  );

  if (!row.unresolvedEvidenceGaps.length) {
    const gaps = [];
    if (!row.buyerVerified && !row.contactName) {
      gaps.push("named_buyer_or_activity_connected_intermediary_unverified");
    }
    if (!row.activityDateVerified) {
      gaps.push("future_activity_date_unverified");
    }
    if (!row.pursuitThesis) {
      gaps.push("hotel_specific_demand_thesis_missing");
    }
    row.unresolvedEvidenceGaps = gaps;
  }

  return row;
}

export function normalizeHunterDiscoveries(rows = [], meta = {}) {
  return (rows || []).map((r) => normalizeHunterDiscovery(r, meta));
}
