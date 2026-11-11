/**
 * V14 weekly incremental research repeatability contract (hotel-generic).
 *
 * Simulates a second weekly cycle against an existing inventory using
 * saved/fixture candidates — zero paid. Does not mutate production.
 */
import { createHash } from "node:crypto";
import {
  acceptFourteenWorkflowOpportunity,
  DEDUP_CLASS,
  ACCEPTANCE_DISPOSITION,
} from "./fourteen-workflow-commercial-acceptance-gates-v1.js";
import { safeDisplayText, firstSafeDisplayText } from "./safe-display-text-v1.js";

export const V14_WEEKLY_INCREMENTAL_REPEATABILITY_V1 =
  "gdi-v14-weekly-incremental-repeatability-v1";

export const WEEKLY_CLASSIFICATION = Object.freeze({
  NEW_OPPORTUNITY: "NEW_OPPORTUNITY",
  EXISTING_OPPORTUNITY_ENRICHED: "EXISTING_OPPORTUNITY_ENRICHED",
  NEW_EDITION_CYCLE: "NEW_EDITION_CYCLE",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  WATCH: "WATCH",
  REJECTED: "REJECTED",
  DUPLICATE: "DUPLICATE",
  ARCHIVED_MATCH: "ARCHIVED_MATCH",
});

function text(v) {
  return safeDisplayText(v);
}

function orgKey(o = {}) {
  return text(o.organizationName || o.companyName || o.title)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function opportunityFingerprint(o = {}) {
  const raw = [
    String(o.id || ""),
    orgKey(o),
    text(o.title || o.displayTitle || o.programmeName).toLowerCase(),
    text(o.eventStartDate || o.startDate || "").slice(0, 10),
    text(o.demandSegmentId || o.researchSegment || ""),
  ].join("|");
  return createHash("sha256").update(raw).digest("hex").slice(0, 16);
}

/**
 * Classify a weekly discovery candidate against existing hotel inventory.
 */
export function classifyWeeklyCandidate(candidate = {}, inventory = {}, opts = {}) {
  const facing = inventory.facingIds || new Set();
  const internal = inventory.internalIds || new Set();
  const archived = inventory.archivedIds || new Set();
  const fingerprints = inventory.fingerprints || new Map();
  const orgKeys = inventory.orgKeys || new Map(); // orgKey → [ids]
  const accountFamilies = inventory.accountFamilies || [];

  const id = String(candidate.id || candidate.opportunityId || "");
  const fp = opportunityFingerprint(candidate);
  const ok = orgKey(candidate);
  const title = text(candidate.title || candidate.programmeName).toLowerCase();

  if (id && archived.has(id)) {
    return {
      classification: WEEKLY_CLASSIFICATION.ARCHIVED_MATCH,
      action: "DO_NOT_RESURRECT",
      fingerprint: fp,
      matchedId: id,
    };
  }

  if (id && (facing.has(id) || internal.has(id))) {
    const hasNewEvidence =
      Array.isArray(candidate.newEvidenceUrls) &&
      candidate.newEvidenceUrls.length > 0;
    return {
      classification: hasNewEvidence
        ? WEEKLY_CLASSIFICATION.EXISTING_OPPORTUNITY_ENRICHED
        : WEEKLY_CLASSIFICATION.DUPLICATE,
      action: hasNewEvidence
        ? "APPEND_EVIDENCE_PRESERVE_DISPOSITION"
        : "DO_NOT_CREATE_TILE",
      fingerprint: fp,
      matchedId: id,
      preserveCustomerDisposition: facing.has(id),
    };
  }

  // Exact fingerprint match
  if (fingerprints.has(fp)) {
    return {
      classification: WEEKLY_CLASSIFICATION.DUPLICATE,
      action: "DO_NOT_CREATE_TILE",
      fingerprint: fp,
      matchedId: fingerprints.get(fp),
    };
  }

  // Same org + similar title → enrichment or new edition
  const priorIds = ok ? orgKeys.get(ok) || [] : [];
  if (priorIds.length) {
    const priorId = priorIds[0];
    const editionHint =
      /20(2[6-9]|3[0-9])/.test(title) &&
      candidate.eventStartDate &&
      opts.priorDatesById?.get(priorId) &&
      String(candidate.eventStartDate).slice(0, 7) !==
        String(opts.priorDatesById.get(priorId)).slice(0, 7);

    if (editionHint) {
      return {
        classification: WEEKLY_CLASSIFICATION.NEW_EDITION_CYCLE,
        action: "CREATE_DISTINCT_OPPORTUNITY_LINK_ACCOUNT_FAMILY",
        fingerprint: fp,
        matchedId: priorId,
        accountFamily: ok,
      };
    }

    return {
      classification: WEEKLY_CLASSIFICATION.EXISTING_OPPORTUNITY_ENRICHED,
      action: "ASSOCIATE_ACCOUNT_FAMILY_APPEND_RESEARCH",
      fingerprint: fp,
      matchedId: priorId,
      accountFamily: ok,
    };
  }

  // Account-family soft match
  for (const fam of accountFamilies) {
    const members = (fam.members || []).map(String);
    const famBlob = `${fam.familyId || ""} ${members.join(" ")}`.toLowerCase();
    if (ok && famBlob.includes(ok.slice(0, 10))) {
      return {
        classification: WEEKLY_CLASSIFICATION.EXISTING_OPPORTUNITY_ENRICHED,
        action: "ASSOCIATE_ACCOUNT_FAMILY",
        fingerprint: fp,
        matchedId: members[0] || null,
        accountFamily: fam.familyId,
      };
    }
  }

  // Commercial gate on candidate (fixture / saved research only)
  const acceptance = acceptFourteenWorkflowOpportunity(candidate, {
    hotelId: opts.hotelId,
    hotelName: opts.hotelName,
    baselineIds: facing,
    archivedIds: archived,
  });

  if (acceptance.dedupClass === DEDUP_CLASS.HISTORICAL_OR_ARCHIVED_MATCH) {
    return {
      classification: WEEKLY_CLASSIFICATION.ARCHIVED_MATCH,
      action: "DO_NOT_RESURRECT",
      fingerprint: fp,
      acceptance,
    };
  }
  if (acceptance.dedupClass === DEDUP_CLASS.DUPLICATE_DO_NOT_STAGE) {
    return {
      classification: WEEKLY_CLASSIFICATION.DUPLICATE,
      action: "DO_NOT_CREATE_TILE",
      fingerprint: fp,
      acceptance,
    };
  }
  if (acceptance.disposition === ACCEPTANCE_DISPOSITION.NOT_CUSTOMER_READY) {
    return {
      classification: WEEKLY_CLASSIFICATION.REJECTED,
      action: "DO_NOT_DEEP_RESEARCH_REPEAT",
      fingerprint: fp,
      acceptance,
    };
  }
  if (acceptance.disposition === ACCEPTANCE_DISPOSITION.WATCH) {
    return {
      classification: WEEKLY_CLASSIFICATION.WATCH,
      action: "MONITOR_ONLY",
      fingerprint: fp,
      acceptance,
    };
  }
  if (
    acceptance.disposition ===
    ACCEPTANCE_DISPOSITION.RESEARCH_CONTINUATION_REQUIRED
  ) {
    return {
      classification: WEEKLY_CLASSIFICATION.RESEARCH_CONTINUATION,
      action: "CONTINUE_ONLY_IF_NEW_EVIDENCE_OR_REFRESH_DUE",
      fingerprint: fp,
      acceptance,
    };
  }

  return {
    classification: WEEKLY_CLASSIFICATION.NEW_OPPORTUNITY,
    action: "STAGE_FOR_INDEPENDENT_GATE_THEN_FOUNDER_REVIEW",
    fingerprint: fp,
    acceptance,
  };
}

/**
 * Build inventory index for weekly classification.
 */
export function buildWeeklyInventoryIndex(opportunities = [], opts = {}) {
  const facingIds = new Set(opts.facingIds || []);
  const archivedIds = new Set(opts.archivedIds || []);
  const internalIds = new Set();
  const fingerprints = new Map();
  const orgKeys = new Map();
  const priorDatesById = new Map();

  for (const o of opportunities || []) {
    const id = String(o.id || "");
    if (!id) continue;
    if (!facingIds.has(id) && !archivedIds.has(id)) internalIds.add(id);
    fingerprints.set(opportunityFingerprint(o), id);
    const ok = orgKey(o);
    if (ok) {
      const arr = orgKeys.get(ok) || [];
      arr.push(id);
      orgKeys.set(ok, arr);
    }
    const d = o.eventStartDate || o.startDate || null;
    if (d) priorDatesById.set(id, d);
  }

  return {
    facingIds,
    internalIds,
    archivedIds,
    fingerprints,
    orgKeys,
    priorDatesById,
    accountFamilies: opts.accountFamilies || [],
  };
}

/**
 * Cross-hotel: same org may exist at multiple hotels — never global suppress.
 */
export function assertHotelScopedDedup(hotelA = {}, hotelB = {}) {
  const aOrgs = new Set((hotelA.opportunities || []).map(orgKey).filter(Boolean));
  const bOrgs = new Set((hotelB.opportunities || []).map(orgKey).filter(Boolean));
  const shared = [...aOrgs].filter((k) => bOrgs.has(k));
  return {
    sharedOrganizationKeys: shared,
    allowedAsSeparateHotelOpportunities: true,
    rule: "Dedup is hotel-scoped; shared org identity is informative, not suppressing",
  };
}

/**
 * Simulate second weekly cycle with controlled fixtures.
 */
export function simulateWeeklyIncrementalCycle({
  hotelId,
  hotelName,
  opportunities = [],
  facingIds = [],
  archivedIds = [],
  fixtures = [],
  accountFamilies = [],
} = {}) {
  const index = buildWeeklyInventoryIndex(opportunities, {
    facingIds,
    archivedIds,
    accountFamilies,
  });
  const results = [];
  for (const fix of fixtures) {
    results.push({
      fixtureId: fix.id || fix.fixtureId || opportunityFingerprint(fix),
      ...classifyWeeklyCandidate(fix, index, {
        hotelId,
        hotelName,
        priorDatesById: index.priorDatesById,
      }),
    });
  }

  const counts = {};
  for (const r of results) {
    counts[r.classification] = (counts[r.classification] || 0) + 1;
  }

  // Integrity: facing dispositions must not flip solely from empty re-research
  const emptyRerun = facingIds.slice(0, 5).map((id) => {
    const prior = opportunities.find((o) => String(o.id) === id);
    if (!prior) return null;
    const thin = {
      id,
      organizationName: prior.organizationName,
      title: prior.title,
      // intentionally thinner — weekly rerun with less info
    };
    const cls = classifyWeeklyCandidate(thin, index, {
      hotelId,
      hotelName,
      priorDatesById: index.priorDatesById,
    });
    return {
      id,
      classification: cls.classification,
      preserveCustomerDisposition: cls.preserveCustomerDisposition === true,
      action: cls.action,
    };
  }).filter(Boolean);

  return {
    version: V14_WEEKLY_INCREMENTAL_REPEATABILITY_V1,
    hotelId,
    hotelName,
    fixtureResults: results,
    classificationCounts: counts,
    facingDispositionPreservedOnThinRerun: emptyRerun.every(
      (r) => r.preserveCustomerDisposition === true,
    ),
    thinRerunChecks: emptyRerun,
    archivedResurrectionAttempts: results.filter(
      (r) => r.classification === WEEKLY_CLASSIFICATION.ARCHIVED_MATCH,
    ).length,
    duplicateTilesPrevented: results.filter(
      (r) =>
        r.classification === WEEKLY_CLASSIFICATION.DUPLICATE ||
        r.action === "DO_NOT_CREATE_TILE",
    ).length,
  };
}

export function buildDefaultWeeklyFixtures(hotel = {}, facingSample = []) {
  const city = firstSafeDisplayText(hotel.city, hotel.market, "destination");
  const sample = facingSample[0] || {};
  return [
    {
      id: sample.id,
      organizationName: sample.organizationName,
      title: sample.title,
      fixtureKind: "exact_prior_id",
    },
    {
      id: `fixture_new_${city}_assoc_2028`,
      organizationName: `Mock ${city} Professional Association`,
      title: `${city} Annual Meeting 2028`,
      eventStartDate: "2028-09-12",
      demandSegmentId: "associations_conferences_events",
      whatIsHappening: `Announced 2028 annual meeting in ${city} with open venue sourcing.`,
      hotelOpportunityThesis: `Plausible meetings and overflow lodging for ${hotel.displayName || hotel.name}.`,
      primaryContact: { name: "Alex Rivera", role: "Meetings Manager" },
      sources: [{ url: "https://example.com/mock-2028-meeting" }],
      recommendedNextStep:
        "Qualify whether venue and housing sourcing remain open for the 2028 edition and who owns the RFP.",
      fixtureKind: "genuinely_new",
    },
    {
      id: `fixture_archived_${sample.id || "x"}`,
      organizationName: sample.organizationName || "Archived Org",
      title: sample.title || "Archived programme",
      fixtureKind: "archived_probe",
      // id will be remapped by caller to an archived id when available
    },
    {
      id: `fixture_edition_${orgKey(sample) || "org"}`,
      organizationName: sample.organizationName || `Mock ${city} Org`,
      title: `${text(sample.title || "Programme")} — 2029 edition`,
      eventStartDate: "2029-05-01",
      demandSegmentId: sample.demandSegmentId || "associations_conferences_events",
      fixtureKind: "new_edition",
    },
  ];
}
