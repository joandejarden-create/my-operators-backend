/**
 * Account / trip / occurrence identity + shared housing pathway marking.
 * Distinct exhibitor accounts may remain separate prospects but must not be
 * counted as independently verified buyers merely via a shared housing center.
 */

import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";

export const IDENTITY_DEDUPE_VERSION = "gdi-parallel-identity-dedupe-v1";

/** Event-name aliases (e.g. ASCB/EMBO Cell Bio). */
const EVENT_ALIASES = Object.freeze([
  {
    canonical: "ascb_embo_cell_bio",
    patterns: [/ascb/i, /embo/i, /cell\s*bio/i],
  },
  {
    canonical: "iesna_flagship",
    patterns: [/iesna/i, /intersolar/i, /energy\s*storage\s*north\s*america/i],
  },
  {
    canonical: "west_afcea_usni",
    patterns: [/\bwest\b/i, /afcea/i, /naval\s*institute/i],
  },
]);

const SHARED_HOUSING_MARKERS = [
  /housing\s*center/i,
  /onpeak/i,
  /maritz/i,
  /spargo/i,
  /official\s*housing/i,
  /exhibitor[- ]room[- ]block/i,
  /group\s*housing/i,
];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function normalizeEventAliasKey(nameOrTitle = "") {
  const s = text(nameOrTitle);
  for (const alias of EVENT_ALIASES) {
    if (alias.patterns.some((re) => re.test(s))) return alias.canonical;
  }
  return normalizeOrgIdentityKey(s).slice(0, 80) || "undated_event";
}

export function buildAccountTripOccurrenceKey({
  organizationName,
  eventStartDate,
  eventName,
  travelingCohort,
} = {}) {
  const org = normalizeOrgIdentityKey(organizationName);
  const start = String(eventStartDate || "").slice(0, 10) || "undated";
  const eventKey = normalizeEventAliasKey(eventName || organizationName);
  const cohort = normalizeOrgIdentityKey(travelingCohort || "").slice(0, 40);
  return `${org}|${eventKey}|${start}|${cohort || "default"}`;
}

export function detectSharedHousingPathway(textBlob = "", contactName = "") {
  const blob = `${textBlob} ${contactName}`;
  const matched = SHARED_HOUSING_MARKERS.filter((re) => re.test(blob)).map((re) =>
    String(re)
  );
  const isShared = matched.length > 0;
  return {
    isSharedHousingPathway: isShared,
    markers: matched,
    note: isShared
      ? "Shared housing/buyer pathway — do not treat as account-specific verified buyer mandate"
      : null,
  };
}

/**
 * Deduplicate candidates by account+trip+occurrence.
 * Marks shared housing pathways; keeps distinct exhibitors separate.
 */
export function dedupeAccountTripCandidates(candidates = []) {
  const byKey = new Map();
  const sharedPathways = new Map();
  const duplicates = [];

  for (const raw of candidates) {
    const c = { ...raw };
    const key =
      c.dedupeKey ||
      buildAccountTripOccurrenceKey({
        organizationName: c.organizationName || c.account,
        eventStartDate: c.eventStartDate || c.timing?.start,
        eventName: c.title || c.timing?.raw || c.organizationName,
        travelingCohort: c.travelingCohort,
      });
    c.accountTripOccurrenceKey = key;
    const housing = detectSharedHousingPathway(
      `${c.sourcingStatus || ""} ${c.demandSignal || ""} ${c.recommendedAction || ""}`,
      c.primaryContact?.name || c.contact?.name
    );
    c.sharedHousingPathway = housing;
    if (housing.isSharedHousingPathway) {
      const pathwayKey =
        normalizeEventAliasKey(c.title || c.timing?.raw || "") ||
        "shared_housing";
      if (!sharedPathways.has(pathwayKey)) sharedPathways.set(pathwayKey, []);
      sharedPathways.get(pathwayKey).push(c.candidateId || c.cardId || key);
      c.independentlyVerifiedBuyer = false;
      c.buyerPathwayNote =
        "Shares housing/agency pathway — not independently verified as account buyer";
    }

    if (byKey.has(key)) {
      duplicates.push({
        kept: byKey.get(key).candidateId || byKey.get(key).cardId,
        dropped: c.candidateId || c.cardId,
        key,
      });
      continue;
    }
    byKey.set(key, c);
  }

  return {
    version: IDENTITY_DEDUPE_VERSION,
    unique: [...byKey.values()],
    duplicateCount: duplicates.length,
    duplicates,
    sharedHousingPathways: [...sharedPathways.entries()].map(([pathway, ids]) => ({
      pathway,
      opportunityIds: ids,
      independentlyVerifiedBuyerCount: 0,
      note: "Multiple accounts may share this housing route; count buyers carefully",
    })),
  };
}
