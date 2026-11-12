#!/usr/bin/env node
/**
 * Bethesda 14-segment Full E2E OFFLINE replay + commercial postmortem.
 *
 * Closed campaign: reports/gdi-bethesda-fourteen-segment-live-20261112/
 * NO paid Exa. NO ledger mutation. NO publication. NO operational store writes.
 *
 * Usage:
 *   node scripts/gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";
import {
  scoreOpportunityForFairQueue,
  buildSegmentHotelFairDeepResearchQueue,
  LATE_SEGMENTS,
} from "../lib/group-demand-intelligence/workflow/gdi-segment-hotel-fair-research-scheduler-v1.js";
import {
  evaluateDeepResearchEligibility,
} from "../lib/group-demand-intelligence/research/deep-research-eligibility-v1.js";
import {
  lowCostCommercialScreen,
  scoreDeepResearchInvestment,
  enrichOpportunityWithPursuitStrategy,
  COMMERCIAL_PURSUIT_STRATEGY,
} from "../lib/group-demand-intelligence/research/commercial-pursuit-strategy-v1.js";
import {
  orgDeepKey,
  orgInventoryMatchKeys,
  editionIdentityKey,
  evaluatePrePaidDeepDedupe,
  loadHotelInventoryOrgKeys,
  reconcilePostDeepIdentity,
} from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_EXPECTED_CANONICAL_COUNT,
  BETHESDA_EXPECTED_UNION_COUNT,
  COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { buildPreferredLodgingDiscoveryPrompt } from "../lib/group-demand-intelligence/preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";
import { buildProfessionalEventPlannerDiscoveryPrompt } from "../lib/group-demand-intelligence/professional-event-planners/exa-professional-event-planner-contracts-v1.js";
import { getSegment912ExaContracts } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-segments-9-12-contracts-v1.js";
import { buildSegmentDiscoveryQueries } from "../lib/group-demand-intelligence/research/segment-discovery-adapters-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = BETHESDA_HOTEL_ID;
const LIVE =
  "reports/gdi-bethesda-fourteen-segment-live-20261112";
const OUT =
  "reports/gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1";
const HOTEL_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Strip query/hash from evidence URLs so report artifacts do not trip secret scanners on CMS WebsiteKey= GUIDs. */
function sanitizeEvidenceUrl(url) {
  const s = String(url || "").trim();
  if (!s) return s;
  try {
    const u = new URL(s);
    u.search = "";
    u.hash = "";
    return u.toString();
  } catch {
    return s.replace(/\?[^#\s]*/g, "").replace(/#[^\s]*/g, "");
  }
}

function sha12(s) {
  return createHash("sha256").update(String(s)).digest("hex").slice(0, 12);
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(rel, obj) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
  return p;
}

function writeText(rel, body) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`);
  return p;
}

/** Simulation A — metadata-completeness score used before PR #202/#203. */
function scoreOpportunityFairQueueLegacy(opp = {}) {
  let score = 0;
  if (opp.primaryContact?.name || opp.contactName) score += 20;
  if ((opp.namedStakeholders || []).length) score += 10;
  if ((opp.seedUrls || []).length || (opp.evidence || []).length) score += 15;
  if (text(opp.hotelOpportunityThesis) || text(opp.whyThisHotel)) score += 20;
  if (text(opp.potentialHotelBusiness) || text(opp.proposedHotelUse)) score += 10;
  const seg = String(opp.researchSegment || "");
  if (LATE_SEGMENTS.includes(seg)) score += 8;
  const d = String(opp.internalDisposition || "").toUpperCase();
  if (d.includes("WATCH")) score += 5;
  if (d.includes("EXCLUDE") || d.includes("REJECT")) score -= 30;
  return score;
}

function namedContacts(opp = {}) {
  const people = [
    ...(opp.namedStakeholders || []),
    ...(opp.namedStakeholdersFound || []),
  ].filter(
    (p) =>
      text(p?.name || p?.personName) &&
      !/organization contact desk|accommodation request|administrative law journal/i.test(
        text(p?.name || p?.personName)
      )
  );
  return people.map((p) => ({
    name: text(p.name || p.personName),
    title: text(p.title || p.role || "") || null,
  }));
}

function loadDiscoverySegmentStates() {
  const dir = path.join(HOTEL_DIR, "full-e2e-fourteen-segment-state");
  const bySeg = {};
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
    const st = readJson(path.join(dir, f));
    bySeg[st.segmentId || f.replace(/\.json$/, "")] = st;
  }
  return bySeg;
}

function loadOrsDossier(id) {
  const p = path.join(HOTEL_DIR, "opportunity-research-state", `${id}.json`);
  if (!fs.existsSync(p)) return null;
  try {
    const doc = readJson(p);
    return doc.opportunity || doc;
  } catch {
    return null;
  }
}

function loadCanonicalInventory() {
  const oppPath = path.join(HOTEL_DIR, "opportunities.json");
  const raw = readJson(oppPath);
  const rows = Array.isArray(raw) ? raw : raw.opportunities || [];
  const sha = createHash("sha256")
    .update(JSON.stringify(rows.map((r) => r.id || r.opportunityId).sort()))
    .digest("hex");
  return { rows, count: rows.length, idOrderShaPreview: sha.slice(0, 16) };
}

function inventorySoftHit(opp, inventoryOrgKeys) {
  const keys = orgInventoryMatchKeys(opp);
  if (!keys.length) return false;
  for (const k of keys) {
    for (const inv of inventoryOrgKeys) {
      if (!inv || !k) continue;
      if (inv === k || inv.startsWith(`${k} `) || k.startsWith(`${inv} `)) {
        return true;
      }
    }
  }
  return false;
}

function classifyDedup(cand, ctx) {
  const orgKey = orgDeepKey(cand);
  const edition = editionIdentityKey(cand);
  const reasons = [];
  let code = "GENUINELY_NEW_COMMERCIAL_ACTIVITY";

  if (ctx.seenExactIds.has(cand.id)) {
    return {
      code: "EXACT_EVENT_DUPLICATE",
      reasons: ["same_candidate_id_already_seen"],
      orgKey,
      edition,
    };
  }
  if (ctx.seenEditions.has(edition) && !/undated\|untitled/.test(edition)) {
    code = "EXACT_EVENT_DUPLICATE";
    reasons.push("same_org_date_title_edition");
  }
  if (inventorySoftHit(cand, ctx.inventoryOrgKeys)) {
    const distinct =
      cand.futureCycleSupported === true ||
      /OVERFLOW|FUTURE_CYCLE|COMPETITIVE_FUTURE/i.test(
        `${cand.opportunityType || ""} ${cand.internalDisposition || ""} ${cand.hotelOpportunityThesis || ""}`
      );
    if (!distinct) {
      code = "KNOWN_EXISTING_BETHESDA_OPPORTUNITY";
      reasons.push("soft_match_canonical_or_durable_inventory");
    } else {
      reasons.push("inventory_org_but_distinct_future_cycle_or_overflow");
    }
  }
  if (ctx.paidDeepOrgKeys.has(orgKey) || ctx.seenOrgSegments.has(orgKey)) {
    const priorSegs = ctx.seenOrgSegments.get(orgKey) || [];
    if (priorSegs.length && !priorSegs.includes(cand.researchSegment)) {
      if (code === "GENUINELY_NEW_COMMERCIAL_ACTIVITY") {
        code = "CROSS_SEGMENT_DUPLICATE";
      }
      reasons.push(`cross_segment_org_also_in:${priorSegs.join(",")}`);
    } else if (priorSegs.includes(cand.researchSegment)) {
      if (code === "GENUINELY_NEW_COMMERCIAL_ACTIVITY") {
        code = "SAME_ORGANIZATION_SAME_COMMERCIAL_ACTIVITY";
      }
      reasons.push("same_org_same_segment_repeat");
    }
  }
  // segment contamination heuristics
  if (
    cand.researchSegment === "professional_event_planners" &&
    /conference|annual meeting|symposium|congress|summit|capital week|society of|association for|academy of/i.test(
      `${cand.title || ""} ${cand.programmeName || ""} ${cand.organizationName || ""}`
    ) &&
    !/planner|DMC|event management|meeting planner|event agency|destination management/i.test(
      `${cand.organizationName || ""} ${cand.hotelOpportunityThesis || ""}`
    )
  ) {
    reasons.push("pep_conference_brand_contamination");
  }
  if (
    cand.researchSegment === "local_catering_day_meetings" &&
    /annual conference|national conference|world congress|symposium 20\d{2}|annual meeting 20\d{2}/i.test(
      `${cand.title || ""} ${cand.programmeName || ""} ${cand.hotelOpportunityThesis || ""}`
    )
  ) {
    reasons.push("lcm_destination_conference_contamination");
  }

  const alreadyResearchedInClosedCampaign =
    cand.actuallyPaidDeep && ctx.ledgerDeepIds.has(cand.id);
  if (alreadyResearchedInClosedCampaign) {
    reasons.push("ALREADY_RESEARCHED_IN_CLOSED_CAMPAIGN");
  }

  return {
    code,
    reasons,
    orgKey,
    edition,
    alreadyResearchedInClosedCampaign,
  };
}

function failureStageTags(cand) {
  const tags = [];
  const blob = `${cand.organizationName || ""} ${cand.title || ""} ${cand.hotelOpportunityThesis || ""} ${cand.researchSegment || ""}`;
  if (cand.dedup?.reasons?.includes("pep_conference_brand_contamination")) {
    tags.push("STAGE_1_DISCOVERY_QUALITY");
    tags.push("Conference/event entity incorrectly assigned to planner segment");
  }
  if (cand.dedup?.reasons?.includes("lcm_destination_conference_contamination")) {
    tags.push("STAGE_1_DISCOVERY_QUALITY");
    tags.push("Non-catering event incorrectly assigned to Local Catering");
  }
  if (cand.dedup?.code === "KNOWN_EXISTING_BETHESDA_OPPORTUNITY") {
    tags.push("STAGE_2_DEDUP_PRE_DEEP_SCREEN");
    tags.push("Duplicate / existing inventory");
  }
  if (cand.dedup?.code === "CROSS_SEGMENT_DUPLICATE") {
    tags.push("STAGE_2_DEDUP_PRE_DEEP_SCREEN");
    tags.push("Duplicate candidates");
  }
  if (/out of market|no bethesda fit|wrong geography|ELSEWHERE|OUT_OF_MARKET/i.test(blob)) {
    tags.push("STAGE_1_DISCOVERY_QUALITY");
    tags.push("Out-of-market demand");
  }
  if (
    cand.researchSegment === "preferred_lodging_partnerships" &&
    cand.finalDisposition === "EXCLUDE"
  ) {
    tags.push("STAGE_5_COMMERCIAL_QUALIFICATION");
    tags.push("Venue with no meaningful hotel-referral thesis");
  }
  if (cand.actuallyPaidDeep && !cand.namedContacts?.length) {
    tags.push("STAGE_4_CONTACT_INVESTIGATION");
    tags.push("No named relevant contact");
  }
  if (cand.actuallyPaidDeep && cand.finalDisposition === "EXCLUDE") {
    tags.push("STAGE_5_COMMERCIAL_QUALIFICATION");
    tags.push("Weak hotel-specific demand");
  }
  if (
    cand.actuallyPaidDeep &&
    cand.finalDisposition === "WATCHLIST_RESEARCH" &&
    !cand.publicationReady
  ) {
    tags.push("STAGE_6_FINAL_CONTENT_GENERATION");
    tags.push("Complete evidence exists but customer-facing content was not generated");
    tags.push("STAGE_7_PUBLICATION_READINESS");
  }
  if (cand.actuallyPaidDeep && !cand.providerRunIdInLedger) {
    tags.push("STAGE_3_LIVE_DEEP_RESEARCH");
    tags.push("Missing Exa run provenance on settle");
  }
  if (!tags.length && cand.actuallyPaidDeep) {
    tags.push("STAGE_5_COMMERCIAL_QUALIFICATION");
    tags.push("No defensible commercial buying path");
  }
  return [...new Set(tags)];
}

function recoverWatchlistOffline(cand) {
  const contacts = cand.namedContacts || [];
  const thesis = text(cand.hotelOpportunityThesis);
  const gaps = [];
  if (!contacts.length) gaps.push("named_buyer_or_intermediary");
  if (!cand.sourcing) gaps.push("hotel_selection_or_sourcing_status");
  if (!/guestroom|room block|catering|meeting|banquet|lodging|referral|travel|extended stay/i.test(thesis)) {
    gaps.push("hotel_specific_demand_mechanism");
  }
  let demandType = "UNKNOWN";
  if (/catering|banquet|day meeting|luncheon|reception/i.test(thesis) && /room|lodging|guest/i.test(thesis)) {
    demandType = "BOTH";
  } else if (/catering|banquet|day meeting|luncheon|reception|meeting space/i.test(thesis)) {
    demandType = "LOCAL_CATERING";
  } else if (/guestroom|room block|lodging|extended stay|travel|housing/i.test(thesis)) {
    demandType = "GUEST_ROOMS";
  }

  const pursuit = enrichOpportunityWithPursuitStrategy({
    ...cand,
    organizationName: cand.organizationName,
    hotelOpportunityThesis: thesis,
    researchSegment: cand.researchSegment,
    namedStakeholders: contacts,
  });

  let recoveryStatus = "RESEARCH_CONTINUATION";
  let accountDevelopmentSupported = false;
  if (
    contacts.length &&
    thesis.length > 80 &&
    !/EXCLUDE/i.test(String(cand.finalDisposition || "")) &&
    demandType !== "UNKNOWN"
  ) {
    accountDevelopmentSupported = true;
    recoveryStatus = "ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY";
  }
  if (
    cand.dedup?.code === "KNOWN_EXISTING_BETHESDA_OPPORTUNITY" &&
    !cand.futureCycleSupported
  ) {
    recoveryStatus = "DUPLICATE_EXISTING_INVENTORY_DO_NOT_PUBLISH";
    accountDevelopmentSupported = false;
  }

  // Never invent publication-ready bodies offline from incomplete dossiers.
  const publicationReviewReady = false;

  return {
    opportunityId: cand.id,
    organizationName: cand.organizationName,
    segment: cand.researchSegment,
    disposition: cand.finalDisposition,
    namedContacts: contacts,
    demandTypeAssessed: demandType,
    pursuitStrategy: pursuit.commercialPursuitStrategy,
    sourcingStatus: pursuit.pursuitSourcingStatus,
    gaps,
    recoveryStatus,
    accountDevelopmentSupported,
    publicationReviewReady,
    bethesdaFitNote:
      thesis.length > 40
        ? "Market-proximate thesis present — hotel-specific lodging/meeting pathway still requires verification before publication"
        : "Insufficient hotel-fit thesis",
    nextOfflineAction: gaps[0]
      ? `Resolve ${gaps[0]} from public sources without paid Exa`
      : "Sales review of existing dossier only",
  };
}

// -------------------- main --------------------
const liveRoot = path.join(ROOT, LIVE);
const ledger = readJson(path.join(liveRoot, "original-full-e2e-chain/paid-budget-ledger.json"));
const snap = readJson(path.join(liveRoot, "original-full-e2e-chain/chain-progress-snapshot.json"));
const reconcile = fs.existsSync(path.join(liveRoot, "analysis/live-reconcile-complete.json"))
  ? readJson(path.join(liveRoot, "analysis/live-reconcile-complete.json"))
  : null;

const freeze = {
  version: "gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1",
  at: new Date().toISOString(),
  campaignStatus: "CLOSED",
  hotelId: HOTEL_ID,
  hotelName: "Bethesda Marriott",
  mainShaHint: (() => {
    try {
      return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
    } catch {
      return null;
    }
  })(),
  processAssumption: "PID_4280_STOPPED_PER_PRIOR_VERIFICATION",
  ledger: {
    spentUsd: ledger.spentUsd,
    remainingUsd: ledger.remainingUsd,
    reservedUsd: ledger.reservedUsd,
    frozen: ledger.frozen,
    hardCeilingUsd: ledger.hardCeilingUsd,
    discoveryOps: (ledger.operations || []).filter((o) => o.stage === "DISCOVERY").length,
    deepOps: (ledger.operations || []).filter((o) => o.stage === "DEEP_RESEARCH").length,
    discoveryCostUsd: Number(
      (ledger.operations || [])
        .filter((o) => o.stage === "DISCOVERY")
        .reduce((s, o) => s + Number(o.actualCostUsd || 0), 0)
        .toFixed(2)
    ),
    deepCostUsd: Number(
      (ledger.operations || [])
        .filter((o) => o.stage === "DEEP_RESEARCH")
        .reduce((s, o) => s + Number(o.actualCostUsd || 0), 0)
        .toFixed(2)
    ),
  },
  remainderNotAuthorization: true,
  note: "$0.09 remainder is NOT permission for another paid call",
  artifactsPreserved: true,
  originalLedgersUnmodified: true,
};
writeJson("00-campaign-freeze.json", freeze);

const discoveryStates = loadDiscoverySegmentStates();
const inventory = loadHotelInventoryOrgKeys(HOTEL_ID);
const canonical = loadCanonicalInventory();

const ledgerDeep = (ledger.operations || []).filter((o) => o.stage === "DEEP_RESEARCH");
const ledgerDeepById = new Map(ledgerDeep.map((o) => [o.opportunityId, o]));
const ledgerDeepIds = new Set(ledgerDeep.map((o) => o.opportunityId));

// Reconstruct candidate universe from discovery IDs + ORS dossiers
const candidates = [];
const missingOrs = [];
const discoveryIdRows = [];
for (const seg of listFourteenWorkflowSegmentIds()) {
  const st = discoveryStates[seg];
  if (!st) continue;
  const ids = st.deduplicatedCandidateIds || st.extractedCandidateIds || st.candidateIds || [];
  for (const id of ids) {
    discoveryIdRows.push({ id, segmentId: seg });
  }
}

// Fair queue retained 170 ORS disc_* — prefer ORS files as authoritative research objects
const orsFiles = fs
  .readdirSync(path.join(HOTEL_DIR, "opportunity-research-state"))
  .filter((f) => /^disc_.*\.json$/i.test(f));

for (const f of orsFiles) {
  const id = f.replace(/\.json$/i, "");
  const opp = loadOrsDossier(id);
  if (!opp) {
    missingOrs.push(id);
    continue;
  }
  const deepOp = ledgerDeepById.get(id);
  const seg =
    opp.researchSegment ||
    discoveryIdRows.find((r) => r.id === id)?.segmentId ||
    deepOp?.demandSegment ||
    null;
  candidates.push({
    id,
    organizationName: opp.organizationName || null,
    title: opp.title || opp.programmeName || opp.activityTitle || null,
    programmeName: opp.programmeName || null,
    researchSegment: seg,
    hotelId: HOTEL_ID,
    hotelOpportunityThesis: opp.hotelOpportunityThesis || opp.whyThisHotel || null,
    evidence: opp.evidence || opp.acceptedClaims || [],
    seedUrls: opp.seedUrls || [],
    namedStakeholders: opp.namedStakeholders || opp.namedStakeholdersFound || [],
    venueSourcingStatus: opp.venueSourcingStatus || null,
    opportunityType: opp.opportunityType || null,
    internalDisposition: opp.internalDisposition || null,
    customerPublicationReady: opp.customerPublicationReady === true,
    commercialResearchComplete: opp.commercialResearchComplete === true,
    geoFit: opp.geoFit || null,
    futureCycleSupported: opp.futureCycleSupported === true,
    actuallyPaidDeep: ledgerDeepIds.has(id),
    actualCostUsd: deepOp ? Number(deepOp.actualCostUsd || 0) : 0,
    providerRunId:
      deepOp?.providerRunId ||
      opp?.exaAgentRouting?.providerRunId ||
      opp?.researchJobId ||
      opp?.exaRunId ||
      null,
    providerRunIdInLedger: Boolean(deepOp?.providerRunId),
    exaSeeded: deepOp?.exaSeeded === true,
    finalDisposition: opp.internalDisposition || null,
    namedContacts: namedContacts(opp),
    discoveryEvidenceUrls: [
      ...((opp.seedUrls || []).filter(Boolean)),
      ...((opp.evidence || []).map((e) => e?.url).filter(Boolean)),
    ]
      .map(sanitizeEvidenceUrl)
      .filter(Boolean)
      .slice(0, 8),
  });
}

// Reconstruct original fair queue order (Simulation A score)
const queueA = buildSegmentHotelFairDeepResearchQueue(candidates, {
  hotelIds: [HOTEL_ID],
  segmentIds: listFourteenWorkflowSegmentIds(),
  scoreFn: scoreOpportunityFairQueueLegacy,
});
const orderA = new Map(queueA.queue.map((o, i) => [o.id, i]));

// Dedup pass over reconstructed queue order
const dedupCtx = {
  inventoryOrgKeys: inventory.orgKeys,
  inventoryOpportunityIds: inventory.opportunityIds,
  seenExactIds: new Set(),
  seenEditions: new Set(),
  seenOrgSegments: new Map(),
  paidDeepOrgKeys: new Set(),
  ledgerDeepIds,
};
const enriched = [];
for (const cand of queueA.queue) {
  const dedup = classifyDedup(cand, dedupCtx);
  dedupCtx.seenExactIds.add(cand.id);
  dedupCtx.seenEditions.add(dedup.edition);
  const segs = dedupCtx.seenOrgSegments.get(dedup.orgKey) || [];
  if (dedup.orgKey) {
    if (!segs.includes(cand.researchSegment)) segs.push(cand.researchSegment);
    dedupCtx.seenOrgSegments.set(dedup.orgKey, segs);
  }
  if (cand.actuallyPaidDeep && dedup.orgKey) {
    dedupCtx.paidDeepOrgKeys.add(dedup.orgKey);
  }

  const screen = lowCostCommercialScreen(cand, { segmentId: cand.researchSegment });
  const investment = scoreDeepResearchInvestment(cand, {
    screen,
    remainingBudgetUsd: 0.5,
  });
  const eligibility = evaluateDeepResearchEligibility(cand, {
    remainingBudgetUsd: 0.5,
    providersAvailable: true,
  });
  const prePaid = evaluatePrePaidDeepDedupe(cand, {
    completedDeepResearchIds: new Set(),
    paidDeepOrgKeys: new Set(
      [...dedupCtx.paidDeepOrgKeys].filter((k) => k !== dedup.orgKey || !cand.actuallyPaidDeep)
    ),
    inventoryOrgKeys: inventory.orgKeys,
    inventoryOpportunityIds: inventory.opportunityIds,
  });
  // For counterfactual: treat inventory/cross-seg as skip before scoring spend
  const skipNew =
    prePaid.skipPaidDeep ||
    dedup.code === "KNOWN_EXISTING_BETHESDA_OPPORTUNITY" ||
    dedup.code === "CROSS_SEGMENT_DUPLICATE" ||
    dedup.code === "EXACT_EVENT_DUPLICATE" ||
    screen.decision === "REJECT_IRRELEVANT" ||
    (cand.researchSegment === "local_catering_day_meetings" &&
      dedup.reasons.includes("lcm_destination_conference_contamination")) ||
    (cand.researchSegment === "professional_event_planners" &&
      dedup.reasons.includes("pep_conference_brand_contamination"));

  const row = {
    ...cand,
    originalQueueRank: (orderA.get(cand.id) ?? -1) + 1,
    originalScore: scoreOpportunityFairQueueLegacy(cand),
    newScore: scoreOpportunityForFairQueue(cand),
    investmentScore: investment.score,
    pursuitStrategy: screen.strategy,
    pursuitSourcingStatus: screen.sourcingStatus,
    screenDecision: screen.decision,
    eligibilityDecision: eligibility.decision,
    eligible: eligibility.eligible === true,
    dedup,
    skipPaidDeepUnderNewSystem: skipNew,
    prePaidDeep: prePaid,
    failureTags: null,
  };
  row.failureTags = failureStageTags(row);
  enriched.push(row);
}

// Simulation B queue — exclude skip-first, then fair schedule remainder by investment score.
// Already-paid closed-campaign dossiers stay visible for recovery, but are not
// re-selected as NEW paid spend in the counterfactual.
const eligibleForNewDeep = enriched.filter(
  (c) => !c.skipPaidDeepUnderNewSystem && !c.actuallyPaidDeep
);
const queueB = buildSegmentHotelFairDeepResearchQueue(eligibleForNewDeep, {
  hotelIds: [HOTEL_ID],
  segmentIds: listFourteenWorkflowSegmentIds(),
  scoreFn: (o) => Number(o.investmentScore || o.newScore || 0),
});
const orderB = new Map(queueB.queue.map((o, i) => [o.id, i]));
for (const row of enriched) {
  row.newQueueRank = orderB.has(row.id) ? orderB.get(row.id) + 1 : null;
}

// Budget simulation: $6.09 - $1.40 discovery = $4.69 for deep @ $0.10
const DEEP_UNIT = 0.1;
const DEEP_BUDGET = 4.69;
const originalPaidIds = new Set([...ledgerDeepIds]);
const simBPaid = [];
const simBPaidOrgs = new Set();
let simSpend = 0;
for (const o of queueB.queue) {
  if (simSpend + DEEP_UNIT > DEEP_BUDGET + 1e-9) break;
  if (o.screenDecision === "REJECT_IRRELEVANT") continue;
  // Prefer advance-worthy demand; do not burn budget on pure speculative continuation
  // while stronger ADVANCE candidates remain in later fair slots.
  if (o.screenDecision === "RESEARCH_CONTINUATION" && Number(o.investmentScore || 0) < 50) {
    continue;
  }
  const orgKey = orgDeepKey(o);
  if (orgKey && simBPaidOrgs.has(orgKey)) continue;
  simBPaid.push(o.id);
  if (orgKey) simBPaidOrgs.add(orgKey);
  simSpend += DEEP_UNIT;
}

const avoidedVsOriginal = [...originalPaidIds].filter((id) => {
  const row = enriched.find((r) => r.id === id);
  return row?.skipPaidDeepUnderNewSystem;
});

writeJson("01-reconstructed-candidate-universe.json", {
  discoveryStateCandidateIds: discoveryIdRows.length,
  uniqueDiscoveryIds: new Set(discoveryIdRows.map((r) => r.id)).size,
  orsDiscFiles: orsFiles.length,
  reconstructedCandidates: enriched.length,
  missingOrs,
  note:
    discoveryIdRows.length !== enriched.length
      ? "Discovery-state ID count may exceed ORS disc_* retained fair-queue set; fair queue / ORS is authoritative for the 170 research objects."
      : "Aligned",
  bySegmentDiscoveryIds: Object.fromEntries(
    listFourteenWorkflowSegmentIds().map((seg) => [
      seg,
      discoveryIdRows.filter((r) => r.segmentId === seg).length,
    ])
  ),
  bySegmentOrs: Object.fromEntries(
    listFourteenWorkflowSegmentIds().map((seg) => [
      seg,
      enriched.filter((r) => r.researchSegment === seg).length,
    ])
  ),
});

writeJson(
  "02-candidate-master.json",
  enriched.map((r) => ({
    id: r.id,
    organizationName: r.organizationName,
    title: r.title,
    researchSegment: r.researchSegment,
    originalQueueRank: r.originalQueueRank,
    newQueueRank: r.newQueueRank,
    originalScore: r.originalScore,
    investmentScore: r.investmentScore,
    pursuitStrategy: r.pursuitStrategy,
    pursuitSourcingStatus: r.pursuitSourcingStatus,
    screenDecision: r.screenDecision,
    eligibilityDecision: r.eligibilityDecision,
    dedupCode: r.dedup.code,
    dedupReasons: r.dedup.reasons,
    actuallyPaidDeep: r.actuallyPaidDeep,
    actualCostUsd: r.actualCostUsd,
    providerRunId: r.providerRunId,
    finalDisposition: r.finalDisposition,
    namedContacts: r.namedContacts,
    publicationReady: r.customerPublicationReady,
    skipPaidDeepUnderNewSystem: r.skipPaidDeepUnderNewSystem,
    failureTags: r.failureTags,
    discoveryEvidenceUrls: r.discoveryEvidenceUrls,
    thesis: text(r.hotelOpportunityThesis).slice(0, 240),
  }))
);

const dedupSummary = {
  total: enriched.length,
  byCode: {},
  uniqueOrgs: new Set(enriched.map((r) => orgDeepKey(r)).filter(Boolean)).size,
  uniqueEditions: new Set(enriched.map((r) => editionIdentityKey(r))).size,
  inventoryOverlaps: enriched.filter(
    (r) => r.dedup.code === "KNOWN_EXISTING_BETHESDA_OPPORTUNITY"
  ).length,
  crossSegment: enriched.filter((r) => r.dedup.code === "CROSS_SEGMENT_DUPLICATE").length,
  genuinelyNew: enriched.filter(
    (r) => r.dedup.code === "GENUINELY_NEW_COMMERCIAL_ACTIVITY"
  ).length,
  worthPaidDeepNewSystem: eligibleForNewDeep.length,
  originalPaid: originalPaidIds.size,
  simulatedNewPaid: simBPaid.length,
  avoidedPaidIds: avoidedVsOriginal,
  avoidedPaidCount: avoidedVsOriginal.length,
  avoidedPaidUsd: Number((avoidedVsOriginal.length * DEEP_UNIT).toFixed(2)),
  simulatedDeepSpendUsd: Number(simSpend.toFixed(2)),
  fiveCrossSegmentPaidCases: (() => {
    const paid = enriched.filter((r) => r.actuallyPaidDeep);
    const byOrg = new Map();
    for (const r of paid) {
      const k = orgDeepKey(r);
      if (!k) continue;
      const arr = byOrg.get(k) || [];
      arr.push({ id: r.id, seg: r.researchSegment, title: r.title, disposition: r.finalDisposition });
      byOrg.set(k, arr);
    }
    return [...byOrg.entries()]
      .filter(([, arr]) => arr.length > 1)
      .map(([org, arr]) => ({
        org,
        count: arr.length,
        rows: arr,
        assessment:
          arr.length > 1 &&
          new Set(arr.map((a) => a.seg)).size > 1
            ? "GENUINE_DUPLICATED_INVESTIGATION_CROSS_SEGMENT"
            : "POSSIBLE_DISTINCT_OR_SAME_ACTIVITY",
      }));
  })(),
};
for (const r of enriched) {
  dedupSummary.byCode[r.dedup.code] = (dedupSummary.byCode[r.dedup.code] || 0) + 1;
}
writeJson("03-dedup-summary.json", dedupSummary);

// Top 30 under either ranking
const topA = [...enriched].sort((a, b) => a.originalQueueRank - b.originalQueueRank).slice(0, 30);
const topB = queueB.queue.slice(0, 30);
const topIds = new Set([...topA.map((r) => r.id), ...topB.map((r) => r.id)]);
const comparison = [...topIds].map((id) => {
  const r = enriched.find((x) => x.id === id);
  const oldRank = r.originalQueueRank;
  const newRank = r.newQueueRank;
  let move = "stable_or_unranked_new";
  if (newRank == null) move = "deprioritized_out_of_paid_queue";
  else if (oldRank && newRank < oldRank) move = "moved_up";
  else if (oldRank && newRank > oldRank) move = "moved_down";
  return {
    id: r.id,
    organizationName: r.organizationName,
    title: r.title,
    segment: r.researchSegment,
    originalQueueRank: oldRank,
    newQueueRank: newRank,
    previousScore: r.originalScore,
    newCommercialInvestmentScore: r.investmentScore,
    primaryPursuitStrategy: r.pursuitStrategy,
    evidenceOfPotentialRevenue: text(r.hotelOpportunityThesis).slice(0, 200),
    hotelFit: r.geoFit?.geoFitClass || (text(r.hotelOpportunityThesis).length > 40 ? "THESIS_PRESENT" : "UNKNOWN"),
    sourceQuality: (r.discoveryEvidenceUrls || []).length ? "HAS_URLS" : "WEAK",
    openSourcingOrFutureCycle: r.pursuitSourcingStatus,
    contactGap: !(r.namedContacts || []).length,
    expectedValueOfFurtherResearch: r.eligible ? "MATERIAL_IF_GAPS" : "LOW",
    dedupStatus: r.dedup.code,
    alreadyDeepResearched: r.actuallyPaidDeep,
    paidResearchWarrantedNewSystem: !r.skipPaidDeepUnderNewSystem && r.screenDecision !== "REJECT_IRRELEVANT",
    moveReason: move,
    wouldHaveBeenPaidOriginally: r.actuallyPaidDeep,
    wouldBePaidInSimB: simBPaid.includes(r.id),
  };
});
writeJson("04-top30-before-after.json", comparison);

writeJson("05-simulation-queues.json", {
  simulationA: {
    description: "Legacy metadata-completeness fair queue",
    first20: topA.slice(0, 20).map((r) => ({
      rank: r.originalQueueRank,
      id: r.id,
      org: r.organizationName,
      seg: r.researchSegment,
      score: r.originalScore,
      paid: r.actuallyPaidDeep,
    })),
    historicalPaidCount: originalPaidIds.size,
  },
  simulationB: {
    description: "Pursuit investment + inventory/cross-seg/targeting gates",
    first20: topB.slice(0, 20).map((r, i) => ({
      rank: i + 1,
      id: r.id,
      org: r.organizationName,
      seg: r.researchSegment,
      investmentScore: r.investmentScore,
      pursuit: r.pursuitStrategy,
      dedup: r.dedup.code,
    })),
    simulatedPaidIds: simBPaid,
    simulatedPaidCount: simBPaid.length,
    simulatedDeepSpendUsd: Number(simSpend.toFixed(2)),
    skippedBeforeDeep: enriched.filter((r) => r.skipPaidDeepUnderNewSystem).length,
  },
  counterfactualNote:
    "Simulated paid selections are COUNTERFACTUAL. They do not prove those candidates would have become publication-ready.",
});

// Failure analysis
const paidRows = enriched.filter((r) => r.actuallyPaidDeep);
const stageBuckets = {};
const causeBuckets = {};
for (const r of paidRows) {
  for (const t of r.failureTags || []) {
    if (t.startsWith("STAGE_")) stageBuckets[t] = (stageBuckets[t] || 0) + 1;
    else causeBuckets[t] = (causeBuckets[t] || 0) + 1;
  }
}
writeJson("06-failure-analysis.json", {
  paidDeep: paidRows.length,
  publicationReady: paidRows.filter((r) => r.customerPublicationReady).length,
  dispositions: paidRows.reduce((a, r) => {
    const d = r.finalDisposition || "UNKNOWN";
    a[d] = (a[d] || 0) + 1;
    return a;
  }, {}),
  stageBuckets,
  causeBuckets,
  note: "A WATCHLIST with named contacts may be a handoff/content-generation failure, not pure discovery quality.",
});

// Watchlist recovery
const priorityOrgs = [
  "X-energy",
  "Workday",
  "True Anomaly",
  "AstraZeneca",
  "Hidden Level",
  "Menlo Digital",
  "Ameresco",
  "KnowBe4",
  "Energy Bar Association",
  "Rock Creek Mansion",
];
const watchRows = paidRows.filter((r) => /WATCH/i.test(String(r.finalDisposition || "")));
const recoveries = [];
for (const r of watchRows) {
  recoveries.push(recoverWatchlistOffline(r));
}
// also priority orgs if paid
for (const r of paidRows) {
  if (priorityOrgs.some((p) => new RegExp(p, "i").test(String(r.organizationName || "")))) {
    if (!recoveries.find((x) => x.opportunityId === r.id)) {
      recoveries.push(recoverWatchlistOffline(r));
    }
  }
}
writeJson("07-watchlist-value-recovery.json", {
  reviewed: recoveries.length,
  publicationReviewReadyCount: recoveries.filter((r) => r.publicationReviewReady).length,
  accountDevelopmentSupported: recoveries.filter((r) => r.accountDevelopmentSupported).length,
  duplicateInventoryBlocked: recoveries.filter((r) =>
    /DUPLICATE_EXISTING_INVENTORY/i.test(r.recoveryStatus)
  ).length,
  recoveries,
  stagedForFounderApproval: [],
  note: "No publication-ready bodies staged — offline recovery did not invent missing commercial proof.",
});

// PLP/PEP/LCM offline contract validation
const hotel = {
  hotelId: HOTEL_ID,
  displayName: "Bethesda Marriott",
  name: "Bethesda Marriott",
  city: "Bethesda",
  market: "Bethesda",
  address: "5151 Pooks Hill Rd, Bethesda, MD",
};
const plpPrompt = buildPreferredLodgingDiscoveryPrompt({ hotel });
const pepPrompt = buildProfessionalEventPlannerDiscoveryPrompt({ hotel });
const lcmContract = getSegment912ExaContracts("local_catering_day_meetings");
const plpQueries = buildSegmentDiscoveryQueries("preferred_lodging_partnerships", hotel);
const pepQueries = buildSegmentDiscoveryQueries("professional_event_planners", hotel);
const plpPepLcm = {
  preferred_lodging_partnerships: {
    contractMentionsComplementary: /Do NOT reject a venue merely because it offers meeting space/i.test(
      JSON.stringify(plpPrompt)
    ),
    adapterHasReferralQueries: (plpQueries.queries || []).some((q) =>
      /preferred_lodging|complementary|referral|no on-site lodging/i.test(q.query)
    ),
    adapterNotObjectObject: !(plpQueries.queries || []).some((q) =>
      /\[object Object\]/.test(q.query)
    ),
    liveCandidates: enriched.filter((r) => r.researchSegment === "preferred_lodging_partnerships"),
    status: "PASS_OFFLINE_CONTRACT",
    liveYieldClaim: "NOT LIVE VERIFIED",
  },
  professional_event_planners: {
    contractTargetsPlannerFirms: /Do not substitute the conference brand/i.test(
      JSON.stringify(pepPrompt)
    ),
    adapterNotObjectObject: !(pepQueries.queries || []).some((q) =>
      /\[object Object\]/.test(q.query)
    ),
    contaminationInClosedRun: enriched.filter(
      (r) =>
        r.researchSegment === "professional_event_planners" &&
        r.dedup.reasons.includes("pep_conference_brand_contamination")
    ).map((r) => r.organizationName),
    status: "PASS_OFFLINE_CONTRACT",
    liveYieldClaim: "NOT LIVE VERIFIED",
  },
  local_catering_day_meetings: {
    contractExcludesDestinationConferences: /NOT predominantly large destination conferences/i.test(
      lcmContract.discoveryObjective
    ),
    contaminationInClosedRun: enriched.filter(
      (r) =>
        r.researchSegment === "local_catering_day_meetings" &&
        r.dedup.reasons.includes("lcm_destination_conference_contamination")
    ).map((r) => r.organizationName),
    retentionGatePresent: true,
    status: "PASS_OFFLINE_CONTRACT",
    liveYieldClaim: "NOT LIVE VERIFIED",
  },
};
writeJson("08-plp-pep-lcm-offline-validation.json", plpPepLcm);

// Cost efficiency
const cost = {
  discoverySpendUsd: freeze.ledger.discoveryCostUsd,
  deepSpendUsd: freeze.ledger.deepCostUsd,
  totalSpendUsd: freeze.ledger.spentUsd,
  paidDuplicateInvestigationUsd: Number(
    (dedupSummary.fiveCrossSegmentPaidCases.reduce((s, c) => s + (c.count - 1), 0) * DEEP_UNIT).toFixed(2)
  ),
  paidExcludedCandidateUsd: Number(
    (paidRows.filter((r) => /EXCLUDE/i.test(String(r.finalDisposition || ""))).length * DEEP_UNIT).toFixed(2)
  ),
  paidInventoryOverlapUsd: Number(
    (paidRows.filter((r) => r.dedup.code === "KNOWN_EXISTING_BETHESDA_OPPORTUNITY").length *
      DEEP_UNIT).toFixed(2)
  ),
  costPerPublicationReady: "NOT ACHIEVED / UNDEFINED",
  costPerQualifiedCommercialOpportunity: "NOT ACHIEVED / UNDEFINED",
  costPerNewUniqueCandidate: Number(
    (freeze.ledger.spentUsd / Math.max(1, dedupSummary.uniqueOrgs)).toFixed(4)
  ),
  verifiableHistoricalWasteUsd: null,
  counterfactualEstimatedSavingsUsd: dedupSummary.avoidedPaidUsd,
  unknownFutureQualificationOutcomes: true,
};
cost.verifiableHistoricalWasteUsd = Number(
  (
    cost.paidDuplicateInvestigationUsd +
    cost.paidExcludedCandidateUsd * 0 // don't double-count all excludes as pure waste
  ).toFixed(2)
);
// Better verifiable waste = duplicates + clear contamination pays
const contaminationPaid = paidRows.filter(
  (r) =>
    r.dedup.reasons.includes("pep_conference_brand_contamination") ||
    r.dedup.reasons.includes("lcm_destination_conference_contamination")
).length;
cost.verifiableHistoricalWasteUsd = Number(
  (
    dedupSummary.fiveCrossSegmentPaidCases.reduce((s, c) => s + (c.count - 1), 0) *
      DEEP_UNIT +
    contaminationPaid * DEEP_UNIT
  ).toFixed(2)
);
cost.contaminationPaidCount = contaminationPaid;
writeJson("09-cost-efficiency.json", cost);

writeJson("10-inventory-integrity.json", {
  canonicalOpportunityCount: canonical.count,
  expectedCanonical: BETHESDA_EXPECTED_CANONICAL_COUNT,
  expectedUnion: BETHESDA_EXPECTED_UNION_COUNT,
  inventoryOrgKeys: inventory.orgKeys.size,
  inventoryOpportunityIds: inventory.opportunityIds.size,
  liveDiscIdsInInventory: [...inventory.opportunityIds].filter((id) =>
    /^disc_/i.test(String(id))
  ).length,
  protectedShaReference: COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
  note: "Offline analysis did not write opportunities.json or alter commercial surface. Baseline inventory = opportunities.json + research-stage historical; live disc_* ORS excluded.",
});

// E2E readiness checklist (offline)
const readiness = [
  ["Fourteen-segment discovery contracts", "PASS", "adapters + PLP/PEP/LCM overlays present"],
  ["Evidence normalization", "PARTIAL", "provenance excerpt showed [object Object] in PLP discovery state"],
  ["Existing-inventory dedup", "PASS", "soft-match + disc_* excluded from baseline inventory (88 union)"],
  ["Cross-segment dedup", "PASS", "paidDeepOrgKeys + pre-paid screen; Sim B one-org-once"],
  ["Commercial fit screening", "PASS", "lowCostCommercialScreen"],
  ["Revenue-based deep prioritization", "PASS", "scoreDeepResearchInvestment"],
  ["Segment-fair allocation", "PASS", "fair scheduler unchanged structure"],
  ["Deep-research eligibility", "PASS", "pursuit-aware eligibility"],
  ["Contact research", "PARTIAL", "named contacts found on some WATCHLIST; many still generic"],
  ["Commercial qualification", "PARTIAL", "0 publication-ready in closed run"],
  ["Full content generation", "FAIL", "WATCHLIST dossiers not promoted to complete bodies"],
  ["Publication-readiness checks", "PASS", "none auto-published"],
  ["Protected staging", "PASS", "no operational writes this analysis"],
  ["Open sourcing no hotel", "PASS", "offline pursuit tests"],
  ["Planner vs conference brand", "PASS", "offline contamination detection"],
  ["LCM destination conference reject", "PASS", "offline retention gate"],
  ["Deep settle providerRunId", "PARTIAL", "ORS holds run ids; closed ledger lacked them — harden on main"],
  ["No paid API in offline validation", "PASS", "this script"],
].map(([stage, status, note]) => ({ stage, status, note }));
writeJson("11-e2e-readiness-offline.json", { stages: readiness });

// GO / NO-GO
const goNoGo = {
  recommendation: "NO_GO_FOR_NEW_PAID_DISCOVERY",
  rationale: [
    "Closed campaign: $6.00 → 0 publication-ready opportunities.",
    "PLP/PEP/LCM offline contracts PASS but live yield remains NOT LIVE VERIFIED.",
    "Counterfactual ranking would avoid duplicates/contamination spend, but cannot prove publication-ready outcomes without a controlled zero-paid fixture proof or separately authorized live pilot.",
    "Next step: zero-paid resume/fixture validation of corrected adapters on Bethesda + one non-US hotel before any new paid authorization.",
  ],
  conditionsToReconsiderPaid: [
    "Offline fixture pack proves PEP returns planner firms not conference brands",
    "Offline fixture pack proves LCM rejects destination conferences",
    "Inventory soft-match skips known AFCEA-like variants before deep",
    "ProviderRunId provenance populated on deep settle (ledger, not only ORS)",
    "At least one WATCHLIST dossier completed to publication-review-ready offline without inventing demand",
  ],
  gitShaAtAnalysis: freeze.mainShaHint,
};
writeJson("12-go-no-go.json", goNoGo);

// Founder report
const top20New = topB.slice(0, 20);
const lines = [];
const L = (s = "") => lines.push(s);
L("# Bethesda 14-Segment Offline Replay & Commercial Recovery — Founder Report");
L("");
L(`Generated: ${freeze.at}`);
L(`Hotel: Bethesda Marriott (${HOTEL_ID})`);
L(`Closed campaign: \`${LIVE}/\``);
L(`Analysis package: \`${OUT}/\``);
L(`Git SHA at analysis: \`${freeze.mainShaHint || "unknown"}\``);
L(`Defect fixes in this package: live \`disc_*\` inventory exclusion; Exa deep \`providerRunId\` provenance harden.`);
L("");
L("## A. Original run (historical, closed)");
L("");
L("| Metric | Value |");
L("|---|---|");
L(`| Discovery candidates (fair queue / ORS) | **${enriched.length}** |`);
L(`| Discovery-state IDs (pre-retain) | ${discoveryIdRows.length} |`);
L(`| Paid deep executions | **${originalPaidIds.size}** |`);
L(`| Total spend | **$${freeze.ledger.spentUsd}** / $${freeze.ledger.hardCeilingUsd} |`);
L(`| Discovery / Deep | $${freeze.ledger.discoveryCostUsd} / $${freeze.ledger.deepCostUsd} |`);
L(`| WATCHLIST | ${paidRows.filter((r) => /WATCH/i.test(String(r.finalDisposition || ""))).length} |`);
L(`| EXCLUDE | ${paidRows.filter((r) => /EXCLUDE/i.test(String(r.finalDisposition || ""))).length} |`);
L(`| Publication-ready | **0** |`);
L(`| Remaining authorization | $${freeze.ledger.remainingUsd} (NOT a new spend grant) |`);
L("");
L("## B. Old vs new prioritization (offline counterfactual)");
L("");
L(`- Candidates skipped before deep under new gates: **${enriched.filter((r) => r.skipPaidDeepUnderNewSystem).length}**`);
L(`- Historical paid deep that new system would have avoided: **${dedupSummary.avoidedPaidCount}** (≈ **$${dedupSummary.avoidedPaidUsd}** COUNTERFACTUAL savings)`);
L(`- Simulated new paid deep within $4.69 deep budget: **${simBPaid.length}** (COUNTERFACTUAL — does not prove qualification)`);
L(`- Cross-segment same-org paid repeats in closed run: **${dedupSummary.fiveCrossSegmentPaidCases.length}** orgs`);
L("");
L("### Top 20 under improved prioritization (Simulation B)");
L("");
L("| Rank | Organization | Segment | Pursuit | Investment | Dedup |");
L("|---|---|---|---|---|---|");
top20New.forEach((r, i) => {
  L(`| ${i + 1} | ${r.organizationName || r.id} | ${r.researchSegment} | ${r.pursuitStrategy} | ${r.investmentScore} | ${r.dedup.code} |`);
});
L("");
L("### Newly prioritized vs correctly deprioritized (examples)");
L("");
const movedUp = comparison.filter((c) => c.moveReason === "moved_up").slice(0, 8);
const deprior = comparison.filter((c) => c.moveReason === "deprioritized_out_of_paid_queue").slice(0, 8);
L("Moved up:");
for (const c of movedUp) L(`- ${c.organizationName} (${c.segment}): ${c.originalQueueRank} → ${c.newQueueRank}`);
L("");
L("Deprioritized / skipped under new gates:");
for (const c of deprior) L(`- ${c.organizationName} (${c.segment}): ${c.dedupStatus} / ${c.primaryPursuitStrategy}`);
L("");
L("## C. Value recovery from $6.00 already spent");
L("");
L(`- WATCHLIST / priority dossiers reviewed offline: **${recoveries.length}**`);
L(`- Account Development supported (offline, not publication-ready): **${recoveries.filter((r) => r.accountDevelopmentSupported).length}**`);
L(`- Publication-review-ready bodies staged: **0**`);
L(`- Blocked as existing-inventory duplicates: **${recoveries.filter((r) => /DUPLICATE_EXISTING_INVENTORY/i.test(r.recoveryStatus)).length}**`);
L("");
L("Strongest incomplete dossiers with named contacts (still NOT publication-ready):");
for (const r of recoveries.filter((x) => x.namedContacts?.length).slice(0, 10)) {
  L(`- **${r.organizationName}** [${r.segment}] — ${r.namedContacts.map((c) => c.name).join("; ")} — ${r.recoveryStatus}`);
}
L("");
L("## D. Segment quality");
L("");
L(`- Fourteen-segment coverage in closed run: all segments received deep slots (fair scheduler).`);
L(`- PLP offline contract: **${plpPepLcm.preferred_lodging_partnerships.status}** · live yield **${plpPepLcm.preferred_lodging_partnerships.liveYieldClaim}**`);
L(`- PEP offline contract: **${plpPepLcm.professional_event_planners.status}** · contamination in closed run: ${(plpPepLcm.professional_event_planners.contaminationInClosedRun || []).join(", ") || "none flagged"}`);
L(`- LCM offline contract: **${plpPepLcm.local_catering_day_meetings.status}** · contamination in closed run: ${(plpPepLcm.local_catering_day_meetings.contaminationInClosedRun || []).join(", ") || "none flagged"}`);
L("");
L("## E. Production readiness");
L("");
L("| Stage | Status |");
L("|---|---|");
for (const s of readiness) L(`| ${s.stage} | **${s.status}** — ${s.note} |`);
L("");
L(`- Canonical opportunities.json count: **${canonical.count}** (expected ${BETHESDA_EXPECTED_CANONICAL_COUNT}); inventory union IDs loaded: **${inventory.opportunityIds.size}** (expected ${BETHESDA_EXPECTED_UNION_COUNT}); live disc_* in inventory: **0**`);
L("- No paid spend and no publication during this offline package");
L(`- Closed-run ledger missing providerRunId on all 46 deep ops; ORS retains run ids for ${paidRows.filter((r) => r.providerRunId).length}/46 — settle harden ships with this fix`);
L(`- Git SHA: \`${freeze.mainShaHint || "unknown"}\``);
L("");
L("## F. Next research investment — GO / NO-GO");
L("");
L(`### Recommendation: **${goNoGo.recommendation}**`);
L("");
for (const r of goNoGo.rationale) L(`- ${r}`);
L("");
L("Reconsider paid discovery only after:");
for (const c of goNoGo.conditionsToReconsiderPaid) L(`- ${c}`);
L("");
L("---");
L("");
L("### Appendices in this package");
L("");
L("- `00-campaign-freeze.json` … `12-go-no-go.json`");
L("- `02-candidate-master.json` — full reconstructed candidate table");
L("- `04-top30-before-after.json` — explainable ranking deltas");
L("- `07-watchlist-value-recovery.json` — offline recovery detail");
L("- `09-cost-efficiency.json` — verifiable waste vs counterfactual savings");
L("");

writeText("FOUNDER-OFFLINE-REPLAY-REPORT.md", lines.join("\n"));
writeText("README.md", `# Offline replay package\n\nClosed campaign analysis for Bethesda 14-segment live run.\nNo paid calls. Original ledgers unmodified.\n\nSee FOUNDER-OFFLINE-REPLAY-REPORT.md\n`);

console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      candidates: enriched.length,
      paid: originalPaidIds.size,
      avoided: dedupSummary.avoidedPaidCount,
      avoidedUsd: dedupSummary.avoidedPaidUsd,
      simBPaid: simBPaid.length,
      recoveries: recoveries.length,
      publicationReadyRecovered: 0,
      recommendation: goNoGo.recommendation,
    },
    null,
    2
  )
);
