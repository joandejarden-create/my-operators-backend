/**
 * Scaling Companies Intelligent Lifecycle V1
 *
 * Orchestration decisions for the existing hotel-generic Full E2E runner.
 * Does NOT create a new research engine, runner mode, or customer-facing status.
 *
 * Decisions: CONTINUE_RESEARCH | REFRESH | REQUALIFY | MONITOR | EXCLUDE | NO_ACTION
 * Plus scheduleNetNewDiscovery when commercially justified.
 *
 * Fail-closed: never invents demand/contacts; never auto-authorizes paid/publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";
import { isScalingCompanyOpportunity } from "../workflow/scaling-companies-research-v1.js";
import { evaluateScalingPublicationQualification } from "../scaling-publication-qualification-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const SCALING_INTELLIGENT_LIFECYCLE_V1 =
  "gdi-scaling-companies-intelligent-lifecycle-v1";

export const LIFECYCLE_DECISION = Object.freeze({
  CONTINUE_RESEARCH: "CONTINUE_RESEARCH",
  REFRESH: "REFRESH",
  REQUALIFY: "REQUALIFY",
  MONITOR: "MONITOR",
  EXCLUDE: "EXCLUDE",
  NO_ACTION: "NO_ACTION",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const WATERSTONE = "recgMYovrrZDJMqzX";
const BETHESDA = "recLuxvwwxID7U2B8";
const WYNDHAM = "recnPB3Q02NO8Ma1o";

/** Known unpublished HOLD orgs that must not be treated as published NET_NEW. */
export const WATERSTONE_HOLD_SEED = Object.freeze([
  {
    organizationName: "MDVIP",
    reasonCodes: ["hold_contact", "js_empty_official_pages"],
    preferredDecision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
    researchFocus: ["official_contact_route", "hq_operations_owner"],
  },
  {
    organizationName: "Innovative",
    reasonCodes: ["identity_domain_unconfirmed", "wrong_host_rejected"],
    preferredDecision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
    researchFocus: ["confirm_legal_entity_and_official_domain", "then_contact_route"],
    gateBeforeContactEnrichment: true,
  },
  {
    organizationName: "Cresa",
    reasonCodes: ["hold_contact", "js_blocked_official_pages"],
    preferredDecision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
    researchFocus: ["official_contact_route", "local_office_phone"],
  },
]);

const STALE_PUBLISHED_DAYS = 90;
const REFRESH_MIN_DAYS = 45;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function orgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function daysSince(iso, nowMs = Date.now()) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return Math.floor((nowMs - t) / (24 * 60 * 60 * 1000));
}

function isPublishedReviewed(o = {}) {
  return (
    o.prospectHotelFacing === true &&
    text(o.prospectPublicationBasis) === "REVIEWED_PACKAGE_V1"
  );
}

function isHoldLabel(label = "") {
  const s = text(label).toUpperCase();
  return s === "HOLD" || s.startsWith("HOLD_");
}

function extractStoppingReasons(memory, orgName) {
  const mem = memory?.memory || memory || null;
  if (!mem || typeof mem !== "object") return [];
  const key = orgKey(orgName);
  const bags = [];
  for (const field of [
    "stoppingReasons",
    "failedStrategies",
    "exhaustedSources",
    "candidates",
    "organizations",
    "holds",
  ]) {
    const v = mem[field];
    if (Array.isArray(v)) bags.push(...v);
    else if (v && typeof v === "object") bags.push(v);
  }
  const out = [];
  for (const row of bags) {
    if (!row || typeof row !== "object") continue;
    const name = orgKey(row.organizationName || row.org || row.company || "");
    if (name && name !== key && !key.includes(name) && !name.includes(key)) continue;
    if (row.stoppingReason) out.push(String(row.stoppingReason));
    if (row.reason) out.push(String(row.reason));
    if (Array.isArray(row.failedStrategies)) {
      out.push(...row.failedStrategies.map(String));
    }
  }
  return [...new Set(out)].slice(0, 12);
}

/**
 * Load unpublished HOLD / research candidates from saved Full E2E artifacts.
 * Does not invent rows — only reads prior maturation/reconciliation evidence.
 */
export function loadSavedHoldAndResearchCandidates(hotelId, { reportsRoot = ROOT } = {}) {
  const id = text(hotelId);
  const holds = [];
  const seen = new Set();

  const pushHold = (row = {}, sourcePath = "") => {
    const org = text(row.organizationName || row.company || row.org);
    if (!org) return;
    const k = orgKey(org);
    if (seen.has(k)) return;
    seen.add(k);
    holds.push({
      organizationName: org,
      opportunityId: row.opportunityId || row.id || row.opportunityPayload?.id || null,
      publicationAuditLabel:
        row.publicationAuditLabel ||
        row.publicationReadiness ||
        row.prospectCustomerLabel ||
        "HOLD_CONTACT",
      sourceUrl: row.sourceUrl || row.url || row.opportunityPayload?.officialSource || null,
      sourcePath,
      unpublished: true,
      identityConcern: /innovative/i.test(org),
      gateBeforeContactEnrichment: /innovative/i.test(org),
    });
  };

  // Hotel-specific known seed (Waterstone HOLD trio)
  if (id === WATERSTONE) {
    for (const seed of WATERSTONE_HOLD_SEED) {
      pushHold(
        {
          organizationName: seed.organizationName,
          publicationAuditLabel: "HOLD_CONTACT",
          identityConcern: seed.gateBeforeContactEnrichment === true,
        },
        "WATERSTONE_HOLD_SEED",
      );
    }
  }

  // Hotel-scoped artifacts only — never attach Waterstone HOLD orgs to peer hotels.
  const candidateFiles =
    id === WATERSTONE
      ? [
          "reports/gdi-waterstone-scaling-contact-recovery-v1/12-reprocessed-maturation.json",
          "reports/gdi-waterstone-scaling-companies-full-e2e-closure-v1/09-hold-candidate-verification.json",
          "reports/gdi-scaling-company-runs/scaling-20261026-183321-a6f40c26/05-reconciliation.json",
          "reports/gdi-scaling-company-runs/scaling-20261026-183321-a6f40c26/12-discovery-handoff-maturation.json",
        ]
      : [];

  for (const rel of candidateFiles) {
    const abs = path.join(reportsRoot, rel);
    if (!fs.existsSync(abs)) continue;
    let doc;
    try {
      doc = JSON.parse(fs.readFileSync(abs, "utf8").replace(/^\uFEFF/, ""));
    } catch {
      continue;
    }
    if (Array.isArray(doc.holdOrgs)) {
      for (const name of doc.holdOrgs) pushHold({ organizationName: name }, rel);
    }
    if (Array.isArray(doc.stillHoldContact)) {
      for (const name of doc.stillHoldContact) pushHold({ organizationName: name }, rel);
    }
    const matured =
      doc.maturation?.matured ||
      doc.matured ||
      doc.netNewThisRun ||
      doc.rows ||
      [];
    if (Array.isArray(matured)) {
      for (const row of matured) {
        const label =
          row.publicationAuditLabel ||
          row.publicationReadiness ||
          row.opportunityPayload?.publicationAuditLabel ||
          "";
        if (isHoldLabel(label) || row.unpublished === true) {
          pushHold(row, rel);
        }
      }
    }
  }

  // Only return holds that are not already published in caller inventory —
  // caller filters against published org keys.
  return holds;
}

function commercialScore(row = {}) {
  let score = 40;
  if (row.published) score += 10;
  if (row.usableContactRoute) score += 15;
  if (row.localConnection) score += 10;
  if (row.demandVerified) score += 20;
  if (row.demandInferred) score += 8;
  if (row.hold) score += 12; // unresolved commercial potential still matters
  if (row.identityConcern) score += 18; // unblock before contact spend
  if (row.missingContact) score += 14;
  if (row.staleDays != null && row.staleDays > STALE_PUBLISHED_DAYS) score += 10;
  if (row.salesFeedbackNegative) score -= 25;
  if (row.priorResearchExhausted) score -= 12;
  if (row.excludedLike) score -= 40;
  return Math.max(0, Math.min(100, score));
}

function researchValueScore(row = {}) {
  // Likelihood further research improves actionability
  let v = 30;
  if (row.identityConcern) v += 35;
  if (row.hold && row.missingContact) v += 25;
  if (row.hold && !row.priorResearchExhausted) v += 15;
  if (row.published && row.staleDays != null && row.staleDays > STALE_PUBLISHED_DAYS) {
    v += 20;
  }
  if (row.published && row.usableContactRoute && (row.staleDays || 0) < REFRESH_MIN_DAYS) {
    v -= 25;
  }
  if (row.priorResearchExhausted && !row.identityConcern) v -= 20;
  return Math.max(0, Math.min(100, v));
}

/**
 * Classify one opportunity / HOLD candidate into a lifecycle decision.
 */
export function classifyScalingLifecycleItem(item = {}, opts = {}) {
  const nowMs = opts.nowMs || Date.now();
  const reasons = [];
  const org = text(item.organizationName || item.company);
  const published = item.published === true || isPublishedReviewed(item);
  const label = text(
    item.publicationAuditLabel || item.prospectCustomerLabel || item.publicationReadiness,
  ).toUpperCase();
  const route = item.usableContactRoute === true
    ? { ok: true }
    : hasUsableContactRoute(item);
  const missingContact = route.ok !== true;
  const staleDays =
    item.staleDays != null
      ? item.staleDays
      : daysSince(
          item.lastResearchedAt ||
            item.maturedAt ||
            item.updatedAt ||
            item.publishedAt ||
            item.lastPublishedUpdateAt,
          nowMs,
        );
  const stopping = item.stoppingReasons || [];
  const priorExhausted =
    item.priorResearchExhausted === true ||
    stopping.some((s) => /exhaust|no_new|repeat|failed_same/i.test(String(s)));
  const identityConcern =
    item.identityConcern === true ||
    item.gateBeforeContactEnrichment === true ||
    /innovative/i.test(org) && (item.unpublished === true || isHoldLabel(label));
  const excludedLike =
    item.excluded === true ||
    /INVALID|DUPLICATE|EXCLUDE|UNSUPPORTED/i.test(label) ||
    text(item.priority).toUpperCase() === "DISQUALIFIED";

  if (excludedLike) {
    reasons.push("invalid_or_disqualified");
    return {
      decision: LIFECYCLE_DECISION.EXCLUDE,
      reasons,
      org,
      commercialScore: 0,
      researchValueScore: 0,
    };
  }

  if (identityConcern) {
    reasons.push("identity_or_domain_must_be_resolved_before_contact_enrichment");
    if (priorExhausted) {
      reasons.push("prior_strategies_noted_but_identity_still_open");
    }
    return {
      decision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
      reasons,
      org,
      researchFocus: ["confirm_legal_entity_and_official_domain"],
      gateBeforeContactEnrichment: true,
      commercialScore: commercialScore({
        hold: true,
        identityConcern: true,
        missingContact,
      }),
      researchValueScore: researchValueScore({
        identityConcern: true,
        hold: true,
        missingContact,
        priorResearchExhausted: priorExhausted,
      }),
    };
  }

  if (!published && (isHoldLabel(label) || item.unpublished === true || item.hold === true)) {
    if (missingContact || /HOLD_CONTACT|HOLD_DEMAND|HOLD_RESEARCH|HOLD_ACCOUNT/i.test(label)) {
      reasons.push(missingContact ? "hold_missing_usable_contact_route" : `hold_label:${label}`);
      if (priorExhausted) {
        reasons.push("prior_research_exhausted_monitor_unless_new_source");
        return {
          decision: LIFECYCLE_DECISION.MONITOR,
          reasons,
          org,
          commercialScore: commercialScore({ hold: true, missingContact, priorResearchExhausted: true }),
          researchValueScore: researchValueScore({
            hold: true,
            missingContact,
            priorResearchExhausted: true,
          }),
        };
      }
      return {
        decision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
        reasons,
        org,
        researchFocus: item.researchFocus || ["official_contact_route", "demand_mechanism"],
        commercialScore: commercialScore({ hold: true, missingContact }),
        researchValueScore: researchValueScore({ hold: true, missingContact }),
      };
    }
  }

  if (published) {
    // One-named-buyer publication rule: published drawers that fail Scaling
    // qualification (e.g. false role-title "buyer", media-only route) must
    // CONTINUE_RESEARCH — do not treat as current_sufficient / NO_ACTION.
    const scalingQual = evaluateScalingPublicationQualification({
      ...item,
      researchSegment: item.researchSegment || "scaling_companies",
    });
    const namedMissing =
      scalingQual?.publicationQualified !== true &&
      (scalingQual?.gaps || []).includes("ONE_NAMED_RELEVANT_BUYER");
    const primaryLooksFake =
      text(item.contactName || item.primaryContact?.name) &&
      !looksLikePersonName(item.contactName || item.primaryContact?.name);
    if (namedMissing || primaryLooksFake) {
      reasons.push(
        namedMissing
          ? "published_missing_named_relevant_buyer"
          : "published_primary_not_person_name",
      );
      return {
        decision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
        reasons,
        org,
        researchFocus: ["named_buyer", "contact_route", "demand_mechanism"],
        commercialScore: commercialScore({
          published: true,
          missingContact: true,
        }),
        researchValueScore: researchValueScore({
          published: true,
          missingContact: true,
        }),
      };
    }
    const demandThin =
      !text(item.guestroomStatus) &&
      !(Array.isArray(item.inferredHotelDemand) && item.inferredHotelDemand.length);
    if (demandThin && missingContact) {
      reasons.push("published_but_material_gaps_requalify");
      return {
        decision: LIFECYCLE_DECISION.REQUALIFY,
        reasons,
        org,
        commercialScore: commercialScore({ published: true, missingContact }),
        researchValueScore: researchValueScore({ published: true, missingContact }),
      };
    }
    if (staleDays != null && staleDays >= STALE_PUBLISHED_DAYS) {
      reasons.push(`published_stale_${staleDays}d`);
      return {
        decision: LIFECYCLE_DECISION.REFRESH,
        reasons,
        org,
        commercialScore: commercialScore({
          published: true,
          usableContactRoute: route.ok,
          staleDays,
        }),
        researchValueScore: researchValueScore({ published: true, staleDays }),
      };
    }
    if (staleDays != null && staleDays >= REFRESH_MIN_DAYS && missingContact) {
      reasons.push("published_aging_missing_contact");
      return {
        decision: LIFECYCLE_DECISION.REFRESH,
        reasons,
        org,
        commercialScore: commercialScore({
          published: true,
          missingContact: true,
          staleDays,
        }),
        researchValueScore: researchValueScore({
          published: true,
          missingContact: true,
          staleDays,
        }),
      };
    }
    reasons.push("published_current_sufficient");
    return {
      decision: LIFECYCLE_DECISION.NO_ACTION,
      reasons,
      org,
      commercialScore: commercialScore({
        published: true,
        usableContactRoute: route.ok,
        staleDays,
      }),
      researchValueScore: researchValueScore({
        published: true,
        usableContactRoute: route.ok,
        staleDays,
      }),
    };
  }

  // Unclassified existing scaling row
  if (missingContact) {
    reasons.push("existing_unpublished_missing_contact");
    return {
      decision: LIFECYCLE_DECISION.CONTINUE_RESEARCH,
      reasons,
      org,
      commercialScore: commercialScore({ missingContact: true }),
      researchValueScore: researchValueScore({ hold: true, missingContact: true }),
    };
  }

  reasons.push("existing_sufficient_monitor");
  return {
    decision: LIFECYCLE_DECISION.MONITOR,
    reasons,
    org,
    commercialScore: commercialScore({ usableContactRoute: true }),
    researchValueScore: 20,
  };
}

/**
 * Build full lifecycle plan for a hotel from inventory + saved HOLDs + memory.
 */
export function buildScalingIntelligentLifecyclePlan({
  hotelId,
  hotelProfile = null,
  scalingOpportunities = [],
  researchMemory = null,
  savedHolds = null,
  maxResearchQueue = 8,
  nowMs = Date.now(),
  reportsRoot = ROOT,
} = {}) {
  const id = text(hotelId);
  const published = [];
  const inventory = [];
  const publishedKeys = new Set();

  for (const o of scalingOpportunities || []) {
    if (!isScalingCompanyOpportunity(o) && o.researchSegment !== "scaling_companies") {
      // still accept explicit scaling rows from runner summarize
      if (!/scaling/i.test(text(o.researchSegment || o.segment))) continue;
    }
    const publishedFlag = isPublishedReviewed(o);
    const route = hasUsableContactRoute(o);
    const stopping = extractStoppingReasons(researchMemory, o.organizationName);
    const row = {
      ...o,
      published: publishedFlag,
      usableContactRoute: route.ok === true,
      missingContact: route.ok !== true,
      stoppingReasons: stopping,
      priorResearchExhausted: stopping.length >= 3,
      localConnection: Boolean(text(o.localConnection || o.localPresence)),
      demandVerified: /VERIFIED/i.test(text(o.demandStatus || o.demandClass)),
      demandInferred:
        Array.isArray(o.inferredHotelDemand) && o.inferredHotelDemand.length > 0,
    };
    inventory.push(row);
    if (publishedFlag) {
      published.push(row);
      publishedKeys.add(orgKey(o.organizationName));
    }
  }

  const holdsRaw =
    savedHolds ||
    loadSavedHoldAndResearchCandidates(id, { reportsRoot });
  const holds = [];
  for (const h of holdsRaw) {
    const k = orgKey(h.organizationName);
    if (publishedKeys.has(k)) continue; // already published — never HOLD/NET_NEW again
    // skip if already in inventory under another id
    if (inventory.some((r) => orgKey(r.organizationName) === k)) continue;
    const seed = WATERSTONE_HOLD_SEED.find(
      (s) => orgKey(s.organizationName) === k,
    );
    holds.push({
      ...h,
      hold: true,
      unpublished: true,
      published: false,
      missingContact: true,
      usableContactRoute: false,
      identityConcern: h.identityConcern || seed?.gateBeforeContactEnrichment === true,
      gateBeforeContactEnrichment:
        h.gateBeforeContactEnrichment || seed?.gateBeforeContactEnrichment === true,
      researchFocus: seed?.researchFocus || h.researchFocus,
      stoppingReasons: extractStoppingReasons(researchMemory, h.organizationName),
    });
  }

  const decisions = [];
  for (const row of [...inventory, ...holds]) {
    const classified = classifyScalingLifecycleItem(row, { nowMs });
    decisions.push({
      organizationName: text(row.organizationName),
      opportunityId: row.id || row.opportunityId || null,
      published: row.published === true,
      unpublishedHold: row.unpublished === true || row.hold === true,
      decision: classified.decision,
      reasons: classified.reasons,
      researchFocus: classified.researchFocus || row.researchFocus || [],
      gateBeforeContactEnrichment: classified.gateBeforeContactEnrichment === true,
      commercialScore: classified.commercialScore,
      researchValueScore: classified.researchValueScore,
      priorityScore: Math.round(
        classified.commercialScore * 0.45 + classified.researchValueScore * 0.55,
      ),
    });
  }

  // Sort research queue: higher priority first; do not auto-prefer NET_NEW over HOLD completion
  const actionable = decisions
    .filter((d) =>
      [
        LIFECYCLE_DECISION.CONTINUE_RESEARCH,
        LIFECYCLE_DECISION.REFRESH,
        LIFECYCLE_DECISION.REQUALIFY,
      ].includes(d.decision),
    )
    .sort((a, b) => b.priorityScore - a.priorityScore);

  const researchQueue = actionable.slice(0, Math.max(0, maxResearchQueue));

  const noActionPublished = decisions.filter(
    (d) => d.published && d.decision === LIFECYCLE_DECISION.NO_ACTION,
  );
  const monitor = decisions.filter((d) => d.decision === LIFECYCLE_DECISION.MONITOR);
  const excluded = decisions.filter((d) => d.decision === LIFECYCLE_DECISION.EXCLUDE);

  // NET_NEW discovery: schedule when inventory is thin OR research queue has spare capacity
  // after prioritizing CONTINUE/REFRESH/REQUALIFY — never treat published orgs as NET_NEW.
  const publishedCount = published.length;
  const holdContinueCount = researchQueue.filter((d) => d.unpublishedHold).length;
  const scheduleNetNewDiscovery =
    publishedCount + holds.length < 12 ||
    (researchQueue.length < maxResearchQueue && holdContinueCount < maxResearchQueue);
  const netNewSlots = scheduleNetNewDiscovery
    ? Math.max(0, maxResearchQueue - researchQueue.length)
    : 0;

  const doNotRediscoverAsNetNew = [...publishedKeys];

  return {
    version: SCALING_INTELLIGENT_LIFECYCLE_V1,
    hotelId: id,
    hotelName: hotelProfile?.displayName || hotelProfile?.hotelName || null,
    generatedAt: new Date(nowMs).toISOString(),
    inventory: {
      scalingInAirtable: inventory.length,
      publishedReviewed: publishedCount,
      unpublishedHoldsLoaded: holds.length,
      decisions: decisions.length,
    },
    decisions,
    researchQueue,
    monitor,
    excluded,
    noActionPublished,
    discoveryPlan: {
      scheduleNetNewDiscovery,
      netNewSlots,
      doNotRediscoverAsNetNew,
      note:
        "Published accounts must never be rediscovered as GENUINELY_NET_NEW. HOLD continuation and justified refresh take queue priority over raw discovery.",
    },
    waterstoneSpecific:
      id === WATERSTONE
        ? {
            publishedExpected: ["CP Group", "WSHC+B", "Hair Club", "MPLT Healthcare"],
            holdExpected: ["MDVIP", "Innovative", "Cresa"],
            innovativeIdentityGate: decisions.find(
              (d) => orgKey(d.organizationName) === "innovative",
            ),
          }
        : null,
    regressionHotelIds: [WATERSTONE, BETHESDA, WYNDHAM],
  };
}

/**
 * Select existing inventory IDs for ORS/refresh from the lifecycle plan.
 */
export function selectLifecycleResearchTargets(plan, { max = 8 } = {}) {
  const queue = plan?.researchQueue || [];
  return queue
    .filter((d) =>
      [
        LIFECYCLE_DECISION.CONTINUE_RESEARCH,
        LIFECYCLE_DECISION.REFRESH,
        LIFECYCLE_DECISION.REQUALIFY,
      ].includes(d.decision),
    )
    .slice(0, max)
    .map((d) => ({
      opportunityId: d.opportunityId,
      organizationName: d.organizationName,
      decision: d.decision,
      gateBeforeContactEnrichment: d.gateBeforeContactEnrichment === true,
      researchFocus: d.researchFocus || [],
      unpublishedHold: d.unpublishedHold === true,
    }));
}

export function summarizeLifecyclePlan(plan) {
  const counts = {};
  for (const d of plan?.decisions || []) {
    counts[d.decision] = (counts[d.decision] || 0) + 1;
  }
  return {
    version: plan?.version,
    hotelId: plan?.hotelId,
    decisionCounts: counts,
    researchQueueSize: plan?.researchQueue?.length || 0,
    scheduleNetNewDiscovery: plan?.discoveryPlan?.scheduleNetNewDiscovery === true,
    netNewSlots: plan?.discoveryPlan?.netNewSlots || 0,
    doNotRediscoverCount: plan?.discoveryPlan?.doNotRediscoverAsNetNew?.length || 0,
  };
}
