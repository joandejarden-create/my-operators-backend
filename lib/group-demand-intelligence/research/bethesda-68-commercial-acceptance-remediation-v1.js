/**
 * Bethesda 68 — Commercial Acceptance Remediation V1
 *
 * Independently supported factual / disposition patches discovered during
 * hotel-sales commercial acceptance. Preserves all 88 IDs. No maturity
 * promotion, no customer publication, no paid research.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyCommercialSurfaceDisposition,
  attachCommercialSurfaceDisposition,
  COMMERCIAL_SURFACE,
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
  primaryCommercialForbiddenIdList,
} from "../gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
  COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
  hotelOpportunitiesPath,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  writeJsonAtomic,
  researchStageHotelDir,
} from "./gdi-research-stage-persistence-v1.js";

export const BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_V1 =
  "gdi-bethesda-68-commercial-acceptance-remediation-v1";

export const ACCEPTANCE_REMEDIATION_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_20261112";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL = "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1";
const HISTORICAL_FIXTURE_REL =
  "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

/**
 * Exact-field patches only. No invented contacts or demand.
 */
export const ACCEPTANCE_FACTUAL_PATCHES = Object.freeze([
  {
    id: "gdi_opp_accp_annual_meeting_20261001",
    fields: {
      commercialSurfaceDisposition: COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED,
      primaryCommercialEligible: false,
      priority: "DISQUALIFIED",
      priorityFilterKey: "DISQUALIFIED",
      customerVisible: false,
      salesReady: false,
      salesActionKey: "MONITOR",
      salesAction: "Monitor",
      summaryWhat:
        "ACCP Annual Meeting 2026 is hosted in Salt Lake City — outside the Bethesda Marriott market. Named planner Lisa C. Miller remains a valid future-cycle contact if a DC-area edition is later evidenced; do not pursue the 2026 host cycle.",
      recommendedAction:
        "Do not pursue ACCP 2026 (Salt Lake City). Archive this cycle; watch for a future Mid-Atlantic / DC destination announcement before reopening commercial outreach.",
      recommendedSalesAction:
        "Out-of-market host cycle — archive from primary commercial list; retain planner contact for future DC-area cycles only.",
    },
    reason: "OUT_OF_MARKET_SALT_LAKE_CITY_HOST",
  },
  {
    id: "gdi_opp_regulatory_information_conference_20260930",
    fields: {
      summaryWhat:
        "NRC Regulatory Information Conference is hybrid at NRC HQ in Rockville. Bethesda Marriott is a lodging-adjacent overflow candidate for attendees — not the contracted host venue.",
    },
    reason: "REPLACE_PLACEHOLDER_SUMMARY_WITH_GROUNDED_FIT",
  },
  {
    id: "gdi_opp_2027_ddaa_annual_conference_8",
    fields: {
      summaryWhat:
        "2027 DDAA Annual Conference — destination still estimated for the Bethesda / Washington region. Commercial lodging and meeting-space details are not yet publicly confirmed; monitor venue/housing announcements before pitching a room block.",
      hotelFitNote:
        "Destination estimated Bethesda / Washington region; no contracted host hotel evidenced yet. Keep light relationship with Brittany Salazar / NADO-DDAA meetings path; do not sell a firm 2027 block until housing opens.",
    },
    reason: "REPLACE_PLACEHOLDER_SUMMARY_WITH_GROUNDED_MONITOR_POSTURE",
  },
  {
    id: "gdi_opp_potomac_memorial_2027",
    fields: {
      recommendedAction:
        "Contact Kathy Hauschild (tournament@potomacsoccer.org) and/or HBC Event Services to request Bethesda Marriott inclusion on the official Potomac Memorial 2027 stay-to-play housing list; confirm peak nights, team travel radius, and housing-partner deadline.",
      recommendedSalesAction:
        "Pursue stay-to-play housing-list inclusion via tournament director / HBC — not venue displacement.",
    },
    reason: "REPLACE_VENUE_MONITOR_COPY_WITH_HOUSING_LIST_ACTION",
  },
  {
    id: "gdi_opp_loudoun_college_showcase_2027",
    fields: {
      recommendedAction:
        "Qualify with Dave Bucciero / HBC whether Beltway inventory (including Bethesda Marriott) is accepted for the March 2027 Loudoun College Showcase stay-to-play program; if Beltway is out of policy, monitor only — do not force a primary Loudoun-block pitch.",
      recommendedSalesAction:
        "Qualify Beltway housing acceptance before pursuit; Loudoun primary fields are outside Bethesda’s natural stay-to-play radius.",
      hotelFitNote:
        "Loudoun County stay-to-play via HBC. Bethesda Marriott is only relevant if the housing partner accepts Beltway / Montgomery inventory; otherwise watch.",
    },
    reason: "REPLACE_VENUE_MONITOR_COPY_WITH_BELTWAY_QUALIFY_ACTION",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadHistoricalDoc({ root = ROOT } = {}) {
  const stagePath = path.join(
    researchStageHotelDir(BETHESDA_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json",
  );
  if (fs.existsSync(stagePath)) {
    return { path: stagePath, exists: true, doc: JSON.parse(fs.readFileSync(stagePath, "utf8")) };
  }
  const fixturePath = path.join(root, HISTORICAL_FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    return { path: fixturePath, exists: true, doc: JSON.parse(fs.readFileSync(fixturePath, "utf8")) };
  }
  return { path: null, exists: false, doc: null };
}

function mergePatch(before, patchFields) {
  const next = { ...before };
  for (const [k, v] of Object.entries(patchFields || {})) {
    next[k] = v;
  }
  const classification = classifyCommercialSurfaceDisposition(next);
  return attachCommercialSurfaceDisposition({
    ...next,
    commercialSurfaceDisposition:
      patchFields?.commercialSurfaceDisposition ||
      classification.surface ||
      next.commercialSurfaceDisposition,
    primaryCommercialEligible:
      patchFields?.primaryCommercialEligible ??
      classification.primaryCommercialEligible,
  });
}

export function buildAcceptanceRemediationDryRun({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const patchById = new Map(ACCEPTANCE_FACTUAL_PATCHES.map((p) => [p.id, p]));
  const planned = [];
  for (const o of store.opportunities || []) {
    const patch = patchById.get(o.id);
    if (!patch) continue;
    const after = mergePatch(o, patch.fields);
    planned.push({
      id: o.id,
      reason: patch.reason,
      beforeSurface: o.commercialSurfaceDisposition || null,
      afterSurface: after.commercialSurfaceDisposition,
      changedFields: Object.keys(patch.fields),
    });
  }

  const nextCanonical = (store.opportunities || []).map((o) => {
    const patch = patchById.get(o.id);
    return patch ? mergePatch(o, patch.fields) : o;
  });

  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    nextCanonical,
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(union.opportunities, {
    hideLinkedFromList: true,
  });
  const commercial = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  const forbidden = new Set(primaryCommercialForbiddenIdList());
  const leaks = commercial.filter((o) => forbidden.has(o.id)).map((o) => o.id);

  return {
    version: BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_V1,
    predecessorSha: store.sha256,
    expectedPredecessorSha: COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
    storeCount: store.count,
    planned,
    projectedCommercialCount: commercial.length,
    forbiddenLeaks: leaks,
    protectedScalingPresent: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
      union.opportunities.some((o) => o.id === id),
    ),
    unionCount: union.opportunities.length,
  };
}

export function applyAcceptanceRemediationAtomic({
  root = ROOT,
  founderAuthorization = false,
  approvalToken = null,
  dryRun = true,
} = {}) {
  if (!founderAuthorization || approvalToken !== ACCEPTANCE_REMEDIATION_APPROVAL_TOKEN) {
    return {
      ok: false,
      code: "FOUNDER_AUTHORIZATION_REQUIRED",
      writeApplied: false,
      requiredToken: ACCEPTANCE_REMEDIATION_APPROVAL_TOKEN,
    };
  }

  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!store.exists || store.count !== 59) {
    return { ok: false, code: "UNEXPECTED_CANONICAL_STORE", count: store.count };
  }
  if (!ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256)) {
    return { ok: false, code: "UNEXPECTED_STORE_SHA", sha256: store.sha256 };
  }

  const dry = buildAcceptanceRemediationDryRun({ root });
  if (dry.forbiddenLeaks.length) {
    return { ok: false, code: "PROJECTED_FORBIDDEN_LEAK", leaks: dry.forbiddenLeaks };
  }
  if (dryRun) {
    return { ok: true, dryRun: true, writeApplied: false, ...dry };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-68-commercial-acceptance-remediation",
  });

  const patchById = new Map(ACCEPTANCE_FACTUAL_PATCHES.map((p) => [p.id, p]));
  const nextCanonical = store.opportunities.map((o) => {
    const patch = patchById.get(o.id);
    return patch ? mergePatch(o, patch.fields) : o;
  });

  assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextCanonical,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  });
  assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: 59,
    nextCount: nextCanonical.length,
  });

  const appliedAt = new Date().toISOString();
  writeJsonAtomic(store.path, {
    ...(store.doc || {}),
    opportunities: nextCanonical,
    updatedAt: appliedAt,
    commercialAcceptanceRemediation: {
      version: BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_V1,
      appliedAt,
      predecessorSha: store.sha256,
      patchedIds: ACCEPTANCE_FACTUAL_PATCHES.map((p) => p.id),
    },
  });

  const hist = loadHistoricalDoc({ root });
  let historicalTouched = 0;
  if (hist.exists && hist.doc) {
    const histOps = Array.isArray(hist.doc.opportunities)
      ? hist.doc.opportunities
      : Array.isArray(hist.doc)
        ? hist.doc
        : [];
    let changed = false;
    const nextHist = histOps.map((o) => {
      const patch = patchById.get(text(o.id));
      if (!patch) return o;
      changed = true;
      historicalTouched += 1;
      return mergePatch(o, patch.fields);
    });
    if (changed) {
      const payload = Array.isArray(hist.doc.opportunities)
        ? { ...hist.doc, opportunities: nextHist, updatedAt: appliedAt }
        : nextHist;
      writeJsonAtomic(hist.path, payload);
    }
  }

  const afterSha = sha256File(hotelOpportunitiesPath(BETHESDA_HOTEL_ID, { root }));
  const reportDir = path.join(root, REPORT_REL);
  fs.mkdirSync(reportDir, { recursive: true });
  writeJsonAtomic(path.join(reportDir, "06-acceptance-remediation-applied.json"), {
    version: BETHESDA_68_COMMERCIAL_ACCEPTANCE_REMEDIATION_V1,
    appliedAt,
    predecessorSha: store.sha256,
    afterSha,
    backupPath: backup.backupPath,
    planned: dry.planned,
    projectedCommercialCount: dry.projectedCommercialCount,
    historicalTouched,
    paidAuthorized: false,
    publicationAuthorized: false,
    maturityPromotionApplied: false,
  });

  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    predecessorSha: store.sha256,
    afterSha,
    backupPath: backup.backupPath,
    planned: dry.planned,
    projectedCommercialCount: dry.projectedCommercialCount,
    historicalTouched,
  };
}
