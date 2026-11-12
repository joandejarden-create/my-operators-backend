/**
 * Bethesda 88 — Contact Completion Wave 2 (post PR #189).
 *
 * Zero-paid named-contact recoveries for remaining commercially credible gaps.
 * Historical research-stage: protected merge apply.
 * Canonical 59: dry-run manifest only until founder authorizes.
 *
 * No paid research. No Sales Ready auto-promotion. No customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
  applyHistoricalContactEnrichment,
  COVERAGE_BUCKET,
} from "./bethesda-88-contact-quality-completion-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { writeJsonAtomic } from "./gdi-research-stage-persistence-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../account-development-eligibility-v1.js";
import { runGdiOpportunityFullE2eQualityOrchestration } from "../gdi-opportunity-full-e2e-quality-orchestrator-v1.js";

export const BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1 =
  "gdi-bethesda-88-contact-completion-wave2-v1";

export const WAVE2_CANONICAL_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_WAVE2_CONTACT_PATCHES_20261112";

export const WAVE2_CANONICAL_IDS = Object.freeze([
  "gdi_opp_nado_ddaa_washcon_2028",
  "gdi_opp_nado_ddaa_washcon_2027_overflow",
  "gdi_opp_series_high_risk_high_reward_research_symposium_2027",
  "gdi_opp_acvnu_renal_week_20270412",
  "gdi_opp_regulatory_information_conference_20260930",
]);

/** Founder-approved Wave 2 canonical contact writes (2026-11-12). ACVNU excluded pending Gmail classification correction. */
export const WAVE2_FOUNDER_APPROVED_CANONICAL_IDS = Object.freeze([
  "gdi_opp_nado_ddaa_washcon_2028",
  "gdi_opp_nado_ddaa_washcon_2027_overflow",
  "gdi_opp_series_high_risk_high_reward_research_symposium_2027",
  "gdi_opp_regulatory_information_conference_20260930",
]);

export const WAVE2_ACVNU_DEFERRED_ID = "gdi_opp_acvnu_renal_week_20270412";

export const WAVE2_HISTORICAL_IDS = Object.freeze([
  "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
  "gdi_s4912_66a1f89e5afb",
  "gdi_s4912_364713d41c67",
  "gdi_beth_v16_evt_90a6ddcb10ca",
  "gdi_beth_v16_evt_0ab91e208405",
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL = "reports/gdi-bethesda-88-contact-completion-e2e-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function coverageCounts(opportunities = []) {
  let named = 0;
  let direct = 0;
  let orgRoute = 0;
  for (const o of opportunities) {
    const s = extractContactSnapshot(o);
    if (s.named) named += 1;
    if (s.hasDirectEmail) direct += 1;
    if (s.hasOrgRoute) orgRoute += 1;
  }
  return { named, direct, orgRoute, total: opportunities.length };
}

function uniqueMissingNamed(opportunities = []) {
  const fam = applyBethesdaCommercialEventFamilyPresentation(opportunities, {
    hideLinkedFromList: true,
  });
  const missing = [];
  for (const row of fam.allAnnotated || []) {
    const snap = extractContactSnapshot(row);
    if (snap.named) continue;
    if (
      row.commercialEventFamilyRole === "LINKED_RESEARCH" ||
      row.commercialEventFamilyRole === "QUARANTINED"
    ) {
      continue;
    }
    missing.push({
      id: row.id,
      title: text(row.title).slice(0, 100),
      org: row.organizationName || null,
      familyRole: row.commercialEventFamilyRole || "STANDALONE",
      coverage: snap.coverage,
      listVisible: row.listVisible !== false,
    });
  }
  return {
    uniqueCommercial: fam.counts.uniqueCommercialOpportunities,
    missing,
    missingCount: missing.length,
  };
}

export function buildWave2CanonicalDryRunPatches({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const patches = [];
  for (const id of WAVE2_CANONICAL_IDS) {
    const before = store.opportunities.find((o) => text(o.id) === id);
    if (!before) {
      patches.push({ id, ok: false, reason: "ID_NOT_IN_STORE" });
      continue;
    }
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    if (!evidence || evidence.rejected) {
      patches.push({ id, ok: false, reason: "EVIDENCE_MISSING" });
      continue;
    }
    const beforeSnap = extractContactSnapshot(before);
    const merged = mergeContactAntiClobber(before, evidence);
    if (evidence.hotelFitHonesty) {
      merged.hotelFitHonesty = evidence.hotelFitHonesty;
      merged.hotelFitNote = evidence.hotelFitNote || merged.hotelFitNote || null;
    }
    const afterSnap = extractContactSnapshot(merged);
    patches.push({
      id,
      ok: true,
      requiresFounderApproval: true,
      before: {
        name: beforeSnap.name,
        coverage: beforeSnap.coverage,
        email: beforeSnap.email,
        orgRoute: beforeSnap.orgEmail || beforeSnap.orgRoute,
      },
      after: {
        name: afterSnap.name,
        coverage: afterSnap.coverage,
        email: afterSnap.email,
        orgRoute: afterSnap.orgEmail || afterSnap.orgRoute,
        title: afterSnap.title,
      },
      hotelFitHonesty: evidence.hotelFitHonesty || null,
      sourceUrl: evidence.primaryContact?.sourceUrl || null,
      fieldDiff: {
        primaryContactName: { from: beforeSnap.name, to: afterSnap.name },
        primaryContactEmail: { from: beforeSnap.email, to: afterSnap.email },
        organizationContactRoute: {
          from: beforeSnap.orgRoute,
          to: afterSnap.orgEmail || afterSnap.orgRoute,
        },
      },
    });
  }
  return {
    version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
    authorized: false,
    storeSha256: store.sha256,
    expectedStoreSha256: CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
    approvalToken: WAVE2_CANONICAL_APPROVAL_TOKEN,
    patchCount: patches.filter((p) => p.ok).length,
    patches,
  };
}

/**
 * Apply wave-2 canonical contact patches. Requires founderAuthorization + token.
 * Pass onlyIds to apply a founder-approved subset (e.g. exclude deferred ACVNU).
 */
export function applyWave2CanonicalContactPatches({
  root = ROOT,
  founderAuthorization = false,
  approvalToken = null,
  dryRun = true,
  onlyIds = null,
} = {}) {
  if (!founderAuthorization || approvalToken !== WAVE2_CANONICAL_APPROVAL_TOKEN) {
    return {
      ok: false,
      code: "FOUNDER_AUTHORIZATION_REQUIRED",
      writeApplied: false,
    };
  }
  const targetIds = Array.isArray(onlyIds) && onlyIds.length
    ? onlyIds.map((id) => text(id)).filter(Boolean)
    : [...WAVE2_CANONICAL_IDS];
  for (const id of targetIds) {
    if (!WAVE2_CANONICAL_IDS.includes(id)) {
      return {
        ok: false,
        code: "ID_NOT_IN_WAVE2_CANONICAL_SET",
        id,
        writeApplied: false,
      };
    }
  }
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (store.sha256 !== CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256) {
    // Idempotent if the requested wave2 ids are already applied.
    const already = targetIds.every((id) => {
      const row = store.opportunities.find((o) => text(o.id) === id);
      return row?.approvedContactUpdate?.wave === "wave2_20261112";
    });
    if (!already) {
      return {
        ok: false,
        code: "UNEXPECTED_STORE_SHA",
        sha256: store.sha256,
        expected: CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
      };
    }
  }
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      onlyIds: targetIds,
      manifest: buildWave2CanonicalDryRunPatches({ root }),
    };
  }

  // Precondition: approved ids must still lack a verified named person (anti-clobber / no overwrite).
  const precheckFailures = [];
  for (const id of targetIds) {
    const before = store.opportunities.find((o) => text(o.id) === id);
    if (!before) {
      precheckFailures.push({ id, reason: "ID_NOT_IN_STORE" });
      continue;
    }
    const snap = extractContactSnapshot(before);
    if (snap.named) {
      precheckFailures.push({
        id,
        reason: "ALREADY_HAS_NAMED_CONTACT",
        name: snap.name,
      });
    }
  }
  if (precheckFailures.length) {
    return {
      ok: false,
      code: "BEFORE_PRECONDITION_FAILED",
      writeApplied: false,
      precheckFailures,
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-wave2-contact-patches",
  });
  const byId = new Map(store.opportunities.map((o) => [text(o.id), o]));
  const applied = [];
  for (const id of targetIds) {
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    const before = byId.get(id);
    if (!before || !evidence || evidence.rejected) continue;
    const beforeSnap = extractContactSnapshot(before);
    const preservedOfficialUrl =
      text(before.contactOfficialUrl) ||
      text(before.contactSourceUrl) ||
      text(before.primaryContact?.sourceUrl) ||
      null;
    let merged = mergeContactAntiClobber(before, evidence);
    if (evidence.hotelFitHonesty) {
      merged.hotelFitHonesty = evidence.hotelFitHonesty;
      merged.hotelFitNote = evidence.hotelFitNote || null;
    }
    // Preserve original official/source URLs when email routes are added.
    if (preservedOfficialUrl) {
      merged.contactOfficialUrl = preservedOfficialUrl;
      if (!text(merged.contactSourceUrl)) {
        merged.contactSourceUrl =
          text(evidence.primaryContact?.sourceUrl) || preservedOfficialUrl;
      }
      merged.contactProvenanceUrls = Array.from(
        new Set(
          [
            preservedOfficialUrl,
            text(evidence.primaryContact?.sourceUrl),
            ...(Array.isArray(evidence.primaryContact?.provenanceUrls)
              ? evidence.primaryContact.provenanceUrls.map((u) => text(u))
              : []),
          ].filter(Boolean)
        )
      );
    }
    // Contact recovery alone must not promote commercial maturity / publication.
    merged.salesReady = false;
    merged.customerFacing = false;
    merged.customerVisible = false;
    merged.publicationApproved = false;
    merged.approvedContactUpdate = {
      version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
      wave: "wave2_20261112",
      appliedAt: new Date().toISOString(),
      founderAuthorized: true,
      approvedIdsSubset: targetIds,
    };
    byId.set(id, merged);
    applied.push({
      id,
      before: beforeSnap,
      after: extractContactSnapshot(merged),
      hotelFitHonesty: evidence.hotelFitHonesty || null,
      preservedOfficialUrl,
    });
  }
  const nextOps = store.opportunities.map((o) => byId.get(text(o.id)) || o);
  assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextOps,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  });
  assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: 59,
    nextCount: nextOps.length,
  });
  // Historical 29 must remain outside this 59-row store; total union stays 88.
  if (nextOps.length !== 59) {
    return {
      ok: false,
      code: "CANONICAL_COUNT_DRIFT",
      writeApplied: false,
      nextCount: nextOps.length,
    };
  }
  const doc = {
    ...(store.doc || {}),
    opportunities: nextOps,
    updatedAt: new Date().toISOString(),
    contactCompletionWave2: {
      version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
      appliedAt: new Date().toISOString(),
      appliedCount: applied.length,
      appliedIds: applied.map((a) => a.id),
      deferredIds: WAVE2_CANONICAL_IDS.filter((id) => !targetIds.includes(id)),
      founderApprovalNote:
        "Four IDs approved 2026-11-12; ACVNU deferred pending Gmail classification correction",
    },
  };
  writeJsonAtomic(store.path, doc);
  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    appliedCount: applied.length,
    applied,
    deferredIds: WAVE2_CANONICAL_IDS.filter((id) => !targetIds.includes(id)),
    backupPath: backup.backupPath,
    beforeSha256: store.sha256,
    afterSha256: sha256File(store.path),
  };
}

export function runBethesda88ContactCompletionWave2({
  root = ROOT,
  applyHistorical = true,
  applyCanonical = false,
  founderAuthorization = false,
  approvalToken = null,
} = {}) {
  const outDir = path.join(root, REPORT_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const storeBefore = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256)) {
    return { ok: false, code: "UNEXPECTED_STORE_SHA", sha256: storeBefore.sha256 };
  }

  const unionBefore = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    storeBefore.opportunities,
    { root }
  );
  const beforeCoverage = coverageCounts(unionBefore.opportunities);
  const beforeUnique = uniqueMissingNamed(unionBefore.opportunities);

  writeJsonAtomic(path.join(outDir, "00-named-contact-gaps-before.json"), {
    generatedAt: new Date().toISOString(),
    storeSha: storeBefore.sha256,
    rawCoverage: beforeCoverage,
    uniqueCommercial: beforeUnique.uniqueCommercial,
    uniqueMissingNamed: beforeUnique.missingCount,
    gaps: beforeUnique.missing,
  });

  let historicalPersist = { writeApplied: false };
  if (applyHistorical) {
    historicalPersist = applyHistoricalContactEnrichment({ root, dryRun: false });
  }

  const canonicalManifest = buildWave2CanonicalDryRunPatches({ root });
  writeJsonAtomic(
    path.join(outDir, "02-canonical-wave2-dry-run-manifest.json"),
    canonicalManifest
  );

  let canonicalPersist = {
    writeApplied: false,
    requiresFounderApproval: true,
  };
  if (applyCanonical) {
    canonicalPersist = applyWave2CanonicalContactPatches({
      root,
      founderAuthorization,
      approvalToken,
      dryRun: false,
    });
  }

  // Projected after: live historical + dry-run canonical merges
  const storeMid = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const unionMid = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    storeMid.opportunities,
    { root }
  );
  const projected = unionMid.opportunities.map((o) => {
    const id = text(o.id);
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    if (!evidence || evidence.rejected || evidence.founderValidation === "FAIL") {
      return o;
    }
    if (evidence.inventory === "canonical" && !canonicalPersist.writeApplied) {
      let merged = mergeContactAntiClobber(o, evidence);
      if (evidence.hotelFitHonesty) {
        merged.hotelFitHonesty = evidence.hotelFitHonesty;
        merged.hotelFitNote = evidence.hotelFitNote || null;
      }
      return merged;
    }
    return o;
  });

  const afterCoverageProjected = coverageCounts(projected);
  const afterUniqueProjected = uniqueMissingNamed(projected);
  const afterLive = coverageCounts(unionMid.opportunities);
  const afterUniqueLive = uniqueMissingNamed(unionMid.opportunities);

  // SR / AD reconciliation on projected bodies (no product mutation)
  let sr = 0;
  let ad = 0;
  const srIds = [];
  for (const opp of projected) {
    try {
      const e = evaluateSharedCommercialEligibility(opp);
      if (e.salesReady === true) {
        sr += 1;
        srIds.push(opp.id);
      }
    } catch {
      /* ignore */
    }
    try {
      if (evaluateAccountDevelopmentEligibility(opp).accountDevelopment === true) {
        ad += 1;
      }
    } catch {
      /* ignore */
    }
  }

  const quality = runGdiOpportunityFullE2eQualityOrchestration({
    root,
    applyHistorical: false,
    applyCanonical: false,
    founderAuthorization: false,
    paidResearchAuthorized: false,
    reportRel: `${REPORT_REL}/full-e2e-recert`,
  });

  const remaining = afterUniqueProjected.missing.map((g) => {
    const disqualified = /disqualified/i.test(g.id) || /disqualified/i.test(g.title || "");
    return {
      ...g,
      reason: disqualified
        ? "DISQUALIFIED_NAMED_CONTACT_NOT_REQUIRED"
        : g.id === "gdi_opp_marriott_hq_adjacent_corporate_watch"
          ? "CATEGORY_WATCH_NO_SINGLE_BUYER_ENTITY"
          : /aao_|iscb_|ntca_|aan_|agb_|ada_|asa_|fba_|ptab_/i.test(g.id)
            ? "ZERO_PAID_EXHAUSTED_OR_CONVENTION_SCALE_NO_PUBLIC_NAMED_MEETINGS_OWNER"
            : g.id.includes("nci_rna") || g.id.includes("request_a_conference")
              ? "NAMED_LODGING_OWNER_REJECTED_PRIOR_FOUNDER_FAIL_FUNCTIONAL_CCR_ONLY"
              : "ZERO_PAID_PUBLIC_SOURCES_EXHAUSTED_NAMED_PERSON_NOT_PUBLISHED",
      paidResearchCandidate: !disqualified && !/marriott_hq/i.test(g.id),
    };
  });

  const paidProposal = {
    authorized: false,
    provider: "Exa Agent (existing GDI Full E2E contact deep research)",
    expectedCalls: 3,
    ceilingUsd: 12,
    opportunityIds: [
      "gdi_opp_2027_nci_rna_biology_symposium_7",
      "gdi_pep_live_c165c897d233",
      "gdi_opp_48th_iscb_annual_conference_2027_12",
    ],
    rationale: [
      {
        id: "gdi_opp_2027_nci_rna_biology_symposium_7",
        question:
          "Confirm current CCR Conferences lodging coordinator for 2027 NCI RNA Biology Symposium (Carrie Wagerman previously rejected).",
        strength: "Bethesda Marriott listed on accommodations — high commercial value if named lodging owner confirmed",
      },
      {
        id: "gdi_pep_live_c165c897d233",
        question: "Verify direct email / phone for named planner Carrie Rice",
        strength: "Named planner already present — direct route unlocks bucket A",
      },
      {
        id: "gdi_opp_48th_iscb_annual_conference_2027_12",
        question: "Identify ISCB 2027 local organizing / housing chair with public contact",
        strength: "Annual conference lodging demand if DC-area cycle confirmed",
      },
    ],
    note: "NOT AUTHORIZATION. Prior Exa allowance exhausted. Stop for explicit founder approval.",
  };

  const report = {
    version: BETHESDA_88_CONTACT_COMPLETION_WAVE2_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_HOTEL_ID,
    safety: {
      paidResearch: false,
      opportunitiesJsonWrites: canonicalPersist.writeApplied === true,
      historicalResearchStageWrites: historicalPersist.writeApplied === true,
      customerPublication: false,
    },
    before: {
      raw: beforeCoverage,
      uniqueMissingNamed: beforeUnique.missingCount,
      uniqueCommercial: beforeUnique.uniqueCommercial,
    },
    afterLive: {
      raw: afterLive,
      uniqueMissingNamed: afterUniqueLive.missingCount,
      note: "Includes historical wave-2 applies only; canonical patches not written unless founder-authorized",
    },
    afterProjectedIncludingCanonicalDryRun: {
      raw: afterCoverageProjected,
      uniqueMissingNamed: afterUniqueProjected.missingCount,
      uniqueCommercial: afterUniqueProjected.uniqueCommercial,
    },
    recovered: {
      historicalIds: WAVE2_HISTORICAL_IDS,
      canonicalDryRunIds: WAVE2_CANONICAL_IDS,
      fromPriorResearch: [],
      fromZeroPaid: [
        ...WAVE2_HISTORICAL_IDS,
        ...WAVE2_CANONICAL_IDS,
      ],
    },
    remainingGaps: remaining,
    engineEligibilityProjected: {
      salesReadyEligible: sr,
      salesReadyIds: srIds,
      accountDevelopmentEligible: ad,
      notes: [
        "gdi_opp_loudoun_college_showcase_2027 — Sales Ready engine pass (Dave Bucciero); historically validated stay-to-play demand.",
        "gdi_opp_nado_ddaa_washcon_2028 — engine may pass after Brittany Salazar contact; GOVERNED maturity remains Research Continuation until 2028 host hotel / Bethesda-eligible lodging is evidenced (hotelFitHonesty=DESTINATION_UNCONFIRMED). Do not auto-publish.",
        "gdi_opp_acts_ts27 — Adam Joyce named with direct email; remains RESEARCH_CONTINUATION (BUYER_RESPONSIBILITY_UNKNOWN / INVALID_BUYER_CONTACT_ATTRIBUTION).",
        "Contact enrichment never sets product salesReady=true or customerFacing=true.",
      ],
    },
    historicalPersist,
    canonicalPersist,
    canonicalDryRunManifest: canonicalManifest,
    proposedPaidBatch: paidProposal,
    fullE2eRecert: {
      ok: quality.ok === true,
      processedCount: quality.execution?.processedCount,
      commercialCertification: quality.afterProjected?.commercialCertification,
      researchIntegrity: quality.afterProjected?.researchIntegrity,
      namedRelevantPrimary: quality.afterProjected?.namedRelevantPrimary,
    },
  };

  writeJsonAtomic(path.join(outDir, "01-before-after-coverage.json"), {
    before: report.before,
    afterLive: report.afterLive,
    afterProjected: report.afterProjectedIncludingCanonicalDryRun,
  });
  writeJsonAtomic(path.join(outDir, "03-remaining-gaps-after-research.json"), {
    count: remaining.length,
    rows: remaining,
  });
  writeJsonAtomic(path.join(outDir, "04-proposed-paid-research-batch.json"), paidProposal);
  writeJsonAtomic(path.join(outDir, "05-sr-ad-reconciliation.json"), report.engineEligibilityProjected);
  writeJsonAtomic(path.join(outDir, "00-wave2-orchestration-summary.json"), {
    version: report.version,
    generatedAt: report.generatedAt,
    safety: report.safety,
    before: report.before,
    afterLive: report.afterLive,
    afterProjected: report.afterProjectedIncludingCanonicalDryRun,
    recovered: report.recovered,
    remainingGapCount: remaining.length,
    engineEligibilityProjected: report.engineEligibilityProjected,
    fullE2eRecert: report.fullE2eRecert,
    proposedPaidAuthorized: false,
  });

  return { ok: true, ...report };
}
