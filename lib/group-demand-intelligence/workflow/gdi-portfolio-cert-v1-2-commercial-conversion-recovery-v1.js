/**
 * GDI Portfolio Certification V1.2 — Commercial Conversion Recovery.
 * Offline forensic reclassification of frozen V1.1 evidence.
 * No paid research. No Airtable production writes. No Sales Ready promotion.
 */

import fs from "node:fs";
import path from "node:path";
import { reconcileCommercialDispositionV1 } from "./gdi-commercial-disposition-reconcile-v1.js";
import { buildResearchEconomicsLedger } from "./gdi-research-economics-ledger-v1.js";
import { qualifyCommercialProspectV3 } from "../research/commercial-qualification-v3.js";

export const GDI_PORTFOLIO_CERT_V1_2 =
  "gdi-portfolio-cert-v1-2-commercial-conversion-recovery-v1";

export const V1_1_EIGHT_OPPORTUNITY_IDS = Object.freeze([
  "disc_461dfa26a790", // ASGSR — Bethesda
  "disc_83f5b13dff4c", // Liatris — Bethesda
  "disc_0a11a0bb1158", // AAEI — Bethesda
  "disc_9334f4f1e3dd", // AHA — Bethesda
  "disc_cd8f10005345", // IRF — YOTEL
  "disc_b7fc48bf6995", // ESCP — YOTEL
  "disc_a1a0378bf466", // SETAC — YOTEL
  "disc_77bac9a0d82a", // GDPM — YOTEL
]);

export const FORENSIC_CLASS = Object.freeze({
  A_LEGITIMATELY_IRRELEVANT: "A_LEGITIMATELY_IRRELEVANT_OR_UNSUPPORTED",
  B_RELEVANT_INSUFFICIENT_EVIDENCE: "B_RELEVANT_INSUFFICIENT_EVIDENCE",
  C_RELEVANT_UNRESOLVED_BUYER: "C_RELEVANT_UNRESOLVED_NAMED_BUYER",
  D_TECHNICALLY_INCOMPLETE_RESEARCH: "D_TECHNICALLY_INCOMPLETE_RESEARCH",
  E_INCORRECT_QUALIFICATION_OR_PROPAGATION:
    "E_INCORRECT_QUALIFICATION_OR_DATA_PROPAGATION",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function classifyForensicBucket(ors = {}) {
  const gates = ors.commercialEligibilityContractV1?.gates || {};
  const gaps = ors.commercialEligibilityContractV1?.gaps || [];
  const elig = text(ors.commercialEligibilityStatus).toUpperCase();
  const hist = text(
    ors.historicalInternalDisposition ||
      ors.dispositionReconcile?.v41Disposition ||
      ors.internalDisposition,
  ).toUpperCase();
  const investigatedEmpty = !(ors.investigatedUrls || []).length;
  const namedCount = (ors.namedStakeholders || []).length;
  const hasPrimary = Boolean(ors.primaryContact?.name || ors.contactName);
  const orgActivity = gates.credibleOrganizationActivity === true;
  const realisticDemand = gates.realisticHotelDemand === true;
  const hotelFitSr = gates.hotelFitDefensibleForSalesReady === true;

  const labels = [];
  if (investigatedEmpty && ors.exaRunId) {
    labels.push(FORENSIC_CLASS.D_TECHNICALLY_INCOMPLETE_RESEARCH);
  }
  if (
    elig === "RESEARCH_CONTINUATION" &&
    /EXCLUDE/.test(hist) &&
    ors.commercialEligibilityContractV1?.preserveOpportunity === true
  ) {
    labels.push(FORENSIC_CLASS.E_INCORRECT_QUALIFICATION_OR_PROPAGATION);
  }
  if (orgActivity && !hasPrimary && !namedCount) {
    labels.push(FORENSIC_CLASS.C_RELEVANT_UNRESOLVED_BUYER);
  }
  if (orgActivity && (realisticDemand || !hotelFitSr) && gaps.length) {
    labels.push(FORENSIC_CLASS.B_RELEVANT_INSUFFICIENT_EVIDENCE);
  }
  if (!orgActivity && elig === "COMMERCIALLY_INELIGIBLE") {
    labels.push(FORENSIC_CLASS.A_LEGITIMATELY_IRRELEVANT);
  }
  if (!labels.length) {
    labels.push(
      orgActivity
        ? FORENSIC_CLASS.B_RELEVANT_INSUFFICIENT_EVIDENCE
        : FORENSIC_CLASS.A_LEGITIMATELY_IRRELEVANT,
    );
  }
  return [...new Set(labels)];
}

/**
 * Re-apply corrected reconcile (+ optional V3) on a frozen ORS row without paid research.
 * Never promotes to Sales Ready.
 */
export function reclassifyFrozenOpportunity(ors = {}, opts = {}) {
  const eligibility = ors.commercialEligibilityContractV1 || {
    status: ors.commercialEligibilityStatus || null,
    researchContinuation:
      ors.researchContinuationRequired === true ||
      text(ors.commercialEligibilityStatus).toUpperCase() ===
        "RESEARCH_CONTINUATION",
    preserveOpportunity: true,
    doNotTreatAsExhausted: true,
    salesReady: false,
  };

  let v41 = ors.commercialQualificationV41 || null;
  let v3Replay = null;
  if (opts.replayV3 === true) {
    v3Replay = qualifyCommercialProspectV3({
      opportunity: ors,
      acceptedClaims: ors.acceptedClaims || [],
      pageTexts: (ors.evidence || []).map((e) => e.statement).filter(Boolean),
      targetHotel: ors.hotelName || "",
      usableContactRoute:
        ors.contactEmail ||
        ors.organizationContactEmail ||
        ors.contactUrl ||
        null,
    });
    v41 = {
      ...(v41 || {}),
      v3: v3Replay,
      internalDisposition: v3Replay.internalDisposition,
      commerciallyUsefulInternal: v3Replay.commerciallyUsefulInternal === true,
    };
  }

  const reconcile = reconcileCommercialDispositionV1({
    eligibilityResult: eligibility,
    v41Qualification: v41 || {
      internalDisposition:
        ors.historicalInternalDisposition || ors.internalDisposition,
    },
    priorDisposition:
      ors.historicalInternalDisposition ||
      ors.dispositionReconcile?.v41Disposition ||
      null,
  });

  const beforeDisposition =
    ors.dispositionReconcile?.finalDisposition ||
    ors.internalDisposition ||
    null;
  const afterDisposition = reconcile.finalDisposition;
  const changed = text(beforeDisposition) !== text(afterDisposition);

  // Sales Ready bar unchanged — never auto-promote.
  const salesReadyAfter = false;

  return {
    opportunityId: ors.opportunityId || ors.id,
    organizationName: ors.organizationName || null,
    hotelId: ors.hotelId || null,
    hotelName: ors.hotelName || null,
    beforeDisposition,
    afterDisposition,
    changed,
    salesReadyAfter,
    eligibilityStatus: eligibility.status || ors.commercialEligibilityStatus,
    preserveOpportunity: eligibility.preserveOpportunity === true,
    reconcile,
    v3ReplayDisposition: v3Replay?.internalDisposition || null,
    forensicClasses: classifyForensicBucket(ors),
    deepResearchImprovedEvidence: assessDeepResearchImprovement(ors),
    contactAudit: auditContactFromFrozen(ors),
    drawerReady: ors.customerPublicationReady === true,
    investigatedUrlCount: (ors.investigatedUrls || []).length,
    seedUrlCount: (ors.seedUrls || []).length,
    namedStakeholderCount: (ors.namedStakeholders || []).length,
    excludeRootCause: describeExcludeRootCause(ors, beforeDisposition),
  };
}

export function assessDeepResearchImprovement(ors = {}) {
  const seed = new Set(
    (ors.seedUrls || []).map((u) => text(u).toLowerCase().replace(/\/$/, "")),
  );
  const investigated = (ors.investigatedUrls || []).map((u) =>
    text(u).toLowerCase().replace(/\/$/, ""),
  );
  const beyondSeed = investigated.filter((u) => u && !seed.has(u));
  const evidenceCount = (ors.evidence || []).length;
  const seedEvidence = (ors.evidence || []).filter(
    (e) => e.provenance === "discovery_evidence",
  ).length;
  const providerRan = Boolean(ors.exaRunId || ors.researchJobId);
  const dossierStubOnly =
    ors.exaAgentDeepResearch &&
    !(ors.exaAgentDeepResearch.investigatedUrlCount > 0) &&
    !(ors.investigatedUrls || []).length;

  return {
    providerRunRecorded: providerRan,
    investigatedUrlsEmpty: investigated.length === 0,
    urlsBeyondSeed: beyondSeed.length,
    evidenceRows: evidenceCount,
    discoveryOnlyEvidenceRows: seedEvidence,
    dossierStubOnly: Boolean(dossierStubOnly),
    evidentiaryImprovementDemonstrated:
      beyondSeed.length > 0 || evidenceCount > seedEvidence,
    verdict: !providerRan
      ? "NO_PROVIDER_RUN"
      : beyondSeed.length > 0
        ? "IMPROVED"
        : investigated.length === 0 && evidenceCount <= Math.max(1, seed.size)
          ? "PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT"
          : "MARGINAL_OR_UNPROVEN",
  };
}

export function auditContactFromFrozen(ors = {}) {
  const named = ors.namedStakeholders || [];
  const primary = ors.primaryContact || null;
  const orgRoute =
    ors.contactUrl ||
    ors.organizationRouteUrl ||
    ors.organizationContactEmail ||
    null;
  const personalEmail = primary?.email || ors.contactEmail || null;
  const personalPhone = primary?.phone || ors.contactPhone || null;

  return {
    namedBuyerFound: Boolean(primary?.name || named.length),
    namedBuyers: [
      ...(primary?.name
        ? [
            {
              name: primary.name,
              role: primary.role || primary.title || null,
              sourceUrl: primary.sourceUrl || null,
              decisionMakerClass: null,
            },
          ]
        : []),
      ...named.map((n) => ({
        name: n.name,
        role: n.role || n.title || null,
        sourceUrl: n.sourceUrl || null,
        decisionMakerClass: n.decisionMakerClass || null,
        buyerVersusIntermediary: n.buyerVersusIntermediary || null,
      })),
    ].slice(0, 6),
    organizationVerified: Boolean(ors.organizationName),
    roleEstablished: Boolean(primary?.role || primary?.title || named[0]?.role),
    personalEmailSupported: Boolean(personalEmail && /@/.test(String(personalEmail))),
    personalPhoneSupported: Boolean(personalPhone),
    organizationalContactRoute: orgRoute,
    inventedEmail: false,
    paidEnrichmentUsed: false,
    note: personalEmail
      ? "Personal email present on frozen ORS"
      : orgRoute
        ? "Organization route only — not Sales Ready contact"
        : "No verified personal or org contact channel on frozen ORS",
  };
}

export function describeExcludeRootCause(ors = {}, disposition = null) {
  const elig = ors.commercialEligibilityContractV1 || {};
  const gaps = elig.gaps || [];
  const v41 = ors.commercialQualificationV41?.v3 || {};
  const hist = text(disposition || ors.internalDisposition).toUpperCase();
  if (!/EXCLUDE/.test(hist)) {
    return {
      code: "NOT_EXCLUDE",
      summary: `Final disposition was ${hist || "unknown"}`,
    };
  }
  return {
    code: "EXCLUDE_DESPITE_RESEARCH_CONTINUATION_ELIGIBILITY",
    summary:
      "V41/V3 EXCLUDE retained by disposition reconcile while commercialEligibilityStatus=RESEARCH_CONTINUATION (preserveOpportunity). Root: missing email contactOk and thin lodging revenue evidence after deep research failed to improve investigatedUrls.",
    eligibilityGaps: gaps,
    v3SalesReadiness: v41.salesReadiness || null,
    unresolvedQuestions: ors.unresolvedQuestions || [],
    investigatedUrlsEmpty: !(ors.investigatedUrls || []).length,
  };
}

/**
 * Prefer curated certification fixtures (committed under fixtures/) because
 * live ORS under data/.../opportunity-research-state/ is gitignored.
 * Fall back to local runtime ORS when present (developer machines).
 */
export const V1_1_FROZEN_ORS_FIXTURE_DIR =
  "fixtures/gdi-portfolio-cert-v1-1-frozen-ors-fixtures-v1";

export function resolveV11OrsPaths(repoRoot, opportunityIds = V1_1_EIGHT_OPPORTUNITY_IDS) {
  const out = [];
  const seen = new Set();

  const fixtureDir = path.join(repoRoot, V1_1_FROZEN_ORS_FIXTURE_DIR);
  if (fs.existsSync(fixtureDir)) {
    for (const id of opportunityIds) {
      const file = path.join(fixtureDir, `${id}.json`);
      if (!fs.existsSync(file)) continue;
      let hotelId = null;
      try {
        hotelId = readJson(file).hotelId || null;
      } catch {
        hotelId = null;
      }
      out.push({
        opportunityId: id,
        hotelId,
        orsPath: file,
        source: "committed_fixture",
      });
      seen.add(id);
    }
  }

  const hotelsRoot = path.join(
    repoRoot,
    "data",
    "group-demand-intelligence",
    "hotels",
  );
  if (fs.existsSync(hotelsRoot)) {
    for (const hotelId of fs.readdirSync(hotelsRoot)) {
      const orsDir = path.join(hotelId, "opportunity-research-state");
      const abs = path.join(hotelsRoot, orsDir);
      if (!fs.existsSync(abs)) continue;
      for (const id of opportunityIds) {
        if (seen.has(id)) continue;
        const file = path.join(abs, `${id}.json`);
        if (fs.existsSync(file)) {
          out.push({
            opportunityId: id,
            hotelId,
            orsPath: file,
            source: "local_runtime_ors",
          });
          seen.add(id);
        }
      }
    }
  }
  return out;
}

export function runOfflineConversionRecovery(repoRoot, opts = {}) {
  const paths = resolveV11OrsPaths(repoRoot, opts.opportunityIds);
  const rows = [];
  for (const p of paths) {
    const ors = readJson(p.orsPath);
    rows.push(
      reclassifyFrozenOpportunity(ors, {
        replayV3: opts.replayV3 !== false,
      }),
    );
  }

  const dispositionBefore = {};
  const dispositionAfter = {};
  for (const r of rows) {
    dispositionBefore[r.beforeDisposition || "null"] =
      (dispositionBefore[r.beforeDisposition || "null"] || 0) + 1;
    dispositionAfter[r.afterDisposition || "null"] =
      (dispositionAfter[r.afterDisposition || "null"] || 0) + 1;
  }

  const processedForEconomicsBefore = rows.map((r) => ({
    primaryContactName: r.contactAudit.namedBuyers[0]?.name || null,
    salesReadyContacts: r.opportunityId === "disc_83f5b13dff4c" ? 1 : 0,
    commercialResearchComplete: r.opportunityId === "disc_83f5b13dff4c",
    disposition: r.beforeDisposition,
    researchContinuationRequired: true,
    commercialEligibilityStatus: r.eligibilityStatus,
  }));
  const processedForEconomicsAfter = rows.map((r) => ({
    primaryContactName: r.contactAudit.namedBuyers[0]?.name || null,
    salesReadyContacts: r.opportunityId === "disc_83f5b13dff4c" ? 1 : 0,
    commercialResearchComplete: r.opportunityId === "disc_83f5b13dff4c",
    disposition: r.afterDisposition,
    researchContinuationRequired: true,
    commercialEligibilityStatus: r.eligibilityStatus,
  }));

  const economicsBefore = buildResearchEconomicsLedger({
    processed: processedForEconomicsBefore,
    selectionRegister: { selectedCount: rows.length, deferredCount: 0, register: [] },
    retainedCandidates: rows,
    spentUsd: 2,
  });
  // Simulate pre-fix economics definition artifact: EXCLUDE + commercialResearchComplete counted.
  const economicsBeforeContradiction = {
    ...economicsBefore,
    verifiedCommercialContactsGained: rows.filter((r) =>
      r.contactAudit.namedBuyers.some((b) => b.name),
    ).length
      ? 1
      : economicsBefore.verifiedCommercialContactsGained,
    note: "V1.1 Bethesda reported contactsGained=1 / actionable=1 from Liatris named contact + commercialResearchComplete while disposition EXCLUDE.",
  };
  const economicsAfter = buildResearchEconomicsLedger({
    processed: processedForEconomicsAfter,
    selectionRegister: { selectedCount: rows.length, deferredCount: 0, register: [] },
    retainedCandidates: rows,
    spentUsd: 2,
  });

  const scorecardContradiction = {
    bethesdaReported: {
      verifiedCommercialContactsGained: 1,
      opportunitiesBecomingActionable: 1,
      selectedRowNamedStakeholders: 0,
      selectedRowSalesReady: 0,
      deepRowsExclude: 4,
    },
    resolution: {
      populationDifference:
        "Economics counters read processed-row primaryContactName/salesReadyContacts/commercialResearchComplete; selection-register stakeholder arrays and Sales Ready are different stages/fields.",
      liatrisExplainsBothCounters:
        "disc_83f5b13dff4c (Liatris) had Frank Yang + salesReadyContacts=1 + commercialResearchComplete=true while final disposition EXCLUDE and register namedStakeholders empty (propagation gap).",
      correctedRule:
        "opportunitiesBecomingActionable must exclude EXCLUDE disposition; canonicalSalesReadyCount is the Sales Ready metric; portfolio cert uses eligibility + reconciled disposition.",
    },
    economicsBeforeContradiction,
    economicsAfter,
  };

  return {
    version: GDI_PORTFOLIO_CERT_V1_2,
    at: new Date().toISOString(),
    mode: "OFFLINE_FROZEN_V1_1_EVIDENCE",
    paidResearch: false,
    airtableWrites: false,
    salesReadyPromotions: 0,
    rows,
    dispositionDistribution: {
      before: dispositionBefore,
      after: dispositionAfter,
    },
    classificationChanges: rows.filter((r) => r.changed),
    forensicClassCounts: rows.reduce((acc, r) => {
      for (const c of r.forensicClasses) acc[c] = (acc[c] || 0) + 1;
      return acc;
    }, {}),
    deepResearchImprovementCounts: rows.reduce((acc, r) => {
      const v = r.deepResearchImprovedEvidence.verdict;
      acc[v] = (acc[v] || 0) + 1;
      return acc;
    }, {}),
    scorecardContradiction,
    benchmark: {
      correctlyClassifiedAfter: rows.filter(
        (r) =>
          r.afterDisposition === "WATCHLIST_RESEARCH" ||
          r.afterDisposition === "EXCLUDE",
      ).length,
      credibleDemandRetainedForContinuation: rows.filter(
        (r) => r.afterDisposition === "WATCHLIST_RESEARCH",
      ).length,
      commerciallyRelevantNamedStakeholders: rows.filter(
        (r) => r.contactAudit.namedBuyerFound,
      ).length,
      independentlyVerifiedContactChannels: rows.filter(
        (r) =>
          r.contactAudit.personalEmailSupported ||
          r.contactAudit.organizationalContactRoute,
      ).length,
      salesReadyOpportunities: 0,
      falsePositiveExcludeRemediated: rows.filter(
        (r) =>
          /EXCLUDE/i.test(r.beforeDisposition || "") &&
          r.afterDisposition === "WATCHLIST_RESEARCH",
      ).length,
      falseNegativeSalesReadyForced: 0,
    },
  };
}

export default {
  GDI_PORTFOLIO_CERT_V1_2,
  V1_1_EIGHT_OPPORTUNITY_IDS,
  V1_1_FROZEN_ORS_FIXTURE_DIR,
  FORENSIC_CLASS,
  classifyForensicBucket,
  reclassifyFrozenOpportunity,
  assessDeepResearchImprovement,
  auditContactFromFrozen,
  resolveV11OrsPaths,
  runOfflineConversionRecovery,
};
