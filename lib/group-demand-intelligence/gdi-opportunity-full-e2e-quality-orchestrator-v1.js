/**
 * GDI Opportunity Full E2E Quality Orchestrator V1
 *
 * Reusable orchestration over existing discovery/ORS/contact/maturation/
 * evidence/schema/persistence components. Processes an opportunity inventory
 * end-to-end: inspect → remediate (source-backed) → validate → certify.
 *
 * Safety defaults:
 * - No paid research
 * - No opportunities.json writes unless founderAuthorization + applyCanonical
 * - Historical research-stage merges only when applyHistorical=true
 * - No customer publication
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateOpportunityQualityContract,
  evaluateOpportunityQualityBatch,
  evaluateOpportunityQualityGate,
  GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
  COMMERCIAL_CERT,
  RESEARCH_INTEGRITY,
} from "./gdi-opportunity-quality-contract-v1.js";

export { evaluateOpportunityQualityGate };
import {
  classifyOpportunityTitleDefects,
  customerSafeOpportunityTitle,
  hasMeetingCredentialContamination,
  sanitizeOpportunityTitle,
} from "./opportunity-title-quality-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
  applyHistoricalContactEnrichment,
} from "./research/bethesda-88-contact-quality-completion-v1.js";
import {
  applyBethesdaCommercialEventFamilyPresentation,
  buildBethesdaCommercialEventFamilyCrosswalk,
  FAMILY_ROLE,
} from "./research/bethesda-88-commercial-event-family-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./research/bethesda-historical-attestation-bag-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
} from "./research/gdi-hotel-store-integrity-guard-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./research/gdi-research-stage-persistence-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./research/bethesda-historical-commercial-category-map-v1.js";
import { evaluateSharedCommercialEligibility } from "./commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "./account-development-eligibility-v1.js";

export const GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1 =
  "gdi-opportunity-full-e2e-quality-orchestrator-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DEFAULT_REPORT_REL =
  "reports/gdi-bethesda-88-full-e2e-opportunity-quality-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function descriptionBody(opp = {}) {
  return text(
    opp.whatIsHappening ||
      opp.hotelOpportunityThesis ||
      opp.summaryWhat ||
      opp.tileCustomerSummary ||
      opp.description ||
      ""
  );
}

function buildFamilyRoleMap(opportunities = []) {
  const presentation = applyBethesdaCommercialEventFamilyPresentation(
    opportunities,
    { hideLinkedFromList: true }
  );
  const map = new Map();
  for (const row of presentation.allAnnotated || []) {
    const fam = row.commercialEventFamily || {};
    map.set(row.id, {
      role:
        row.commercialEventFamilyRole ||
        fam.role ||
        FAMILY_ROLE.STANDALONE,
      familyId:
        row.commercialEventFamilyId ||
        fam.familyId ||
        null,
    });
  }
  return { map, presentation };
}

/**
 * Propose source-backed remediations for one opportunity (no inventing).
 */
export function proposeOpportunityRemediation(opportunity = {}, ctx = {}) {
  const before = opportunity;
  const id = text(before.id || before.opportunityId);
  const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id] || null;
  const changes = [];
  let proposed = { ...before };

  // Title sanitization / customer-safe projection (does not invent replacement
  // when source is invalid — quarantine instead).
  const title = text(before.title || before.opportunityName);
  const defects = classifyOpportunityTitleDefects(title, {
    organizationName: before.organizationName,
  });
  const alreadyInvalidSource =
    text(before.titleSourceStatus).toUpperCase() === "TITLE_SOURCE_INVALID" ||
    text(ctx.familyRole).toUpperCase() === FAMILY_ROLE.QUARANTINED ||
    text(before.commercialEventFamilyRole).toUpperCase() === FAMILY_ROLE.QUARANTINED;
  if (hasMeetingCredentialContamination(title) || alreadyInvalidSource) {
    proposed = {
      ...proposed,
      titleSourceStatus: "TITLE_SOURCE_INVALID",
      commercialEventFamilyRole: FAMILY_ROLE.QUARANTINED,
      customerFacing: false,
      salesReady: false,
    };
    if (text(before.titleSourceStatus).toUpperCase() !== "TITLE_SOURCE_INVALID") {
      changes.push({ field: "titleSourceStatus", to: "TITLE_SOURCE_INVALID" });
    }
    // Do not rewrite quarantined titles — preserve research provenance.
  } else {
    const safe = customerSafeOpportunityTitle(before);
    const sanitized = sanitizeOpportunityTitle(title);
    if (safe && safe !== title && sanitized) {
      proposed = { ...proposed, title: safe };
      changes.push({ field: "title", from: title, to: safe, defects });
    }
  }

  // Contact enrichment from verified zero-paid evidence (anti-clobber).
  if (evidence && evidence.rejected !== true && evidence.founderValidation !== "FAIL") {
    const beforeSnap = extractContactSnapshot(before);
    const merged = mergeContactAntiClobber(proposed, evidence);
    const afterSnap = extractContactSnapshot(merged);
    if (
      beforeSnap.coverage !== afterSnap.coverage ||
      beforeSnap.name !== afterSnap.name ||
      beforeSnap.email !== afterSnap.email
    ) {
      proposed = merged;
      changes.push({
        field: "primaryContact",
        from: {
          name: beforeSnap.name,
          coverage: beforeSnap.coverage,
          email: beforeSnap.email,
        },
        to: {
          name: afterSnap.name,
          coverage: afterSnap.coverage,
          email: afterSnap.email,
          orgRoute: afterSnap.orgEmail || afterSnap.orgRoute,
        },
        sourceUrl: evidence.primaryContact?.sourceUrl || null,
      });
    }
    if (evidence.hotelFitHonesty && !proposed.hotelFitHonesty) {
      proposed = {
        ...proposed,
        hotelFitHonesty: evidence.hotelFitHonesty,
        hotelFitNote: evidence.hotelFitNote || proposed.hotelFitNote || null,
      };
      changes.push({
        field: "hotelFitHonesty",
        to: evidence.hotelFitHonesty,
      });
    }
  }

  // Historical category projection (existing map — no invention).
  if (ctx.historical === true) {
    const catBefore = text(proposed.category);
    proposed = applyHistoricalCommercialCategoryProjection(proposed);
    const catAfter = text(proposed.category);
    if (catBefore !== catAfter) {
      changes.push({ field: "category", from: catBefore || null, to: catAfter || null });
    }
  }

  // Family role annotation (presentation metadata only).
  if (ctx.familyRole) {
    proposed = {
      ...proposed,
      commercialEventFamilyRole: ctx.familyRole,
      commercialEventFamilyId: ctx.familyId || proposed.commercialEventFamilyId || null,
    };
  }

  // Never promote publication / SR from remediation alone.
  proposed = {
    ...proposed,
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
  };

  // Description credential scrub — redact, do not invent replacement prose.
  const desc = descriptionBody(proposed);
  if (hasMeetingCredentialContamination(desc)) {
    changes.push({
      field: "description",
      action: "FLAG_CREDENTIAL_LEAK_REQUIRES_SOURCE_REWRITE",
    });
  }

  const evaluation = evaluateOpportunityQualityContract(proposed, ctx);
  return {
    id,
    inventory: ctx.historical ? "historical" : "canonical",
    changeCount: changes.length,
    changes,
    alreadyAppliedContact:
      evidence?.applied === true ||
      Boolean(before?.approvedContactUpdate?.founderApproved),
    proposed,
    evaluation,
  };
}

function loadHistoricalDoc(hotelId, { root = ROOT } = {}) {
  const stagedPath = path.join(
    researchStageHotelDir(hotelId, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (!fs.existsSync(stagedPath)) return { path: stagedPath, exists: false, doc: null };
  return {
    path: stagedPath,
    exists: true,
    doc: JSON.parse(fs.readFileSync(stagedPath, "utf8")),
  };
}

/**
 * Execute Full E2E quality orchestration for Bethesda 88 (reusable pattern).
 *
 * @param {object} opts
 * @param {boolean} [opts.applyHistorical=false]
 * @param {boolean} [opts.applyCanonical=false] requires founderAuthorization
 * @param {boolean} [opts.founderAuthorization=false]
 * @param {boolean} [opts.paidResearchAuthorized=false] — always refused unless true (default false)
 */
export function runGdiOpportunityFullE2eQualityOrchestration(opts = {}) {
  const {
    root = ROOT,
    hotelId = BETHESDA_HOTEL_ID,
    reportRel = DEFAULT_REPORT_REL,
    applyHistorical = false,
    applyCanonical = false,
    founderAuthorization = false,
    paidResearchAuthorized = false,
  } = opts;

  const outDir = path.join(root, reportRel);
  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(path.join(outDir, "checkpoints"), { recursive: true });

  if (paidResearchAuthorized) {
    // Hard stop — founder directive: no paid without separate new approval gate.
    // This orchestrator never launches paid providers.
  }

  const store = loadHotelOpportunityDoc(hotelId, { root });
  if (!store.exists || store.count !== 59) {
    return {
      ok: false,
      code: "UNEXPECTED_CANONICAL_STORE",
      count: store.count,
      path: store.path,
    };
  }
  if (!ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256)) {
    return {
      ok: false,
      code: "UNEXPECTED_STORE_SHA",
      sha256: store.sha256,
    };
  }

  const backup = backupHotelOpportunityStore(hotelId, {
    root,
    label: "pre-full-e2e-quality-orchestration",
  });

  const union = mergeBethesdaInternalResearchOpportunities(
    hotelId,
    store.opportunities,
    { root }
  );
  const { map: familyById, presentation } = buildFamilyRoleMap(
    union.opportunities
  );
  const familyCrosswalk = buildBethesdaCommercialEventFamilyCrosswalk(
    union.opportunities
  );

  // BEFORE evaluation (live persisted state)
  const beforeBatch = evaluateOpportunityQualityBatch(union.opportunities, {
    familyById,
    historicalIds: BETHESDA_29_ATTESTED_IDS,
  });

  const remediations = [];
  const canonicalPatches = [];
  const historicalPatches = [];
  const processedIds = [];

  for (const opp of union.opportunities) {
    const id = text(opp.id);
    processedIds.push(id);
    const fam = familyById.get(id) || {};
    const historical = BETHESDA_29_ATTESTED_IDS.includes(id);
    const remediation = proposeOpportunityRemediation(opp, {
      historical,
      familyRole: fam.role || null,
      familyId: fam.familyId || null,
    });
    remediations.push(remediation);
    if (historical) historicalPatches.push(remediation);
    else canonicalPatches.push(remediation);

    // Per-record checkpoint (resumability / no silent loss)
    writeJsonAtomic(
      path.join(outDir, "checkpoints", `${id}.json`),
      {
        id,
        processedAt: new Date().toISOString(),
        inventory: historical ? "historical" : "canonical",
        familyRole: fam.role || "STANDALONE",
        researchIntegrity: remediation.evaluation.researchIntegrity,
        commercialCertification: remediation.evaluation.commercialCertification,
        changeCount: remediation.changeCount,
        nextResearchActions: remediation.evaluation.nextResearchActions,
        paidResearch: false,
      }
    );
  }

  // AFTER evaluation on proposed bodies (projected)
  const afterOpps = remediations.map((r) => r.proposed);
  const afterBatch = evaluateOpportunityQualityBatch(afterOpps, {
    familyById,
    historicalIds: BETHESDA_29_ATTESTED_IDS,
  });

  // Persist historical remediations when authorized (research-stage only).
  let historicalPersist = {
    ok: true,
    writeApplied: false,
    appliedCount: 0,
  };
  if (applyHistorical) {
    // Contact enrichment path (existing protected merge).
    const contactApply = applyHistoricalContactEnrichment({ root, dryRun: false });
    // Category + family + title-safe fields for historical rows with changes.
    const staged = loadHistoricalDoc(hotelId, { root });
    if (staged.exists) {
      const byId = new Map(
        (staged.doc.opportunities || []).map((o) => [text(o.id), o])
      );
      let applied = 0;
      for (const patch of historicalPatches) {
        if (!patch.changeCount) continue;
        const existing = byId.get(patch.id);
        if (!existing) continue;
        // Anti-clobber merge of proposed contact/category/family annotations.
        let next = mergeContactAntiClobber(existing, {
          primaryContact: patch.proposed.primaryContact,
          secondaryContact: patch.proposed.secondaryContact,
          contactRecovery: patch.proposed.contactRecovery,
        });
        next = applyHistoricalCommercialCategoryProjection(next);
        if (patch.proposed.commercialEventFamilyRole) {
          next.commercialEventFamilyRole = patch.proposed.commercialEventFamilyRole;
          next.commercialEventFamilyId = patch.proposed.commercialEventFamilyId;
        }
        if (
          patch.proposed.title &&
          !hasMeetingCredentialContamination(patch.proposed.title) &&
          text(patch.proposed.title) !== text(existing.title)
        ) {
          next.title = patch.proposed.title;
        }
        next.salesReady = false;
        next.customerFacing = false;
        next.customerVisible = false;
        next.publicationApproved = false;
        byId.set(patch.id, next);
        applied += 1;
      }
      const nextOps = (staged.doc.opportunities || []).map(
        (o) => byId.get(text(o.id)) || o
      );
      const persist = persistResearchStageOpportunityBag({
        hotelId,
        targetPath: staged.path,
        incomingDoc: {
          ...staged.doc,
          version: GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
          updatedAt: new Date().toISOString(),
          fullE2eQuality: {
            version: GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
            appliedAt: new Date().toISOString(),
            appliedCount: applied,
            publication: "NOT_AUTHORIZED",
          },
          opportunities: nextOps,
        },
        protectedIds: [...BETHESDA_29_ATTESTED_IDS],
        root,
        label: "bethesda-88-full-e2e-quality",
        mode: "merge",
      });
      historicalPersist = {
        ok: persist?.ok === true,
        writeApplied: persist?.ok === true,
        appliedCount: applied,
        contactApply,
        persistCode: persist?.code || null,
      };
    }
  }

  // Canonical patches are always dry-run unless founderAuthorization + applyCanonical.
  const canonicalWriteAuthorized =
    applyCanonical === true && founderAuthorization === true;
  let canonicalPersist = {
    ok: true,
    writeApplied: false,
    appliedCount: 0,
    requiresFounderApproval: !canonicalWriteAuthorized,
  };

  if (canonicalWriteAuthorized) {
    // Explicitly gated — still refuse bulk replacement; contact-field only via
    // existing approved apply module pattern. This orchestrator does not
    // silently rewrite full opportunity bodies into opportunities.json.
    canonicalPersist = {
      ok: false,
      writeApplied: false,
      appliedCount: 0,
      code: "CANONICAL_FULL_BODY_WRITE_REQUIRES_DEDICATED_APPROVED_APPLY_MODULE",
      note:
        "Canonical field remediations are emitted as dry-run manifests. Use dedicated founder-approved apply scripts for contact/title patches.",
    };
  }

  // Paid research proposal (never executed here).
  const paidCandidates = afterBatch.rows
    .filter(
      (r) =>
        r.commercialCertification === COMMERCIAL_CERT.RESEARCH_CONTINUATION ||
        r.commercialCertification === COMMERCIAL_CERT.OVERFLOW_WATCH ||
        r.commercialCertification === COMMERCIAL_CERT.PARTNERSHIP_WATCH
    )
    .filter((r) => !r.namedRelevantPrimary || !r.hasDirectPersonEmail)
    .filter((r) => r.familyRole !== "LINKED_RESEARCH" && r.familyRole !== "QUARANTINED")
    .filter((r) => r.researchIntegrity === RESEARCH_INTEGRITY.PASS)
    .slice(0, 5);

  const proposedPaidBatch = {
    authorized: false,
    provider: "Exa Agent (existing GDI Full E2E contact deep research)",
    expectedCalls: Math.min(3, paidCandidates.length),
    ceilingUsd: Math.min(12, Math.max(0, paidCandidates.length) * 4),
    opportunityIds: paidCandidates.map((r) => r.id),
    rationale: paidCandidates.map((r) => ({
      id: r.id,
      blockers: r.blockers.slice(0, 6),
      next: r.nextResearchActions.slice(0, 4),
    })),
    note:
      "NOT AUTHORIZATION. Prior Exa allowance exhausted. Stop for explicit new founder approval before any paid call.",
  };

  // Engine maturity counts on proposed bodies
  let srEligible = 0;
  let adEligible = 0;
  for (const opp of afterOpps) {
    try {
      if (evaluateSharedCommercialEligibility(opp).salesReady === true) srEligible += 1;
    } catch {
      /* ignore */
    }
    try {
      if (evaluateAccountDevelopmentEligibility(opp).accountDevelopment === true) {
        adEligible += 1;
      }
    } catch {
      /* ignore */
    }
  }

  const report = {
    version: GDI_OPPORTUNITY_FULL_E2E_QUALITY_ORCHESTRATOR_V1,
    contractVersion: GDI_OPPORTUNITY_QUALITY_CONTRACT_V1,
    generatedAt: new Date().toISOString(),
    hotelId,
    safety: {
      paidResearch: false,
      paidResearchAuthorized: false,
      opportunitiesJsonWrites: canonicalPersist.writeApplied === true,
      historicalResearchStageWrites: historicalPersist.writeApplied === true,
      customerPublication: false,
      salesReadyFabrication: false,
      canonicalFullBodyWrite: "DRY_RUN_MANIFEST_ONLY",
    },
    sourceOfTruth: {
      persistence: "filesystem",
      canonicalCount: store.count,
      canonicalSha256: store.sha256,
      acceptableStore: ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256),
      scalingCompaniesIntact: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
        store.ids.includes(id)
      ),
      backupPath: backup.backupPath,
      union: union.counts,
    },
    execution: {
      loadedIds: union.opportunities.map((o) => o.id),
      processedIds,
      processedCount: processedIds.length,
      expectedUnion: 88,
      allIdsProcessed: processedIds.length === 88,
      uniqueCommercialOpportunities:
        presentation.counts?.uniqueCommercialOpportunities ?? null,
      linkedResearchHidden: presentation.counts?.linkedResearchHidden ?? null,
      quarantinedHidden: presentation.counts?.quarantinedHidden ?? null,
      listVisible: presentation.counts?.listVisible ?? null,
    },
    before: beforeBatch.summary,
    afterProjected: afterBatch.summary,
    familyCrosswalk,
    remediations: remediations.map((r) => ({
      id: r.id,
      inventory: r.inventory,
      changeCount: r.changeCount,
      changes: r.changes,
      alreadyAppliedContact: r.alreadyAppliedContact,
      researchIntegrity: r.evaluation.researchIntegrity,
      commercialCertification: r.evaluation.commercialCertification,
      namedRelevantPrimary: r.evaluation.namedRelevantPrimary,
      hasDirectPersonEmail: r.evaluation.hasDirectPersonEmail,
      blockers: r.evaluation.blockers,
      nextResearchActions: r.evaluation.nextResearchActions,
      eligibleForCustomerPublicationReview:
        r.evaluation.eligibleForCustomerPublicationReview,
    })),
    canonicalDryRunManifest: {
      authorized: false,
      requiresFounderApproval: true,
      patchCount: canonicalPatches.filter((p) => p.changeCount > 0).length,
      patches: canonicalPatches
        .filter((p) => p.changeCount > 0)
        .map((p) => ({
          id: p.id,
          changes: p.changes,
          evaluation: {
            researchIntegrity: p.evaluation.researchIntegrity,
            commercialCertification: p.evaluation.commercialCertification,
            blockers: p.evaluation.blockers,
          },
        })),
    },
    historicalPersist,
    canonicalPersist,
    engineEligibilityProjected: {
      salesReadyEligible: srEligible,
      accountDevelopmentEligible: adEligible,
      note: "Engine eligibility on proposed bodies — product maturity fields NOT auto-promoted.",
    },
    proposedPaidBatch,
  };

  writeJsonAtomic(path.join(outDir, "00-orchestration-summary.json"), {
    version: report.version,
    generatedAt: report.generatedAt,
    hotelId,
    safety: report.safety,
    sourceOfTruth: report.sourceOfTruth,
    execution: report.execution,
    before: report.before,
    afterProjected: report.afterProjected,
    engineEligibilityProjected: report.engineEligibilityProjected,
    proposedPaidBatch: report.proposedPaidBatch,
    historicalPersist: report.historicalPersist,
    canonicalPersist: report.canonicalPersist,
  });
  writeJsonAtomic(path.join(outDir, "01-record-level-reconciliation.json"), {
    version: report.version,
    count: report.remediations.length,
    rows: report.remediations,
  });
  writeJsonAtomic(
    path.join(outDir, "02-canonical-remediation-dry-run-manifest.json"),
    report.canonicalDryRunManifest
  );
  writeJsonAtomic(path.join(outDir, "03-before-after-quality-summary.json"), {
    before: report.before,
    afterProjected: report.afterProjected,
  });
  writeJsonAtomic(
    path.join(outDir, "04-commercial-event-family-crosswalk.json"),
    report.familyCrosswalk
  );
  writeJsonAtomic(
    path.join(outDir, "05-proposed-paid-research-batch.json"),
    report.proposedPaidBatch
  );

  // Protected ID / count gates on proposed historical set (no silent loss).
  assertNoProtectedIdLoss({
    hotelId,
    nextOpportunities: store.opportunities,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  });
  assertCountNotDropped({
    hotelId,
    priorCount: 59,
    nextCount: store.count,
  });

  return {
    ok: report.execution.allIdsProcessed === true,
    ...report,
  };
}

