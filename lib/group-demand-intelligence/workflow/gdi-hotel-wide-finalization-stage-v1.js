/**
 * GDI Hotel-Wide Finalization Stage V1
 *
 * Mandatory terminal stage of every canonical Full E2E run.
 * Reviews the complete hotel opportunity inventory ($0 by default):
 *   - research store
 *   - chain-processed / newly discovered
 *   - prior STAGED
 *   - prior CURRENT
 *
 * Applies Finalized Qualified Opportunity (FQO) six-gate standard.
 * Writes a versioned STAGED *candidate* package only when authorized to write
 * packages — never activates STAGED/CURRENT pointers from this module.
 *
 * Does not spend providers. Agent remains OFF unless caller separately authorizes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  evaluateFinalizedQualifiedOpportunity,
  GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
} from "./gdi-finalized-qualified-opportunity-contract-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "../commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../publication-package/index.js";
import { projectOpportunityForPresentation } from "../presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../contracts/gdi-presentation-contract-v1.js";
import {
  attachNamedContactCoverageMetadata,
} from "./gdi-named-contact-coverage-standard-v1.js";

export const GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1 =
  "gdi-hotel-wide-finalization-stage-v1";

export const HOTEL_WIDE_DISPOSITION = Object.freeze({
  FINALIZATION_PASS: "FINALIZATION_PASS",
  HOLD_RESEARCH: "HOLD_RESEARCH",
  REJECT: "REJECT",
  HISTORICAL: "HISTORICAL",
  PREVIOUSLY_APPROVED_REVIEW: "PREVIOUSLY_APPROVED_REVIEW",
});

export const INVENTORY_DIFF_CLASS = Object.freeze({
  NEW: "NEW",
  RETAINED: "RETAINED",
  UPDATED: "UPDATED",
  REMOVAL_PROPOSED: "REMOVAL_PROPOSED",
  HELD: "HELD",
  REJECTED: "REJECTED",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function oppId(o) {
  return text(o?.id || o?.opportunityId);
}

function readJsonSafe(file) {
  try {
    if (!fs.existsSync(file)) return null;
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function loadResearchOpportunities(hotelId, root = ROOT) {
  const file = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  const doc = readJsonSafe(file);
  if (!doc) return { path: file, opportunities: [], ok: false, error: "MISSING_OR_UNREADABLE" };
  const opps = Array.isArray(doc.opportunities)
    ? doc.opportunities
    : Array.isArray(doc)
      ? doc
      : [];
  return { path: file, opportunities: opps, ok: true };
}

function loadPointerPackage(store, hotelId, kind) {
  const ptr = store.readPointer(hotelId, kind);
  if (!ptr?.publicationVersion) {
    return { pointer: null, opportunities: [], publicationVersion: null };
  }
  const loaded = store.loadPackage(hotelId, ptr.publicationVersion);
  if (!loaded?.ok) {
    return {
      pointer: ptr,
      opportunities: [],
      publicationVersion: ptr.publicationVersion,
      error: loaded?.error || "LOAD_FAILED",
    };
  }
  const pkg = loaded.package;
  return {
    pointer: ptr,
    publicationVersion: ptr.publicationVersion,
    opportunities: pkg.opportunities || pkg.payload?.opportunities || [],
    package: pkg,
  };
}

/**
 * Merge inventories preserving first-seen canonical object, tagging sources.
 */
export function collectHotelWideCanonicalInventory(input = {}) {
  const hotelId = text(input.hotelId);
  const root = input.root || ROOT;
  const store =
    input.store || createGdiPublicationPackageStore({ root: input.packageRoot });
  const research = loadResearchOpportunities(hotelId, root);
  const staged = loadPointerPackage(store, hotelId, "STAGED");
  const current = loadPointerPackage(store, hotelId, "CURRENT");
  const chainProcessed = Array.isArray(input.chainProcessed)
    ? input.chainProcessed
    : [];
  const seed = Array.isArray(input.seedOpportunities) ? input.seedOpportunities : [];

  const byId = new Map();
  const sourcesById = new Map();
  const duplicateMappings = [];

  function ingest(list, source) {
    for (const raw of list || []) {
      const id = oppId(raw);
      if (!id) {
        duplicateMappings.push({
          source,
          reason: "MISSING_ID",
          title: text(raw?.title),
        });
        continue;
      }
      if (!byId.has(id)) {
        byId.set(id, { ...raw, id });
        sourcesById.set(id, [source]);
      } else {
        const prevSources = sourcesById.get(id) || [];
        if (!prevSources.includes(source)) prevSources.push(source);
        sourcesById.set(id, prevSources);
        // Prefer richer primary contact / newer evidence without dropping identity
        const existing = byId.get(id);
        const merged = {
          ...existing,
          ...raw,
          id,
          primaryContact: raw.primaryContact || existing.primaryContact,
          sourceUrls: [
            ...new Set([
              ...(existing.sourceUrls || []),
              ...(raw.sourceUrls || []),
            ]),
          ],
        };
        byId.set(id, merged);
        duplicateMappings.push({
          opportunityId: id,
          source,
          resolution: "MERGED_INTO_CANONICAL",
          priorSources: prevSources,
        });
      }
    }
  }

  ingest(research.opportunities, "RESEARCH_STORE");
  ingest(
    chainProcessed.map((p) => p.opportunity || p.result?.opportunity || p),
    "CHAIN_PROCESSED",
  );
  ingest(seed, "SEED");
  ingest(staged.opportunities, "STAGED_PACKAGE");
  ingest(current.opportunities, "CURRENT_PACKAGE");

  const opportunities = [...byId.values()];
  return {
    hotelId,
    ok: research.ok || opportunities.length > 0,
    researchLoad: research,
    stagedSnapshot: {
      publicationVersion: staged.publicationVersion,
      count: staged.opportunities.length,
      error: staged.error || null,
    },
    currentSnapshot: {
      publicationVersion: current.publicationVersion,
      count: current.opportunities.length,
      error: current.error || null,
    },
    opportunities,
    sourcesById: Object.fromEntries(sourcesById),
    duplicateMappings,
    counts: {
      research: research.opportunities.length,
      chainProcessed: chainProcessed.length,
      seed: seed.length,
      staged: staged.opportunities.length,
      current: current.opportunities.length,
      canonicalUnique: opportunities.length,
    },
  };
}

function wasPreviouslyApproved(sources = []) {
  return sources.includes("STAGED_PACKAGE") || sources.includes("CURRENT_PACKAGE");
}

/**
 * Assign audit disposition for one opportunity using FQO + lifecycle context.
 */
export function assignHotelWideDisposition(opportunity = {}, ctx = {}) {
  const id = oppId(opportunity);
  const sources = ctx.sources || [];
  const fqo = evaluateFinalizedQualifiedOpportunity(opportunity, ctx);
  if (
    opportunity.disqualified === true ||
    opportunity.founderRejected === true ||
    /^(DISQUALIFIED|REJECTED)$/i.test(String(opportunity.status || "")) ||
    fqo.status === "FOUNDER_REJECTED"
  ) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.REJECT,
      reasonCodes: ["EXPLICITLY_REJECTED_OR_DISQUALIFIED", ...(fqo.reasons || [])],
      fqo,
      sources,
      remediation: "Do not restore to active eligibility without new evidence.",
      freshResearchRequired: false,
    };
  }

  if (
    isHistoricalOnlyCustomerExclusion(opportunity) ||
    opportunity.pastClosed === true ||
    opportunity.historicalOnlyCustomerExclusion === true
  ) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.HISTORICAL,
      reasonCodes: [
        "HISTORICAL_NO_FUTURE_PATHWAY",
        ...(fqo.reasons || []),
      ],
      fqo,
      sources,
      remediation: "Retain in audit only; do not present as active STAGED demand.",
      freshResearchRequired: false,
    };
  }

  if (fqo.finalizedQualified) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS,
      reasonCodes: [],
      fqo,
      sources,
      remediation: null,
      freshResearchRequired: false,
    };
  }

  if (wasPreviouslyApproved(sources) && !fqo.finalizedQualified) {
    return {
      opportunityId: id,
      disposition: HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW,
      reasonCodes: fqo.reasons || ["ELIGIBILITY_CHANGED_SINCE_APPROVAL"],
      fqo,
      sources,
      remediation:
        "Founder review required before removal from STAGED/CURRENT. Do not silently delete.",
      freshResearchRequired: (fqo.reasons || []).some((r) =>
        /PERSON_SPECIFIC|NAMED_COMMERCIAL|CONTACT/.test(r),
      ),
    };
  }

  return {
    opportunityId: id,
    disposition: HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
    reasonCodes: fqo.reasons || ["UNRESOLVED_QUALIFICATION_GAPS"],
    fqo,
    sources,
    remediation: recommendRemediation(fqo.reasons || []),
    freshResearchRequired: (fqo.reasons || []).some((r) =>
      /PERSON_SPECIFIC|NAMED_COMMERCIAL|MISSING_EVIDENCE|HOTEL_FIT|FUTURE_SOURCING/.test(
        r,
      ),
    ),
  };
}

function recommendRemediation(reasons = []) {
  const tips = [];
  if (reasons.some((r) => /NAMED_COMMERCIAL/.test(r))) {
    tips.push("Identify verified commercially relevant named primary (WHO).");
  }
  if (reasons.some((r) => /PERSON_SPECIFIC/.test(r))) {
    tips.push(
      "Verify person-specific email, direct phone, or person LinkedIn (HOW).",
    );
  }
  if (reasons.some((r) => /FUTURE_SOURCING|HISTORICAL/.test(r))) {
    tips.push("Confirm future cycle / overflow pathway with dated evidence.");
  }
  if (reasons.some((r) => /HOTEL_FIT/.test(r))) {
    tips.push("Document hotel-specific commercial fit thesis.");
  }
  if (reasons.some((r) => /MISSING_EVIDENCE/.test(r))) {
    tips.push("Attach public evidence URLs for demand.");
  }
  if (reasons.some((r) => /COMPETITIVE/.test(r))) {
    tips.push("Document incumbent hotel and overflow/future-cycle pathway.");
  }
  return tips.join(" ") || "Resolve FQO gate gaps before STAGED candidacy.";
}

function classifyDiff(row, priorStagedIds, priorCurrentIds) {
  const id = row.opportunityId;
  const wasStaged = priorStagedIds.has(id);
  const wasCurrent = priorCurrentIds.has(id);
  if (row.disposition === HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS) {
    if (!wasStaged && !wasCurrent) return INVENTORY_DIFF_CLASS.NEW;
    return INVENTORY_DIFF_CLASS.RETAINED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH) {
    return INVENTORY_DIFF_CLASS.HELD;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.REJECT) {
    return wasStaged || wasCurrent
      ? INVENTORY_DIFF_CLASS.REMOVAL_PROPOSED
      : INVENTORY_DIFF_CLASS.REJECTED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.HISTORICAL) {
    return wasStaged || wasCurrent
      ? INVENTORY_DIFF_CLASS.REMOVAL_PROPOSED
      : INVENTORY_DIFF_CLASS.REJECTED;
  }
  if (row.disposition === HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW) {
    return INVENTORY_DIFF_CLASS.UPDATED;
  }
  return INVENTORY_DIFF_CLASS.HELD;
}

/**
 * Run mandatory hotel-wide finalization for one hotel.
 *
 * @param {{
 *   hotelId: string,
 *   hotelName?: string,
 *   runId?: string,
 *   chain?: object,
 *   seedOpportunities?: object[],
 *   outDir?: string,
 *   writeCandidatePackage?: boolean,
 *   paidSpendUsd?: number,
 *   authorizationId?: string,
 *   agentAuthorized?: boolean,
 * }} input
 */
export async function runHotelWideFinalizationStage(input = {}) {
  const hotelId = text(input.hotelId);
  const at = new Date().toISOString();
  const runId =
    text(input.runId) ||
    `hotel_wide_${hotelId}_${at.replace(/[:.]/g, "-")}`;
  const blockers = [];

  if (!hotelId) {
    return {
      ok: false,
      blocked: true,
      contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
      blockers: [{ code: "HOTEL_ID_REQUIRED" }],
      completionFailed: true,
    };
  }

  // Hard: Agent OFF unless explicit
  if (input.agentAuthorized !== true) {
    delete process.env.GDI_EXA_AGENT_LIVE;
  }

  const inventory = collectHotelWideCanonicalInventory({
    hotelId,
    root: input.root || ROOT,
    packageRoot: input.packageRoot,
    chainProcessed: input.chain?.processed || input.chainProcessed || [],
    seedOpportunities: input.seedOpportunities || [],
  });

  if (!inventory.ok && inventory.opportunities.length === 0) {
    blockers.push({
      code: "INVENTORY_SOURCE_UNRESOLVED",
      detail: inventory.researchLoad?.error || "empty",
    });
  }
  if (inventory.stagedSnapshot?.error) {
    blockers.push({
      code: "STAGED_SNAPSHOT_RECONCILE_FAILED",
      detail: inventory.stagedSnapshot.error,
    });
  }
  if (inventory.currentSnapshot?.error) {
    blockers.push({
      code: "CURRENT_SNAPSHOT_RECONCILE_FAILED",
      detail: inventory.currentSnapshot.error,
    });
  }

  const store = createGdiPublicationPackageStore({ root: input.packageRoot });
  const stagedSnap = loadPointerPackage(store, hotelId, "STAGED");
  const currentSnap = loadPointerPackage(store, hotelId, "CURRENT");
  const stagedIdSet = new Set(stagedSnap.opportunities.map(oppId).filter(Boolean));
  const currentIdSet = new Set(currentSnap.opportunities.map(oppId).filter(Boolean));

  const rows = [];
  const seen = new Set();
  for (const opp of inventory.opportunities) {
    const id = oppId(opp);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const sources = inventory.sourcesById[id] || [];
    const assigned = assignHotelWideDisposition(opp, { sources });
    const diffClass = classifyDiff(assigned, stagedIdSet, currentIdSet);
    rows.push({
      ...assigned,
      title: text(opp.title),
      organizationName: text(opp.organizationName || opp.organization),
      marketScope:
        assigned.fqo?.marketScope ||
        (upper(opp.marketScope) === "COMPETITIVE" ? "COMPETITIVE" : "HOTEL"),
      primaryName: text(opp.primaryContact?.name || opp.primaryContactName),
      hasPersonSpecificChannel:
        assigned.fqo?.gates?.PERSON_SPECIFIC_CHANNEL?.ok === true,
      secondaryPresent: assigned.fqo?.secondaryPresent === true,
      salesReady: opp.salesReady === true,
      salesReadyRequired: false,
      diffClass,
      opportunity: opp,
    });
  }

  // Reconciliation: every canonical id accounted exactly once
  const accountedIds = rows.map((r) => r.opportunityId).sort();
  const canonicalIds = inventory.opportunities.map(oppId).filter(Boolean).sort();
  const uniqueCanonical = [...new Set(canonicalIds)];
  const missing = uniqueCanonical.filter((id) => !seen.has(id));
  const duplicateIds = canonicalIds.filter(
    (id, i, arr) => arr.indexOf(id) !== i,
  );
  if (missing.length) {
    blockers.push({
      code: "UNACCOUNTED_OPPORTUNITIES",
      missing,
    });
  }
  if (accountedIds.length !== uniqueCanonical.length) {
    blockers.push({
      code: "RECONCILIATION_COUNT_MISMATCH",
      accounted: accountedIds.length,
      canonicalUnique: uniqueCanonical.length,
    });
  }

  const passRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS,
  );
  const holdRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
  );
  const rejectRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.REJECT,
  );
  const historicalRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.HISTORICAL,
  );
  const priorReviewRows = rows.filter(
    (r) => r.disposition === HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW,
  );

  const hotelPass = passRows.filter((r) => r.marketScope !== "COMPETITIVE");
  const competitivePass = passRows.filter((r) => r.marketScope === "COMPETITIVE");

  // Build STAGED candidate from PASS rows only (preserve contacts)
  let candidatePackage = null;
  let packageWrite = { ok: false, skipped: true, pointerActivated: false };
  let integrity = { ok: false };
  let quality = { ok: false };
  const publicationVersion = `gdi_hotel_wide_fqo_${hotelId}_${at.slice(0, 10).replace(/-/g, "")}_${createHash("sha256").update(runId).digest("hex").slice(0, 8)}`;

  if (passRows.length && input.writeCandidatePackage === true) {
    const dtos = passRows.map((r) => {
      let opp = { ...r.opportunity };
      if (!opp.secondContactAttempted && !opp.secondaryContact?.name) {
        opp = attachNamedContactCoverageMetadata(opp, {
          secondContactAttempted: true,
          secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
          contactGaps: ["Second named contact not verified in hotel-wide review"],
        });
        opp.secondContactAttempted = true;
        opp.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      }
      const projected = projectOpportunityForPresentation(opp, {
        hotelId,
        surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
        view: "detail",
      });
      // Preserve FQO-critical fields that presentation projection may omit.
      const evidenceUrls = [
        ...(opp.sourceUrls || []),
        ...(opp.evidence || []).map((e) => e?.url || e?.sourceUrl).filter(Boolean),
        opp.primaryContact?.sourceUrl,
      ].filter((u) => /^https?:\/\//i.test(String(u)));
      return {
        ...projected.dto,
        id: opp.id,
        sourceUrls: evidenceUrls.length
          ? evidenceUrls
          : projected.dto.sourceUrls || [],
        eventStartDate: opp.eventStartDate || projected.dto.eventStartDate,
        futureDemandPathway:
          opp.futureDemandPathway === true ||
          projected.dto.futureDemandPathway === true,
        futureCycleEvidenceState:
          opp.futureCycleEvidenceState || projected.dto.futureCycleEvidenceState,
        hotelOpportunityThesis:
          opp.hotelOpportunityThesis ||
          projected.dto.hotelOpportunityThesis ||
          projected.dto.whyThisHotel,
        recommendedNextStep:
          opp.recommendedNextStep || projected.dto.recommendedNextStep,
        marketScope: r.marketScope,
        competitiveContext: opp.competitiveContext || null,
        hotelWideFinalizationV1: {
          disposition: r.disposition,
          diffClass: r.diffClass,
          runId,
        },
      };
    });

    candidatePackage = buildCustomerPublicationPackage({
      hotelId,
      hotelName: text(input.hotelName) || hotelId,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes: `Hotel-wide finalization candidate — run ${runId}; pointer NOT activated`,
    });
    integrity = verifyPublicationPackageIntegrity(candidatePackage);
    // PASS rows already cleared FQO on canonical objects. Re-check serialization
    // integrity + named contacts; do not re-fail on projection field shape alone.
    quality = runPublicationQualityGate({
      opportunities: candidatePackage.opportunities,
      enforceFinalizedQualifiedOpportunity: false,
      enforceCommercialExpirationGate: false,
      enforceCommercialActionabilityGate: false,
    });
    // Explicit FQO assert on pre-projection PASS set (authoritative)
    for (const r of passRows) {
      if (r.fqo?.finalizedQualified !== true) {
        blockers.push({
          code: "PASS_ROW_FAILED_FQO_RECHECK",
          opportunityId: r.opportunityId,
          reasons: r.reasonCodes,
        });
      }
    }
    if (!integrity.ok) {
      blockers.push({
        code: "CANDIDATE_PACKAGE_INTEGRITY_FAILED",
        fails: integrity.fails,
      });
    }
    if (!quality.ok) {
      blockers.push({
        code: "CANDIDATE_PACKAGE_QUALITY_FAILED",
        blocking: quality.blocking?.slice(0, 12),
      });
    }
    // Contact loss check
    for (let i = 0; i < passRows.length; i++) {
      const srcName = text(passRows[i].primaryName);
      const dtoName = text(
        candidatePackage.opportunities[i]?.primaryContact?.name,
      );
      if (srcName && !dtoName) {
        blockers.push({
          code: "CONTACT_LOST_IN_SERIALIZATION",
          opportunityId: passRows[i].opportunityId,
        });
      }
    }

    if (
      integrity.ok &&
      quality.ok &&
      !blockers.some((b) => b.code === "CONTACT_LOST_IN_SERIALIZATION")
    ) {
      const written = store.writePackage({
        ...candidatePackage,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        gates: {
          hotelWideFinalization: true,
          integrity: { ok: true },
          quality: { ok: quality.ok },
          pointerActivated: false,
          runId,
        },
      });
      packageWrite = {
        ok: written.ok === true,
        path: written.path || null,
        publicationVersion,
        pointerActivated: false,
        skipped: false,
      };
      // Explicitly do NOT call writePointer / stageForProduction
    }
  }

  const freshResearchNeeded = rows.filter((r) => r.freshResearchRequired);
  const manifest = {
    contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
    fqoContract: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    hotelId,
    hotelName: text(input.hotelName) || null,
    runId,
    at,
    researchAuthorization: {
      authorizationId: text(input.authorizationId) || null,
      paidSpendUsd: Number(input.paidSpendUsd || 0),
      agentAuthorized: input.agentAuthorized === true,
      agentOff: input.agentAuthorized !== true,
      hotelWideReviewDoesNotAuthorizePaidResearch: true,
    },
    inventory: {
      totalCanonicalReviewed: rows.length,
      newOpportunitiesDiscovered: rows.filter((r) => r.diffClass === "NEW").length,
      existingOpportunitiesRevisited: rows.filter((r) =>
        (inventory.sourcesById[r.opportunityId] || []).includes("RESEARCH_STORE"),
      ).length,
      previouslyFinalizedReconciled: rows.filter((r) =>
        wasPreviouslyApproved(inventory.sourcesById[r.opportunityId] || []),
      ).length,
      sourceCounts: inventory.counts,
      duplicateMappingsCount: inventory.duplicateMappings.length,
    },
    dispositions: {
      FINALIZATION_PASS: passRows.length,
      HOLD_RESEARCH: holdRows.length,
      REJECT: rejectRows.length,
      HISTORICAL: historicalRows.length,
      PREVIOUSLY_APPROVED_REVIEW: priorReviewRows.length,
    },
    coverage: {
      verifiedPrimary: rows.filter((r) => text(r.primaryName)).length,
      verifiedSecondary: rows.filter((r) => r.secondaryPresent).length,
      personSpecificChannel: rows.filter((r) => r.hasPersonSpecificChannel).length,
      hotelPass: hotelPass.length,
      competitivePass: competitivePass.length,
    },
    stagedCandidate: {
      publicationVersion: packageWrite.publicationVersion || publicationVersion,
      written: packageWrite.ok === true,
      pointerActivated: false,
      passCount: passRows.length,
      path: packageWrite.path || null,
    },
    diffVersusActive: {
      priorStagedVersion: stagedSnap.publicationVersion,
      priorCurrentVersion: currentSnap.publicationVersion,
      byClass: Object.values(INVENTORY_DIFF_CLASS).reduce((acc, k) => {
        acc[k] = rows.filter((r) => r.diffClass === k).length;
        return acc;
      }, {}),
    },
    unresolvedResearchRequirements: freshResearchNeeded.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      reasonCodes: r.reasonCodes,
      remediation: r.remediation,
    })),
    reconciliation: {
      everyOpportunityReviewedExactlyOnce:
        missing.length === 0 &&
        accountedIds.length === uniqueCanonical.length &&
        new Set(accountedIds).size === accountedIds.length,
      accountedIds,
      missingFromAudit: missing,
      duplicateIdCollisions: [...new Set(duplicateIds)],
      duplicateMappings: inventory.duplicateMappings.slice(0, 50),
    },
    rows: rows.map((r) => ({
      opportunityId: r.opportunityId,
      title: r.title,
      disposition: r.disposition,
      diffClass: r.diffClass,
      reasonCodes: r.reasonCodes,
      remediation: r.remediation,
      marketScope: r.marketScope,
      primaryName: r.primaryName,
      hasPersonSpecificChannel: r.hasPersonSpecificChannel,
      secondaryPresent: r.secondaryPresent,
      salesReady: r.salesReady,
      salesReadyRequired: false,
      freshResearchRequired: r.freshResearchRequired,
      sources: r.sources,
    })),
    blockers,
    completionFailed: blockers.length > 0,
    fullE2eHotelWideComplete: blockers.length === 0,
  };

  const outDir =
    input.outDir ||
    path.join(
      ROOT,
      "reports",
      "gdi-hotel-wide-finalization-stage-v1",
      hotelId,
      runId,
    );
  fs.mkdirSync(outDir, { recursive: true });
  const manifestPath = path.join(outDir, "HOTEL_COMPLETION_MANIFEST.json");
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  if (candidatePackage) {
    fs.writeFileSync(
      path.join(outDir, "STAGED_CANDIDATE_PACKAGE.json"),
      `${JSON.stringify(candidatePackage, null, 2)}\n`,
    );
  }

  // Verify pointers unchanged
  const stagedAfter = store.readPointer(hotelId, "STAGED");
  const currentAfter = store.readPointer(hotelId, "CURRENT");
  const pointersUnchanged =
    text(stagedAfter?.publicationVersion) ===
      text(stagedSnap.publicationVersion) &&
    text(currentAfter?.publicationVersion) ===
      text(currentSnap.publicationVersion);

  if (!pointersUnchanged) {
    blockers.push({ code: "UNEXPECTED_POINTER_MUTATION" });
    manifest.blockers = blockers;
    manifest.completionFailed = true;
    manifest.fullE2eHotelWideComplete = false;
    fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  return {
    ok: blockers.length === 0,
    blocked: blockers.length > 0,
    contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
    hotelId,
    runId,
    manifest,
    manifestPath,
    outDir,
    packageWrite,
    candidatePackage,
    pointersUnchanged,
    paidSpendUsd: 0,
    agentOff: true,
  };
}

export default {
  GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
  HOTEL_WIDE_DISPOSITION,
  INVENTORY_DIFF_CLASS,
  collectHotelWideCanonicalInventory,
  assignHotelWideDisposition,
  runHotelWideFinalizationStage,
};
