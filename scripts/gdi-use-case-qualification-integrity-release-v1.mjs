#!/usr/bin/env node
/**
 * Authorized release: use-case qualification integrity + PE seven restore +
 * Wisedocs/Shipyard basis repair.
 *
 *   node scripts/gdi-use-case-qualification-integrity-release-v1.mjs --dry-run
 *   node scripts/gdi-use-case-qualification-integrity-release-v1.mjs --apply
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { upsertSingleOpportunity } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  evaluateProspectPublicationEligibility,
  authorizeProspectPublicationPromotion,
  REVIEWED_PUBLICATION_BASIS,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import {
  isReviewedProspectCustomerVisible,
} from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-use-case-qualification-integrity/release");
const apply = process.argv.includes("--apply");
const dryRun = !apply;

const ACTIVE_HOTELS = ["rec0eTEnCqlsGJJt9", "recnPB3Q02NO8Ma1o"];
const AUTHORIZED_IDS = Object.freeze({
  peRestore: [
    "gdi_pe_295cfef5822e63c3", // Sparks Gallery
    "gdi_pe_879047c3211d2e82", // City Cruises
    "gdi_pe_5ce20a54b903e243", // University Club
    "gdi_pe_ca47fef6aa622566", // deck655
    "gdi_pe_5f72b9d7665314b5", // Flagship
    "gdi_pe_4b9b49bae4136428", // Nolita House
    "gdi_pe_a7a2fda2a474aa0b", // The House Next Door
  ],
  basisRepair: [
    "gdi_sc_0fe98055e8df9f75", // Wisedocs
    "gdi_sc_188e25b6b3b56637", // Shipyard
  ],
});
const AUTHORIZED_SET = new Set([
  ...AUTHORIZED_IDS.peRestore,
  ...AUTHORIZED_IDS.basisRepair,
]);

const QUEUED_HOTEL_NAMES = [
  "Renaissance Times Square",
  "Falls Church Marriott Fairview Park",
  "Renaissance Arlington Capital View",
  // remaining three of six-hotel batch if named in configs
];

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function listConfiguredHotelIds() {
  const dir = path.join(ROOT, "config/group-demand-intelligence/hotels");
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/, ""));
}

function visibilitySnapshot(opp, hotelId) {
  const elig = evaluateProspectPublicationEligibility(opp);
  const reviewed = isReviewedProspectCustomerVisible(opp, { hotelId });
  const filtered = filterCustomerFacingOpportunities([opp], { hotelId });
  return {
    customerVisible: opp.customerVisible === true,
    prospectHotelFacing: opp.prospectHotelFacing === true,
    prospectPublicationBasis: opp.prospectPublicationBasis || null,
    eligibility: elig.eligibility,
    hotelFacingElig: elig.hotelFacing === true,
    suitabilityFailed: elig.suitability?.failed || [],
    useCase: elig.suitability?.useCase || null,
    routeReason: elig.route?.reason || null,
    reviewedPathVisible: reviewed,
    filterVisible: filtered.length === 1,
  };
}

function evidenceOk(opp) {
  const sources = [
    ...(Array.isArray(opp.sources) ? opp.sources : []),
    opp.officialSource,
    opp.sourceUrl,
  ].filter(Boolean);
  const hasSource = sources.some((s) => {
    const u = typeof s === "string" ? s : s?.url;
    return u && /^https?:\/\//i.test(String(u));
  });
  const hasFit =
    (opp.hotelFitScore != null && Number(opp.hotelFitScore) > 0) ||
    Boolean(String(opp.fitExplanation || opp.hotelOpportunityThesis || "").trim());
  return { hasSource, hasFit, sourceCount: sources.length };
}

async function freezeBaseline(hotelIds) {
  const baseline = {
    generatedAt: new Date().toISOString(),
    hotels: {},
  };
  for (const hotelId of hotelIds) {
    try {
      invalidateGdiHotelReadCache(hotelId);
    } catch {
      /* optional */
    }
    let doc;
    try {
      doc = await loadOpportunitiesCanonical(hotelId);
    } catch (err) {
      baseline.hotels[hotelId] = {
        error: err.message,
        opportunities: [],
      };
      continue;
    }
    const rows = (doc.opportunities || []).map((o) => ({
      id: o.id,
      organizationName: o.organizationName || o.company || null,
      title: o.title || null,
      opportunityType: o.opportunityType || null,
      researchSegment: o.researchSegment || null,
      ...visibilitySnapshot(o, hotelId),
    }));
    baseline.hotels[hotelId] = {
      persistence: doc.persistence,
      count: rows.length,
      facingFilterCount: rows.filter((r) => r.filterVisible).length,
      reviewedVisibleCount: rows.filter((r) => r.reviewedPathVisible).length,
      opportunities: rows,
    };
    // Backup full docs for active hotels only
    if (ACTIVE_HOTELS.includes(hotelId)) {
      const bak = path.join(
        OUT,
        "backups",
        `${hotelId}-opportunities-${baseline.generatedAt.replace(/[:.]/g, "-")}.json`
      );
      ensureDir(path.dirname(bak));
      fs.writeFileSync(bak, JSON.stringify(doc, null, 2) + "\n");
    }
  }
  return baseline;
}

function replayContractDiff(baseline) {
  const diffs = [];
  for (const [hotelId, hotel] of Object.entries(baseline.hotels)) {
    for (const row of hotel.opportunities || []) {
      // Proposed = eligibility under new contract + existing basis/facing for reviewed path
      // Simulate: if HOTEL_FACING elig and (basis present OR would need promotion)
      const currentlyReviewed = row.reviewedPathVisible === true;
      const currentlyFilter = row.filterVisible === true;
      // Proposed reviewed visibility if we only ship contract (no writes):
      // needs basis + facing + elig
      const proposedReviewedIfContractOnly =
        row.hotelFacingElig &&
        row.prospectPublicationBasis === REVIEWED_PUBLICATION_BASIS &&
        row.prospectHotelFacing === true &&
        row.customerVisible !== false;

      // Proposed after authorized promotion for authorized IDs that qualify
      const inAuthorized = AUTHORIZED_SET.has(row.id);
      let proposedAfterAuthorized = currentlyReviewed;
      let action = "none";
      if (inAuthorized && row.hotelFacingElig) {
        if (
          row.prospectPublicationBasis !== REVIEWED_PUBLICATION_BASIS ||
          !currentlyReviewed
        ) {
          proposedAfterAuthorized = true;
          action = AUTHORIZED_IDS.basisRepair.includes(row.id)
            ? "basis_repair"
            : "pe_restore_via_contract";
        }
      } else if (
        !inAuthorized &&
        proposedReviewedIfContractOnly !== currentlyReviewed
      ) {
        action = "additional_contract_effect_hold";
        proposedAfterAuthorized = currentlyReviewed; // hold — do not apply
      }

      if (
        currentlyReviewed !== proposedReviewedIfContractOnly ||
        currentlyReviewed !== proposedAfterAuthorized ||
        inAuthorized
      ) {
        diffs.push({
          hotelId,
          opportunityId: row.id,
          organizationName: row.organizationName,
          useCase: row.useCase,
          currentVisibility: {
            reviewed: currentlyReviewed,
            filter: currentlyFilter,
            basis: row.prospectPublicationBasis,
            facing: row.prospectHotelFacing,
            eligibility: row.eligibility,
          },
          proposedVisibilityContractOnly: {
            reviewed: proposedReviewedIfContractOnly,
          },
          proposedVisibilityAuthorizedRelease: {
            reviewed: proposedAfterAuthorized,
            action,
          },
          ruleEvidence: {
            suitabilityFailed: row.suitabilityFailed,
            routeReason: row.routeReason,
          },
          withinAuthorizedChangeSet: inAuthorized,
        });
      }
    }
  }
  return diffs;
}

async function recheckAndApplyNine() {
  const results = [];
  const byHotel = {
    rec0eTEnCqlsGJJt9: await loadOpportunitiesCanonical("rec0eTEnCqlsGJJt9"),
    recnPB3Q02NO8Ma1o: await loadOpportunitiesCanonical("recnPB3Q02NO8Ma1o"),
  };

  const allIds = [...AUTHORIZED_IDS.peRestore, ...AUTHORIZED_IDS.basisRepair];
  for (const id of allIds) {
    let hotelId = null;
    let opp = null;
    for (const [hid, doc] of Object.entries(byHotel)) {
      const found = (doc.opportunities || []).find((o) => o.id === id);
      if (found) {
        hotelId = hid;
        opp = found;
        break;
      }
    }
    if (!opp) {
      results.push({
        opportunityId: id,
        disposition: "MISSING",
        applied: false,
        reason: "not_found_in_canonical",
      });
      continue;
    }

    const before = visibilitySnapshot(opp, hotelId);
    const evidence = evidenceOk(opp);
    const elig = evaluateProspectPublicationEligibility(opp);

    // Fresh qualification gate
    if (!elig.hotelFacing) {
      results.push({
        hotelId,
        opportunityId: id,
        organizationName: opp.organizationName,
        disposition: "RETAIN_INTERNAL",
        applied: false,
        reason: `eligibility_failed:${(elig.reasons || []).join(",")}`,
        before,
        evidence,
        eligibility: elig.eligibility,
      });
      continue;
    }
    if (!evidence.hasSource || !evidence.hasFit) {
      results.push({
        hotelId,
        opportunityId: id,
        organizationName: opp.organizationName,
        disposition: "RETAIN_INTERNAL",
        applied: false,
        reason: !evidence.hasSource
          ? "missing_source_references"
          : "missing_hotel_fit_evidence",
        before,
        evidence,
      });
      continue;
    }

    // Conflict check: do not clobber newer contact-depth fields
    const contactFingerprint = {
      name: opp.primaryContact?.name || opp.primaryContactName || null,
      personEmail: opp.primaryContactEmail || opp.primaryContact?.email || null,
      namedStatus: opp.namedContactResearchStatus || null,
      depthOutcome: opp.contactDepthResearch?.outcome || null,
    };

    const promotion = authorizeProspectPublicationPromotion(opp, {
      authorizePublication: true,
      reason: AUTHORIZED_IDS.basisRepair.includes(id)
        ? "authorized_wisedocs_shipyard_basis_repair"
        : "authorized_pe_seven_restore_via_use_case_contract",
      authorizedBy: "gdi-use-case-qualification-integrity-release-v1",
    });

    if (!promotion.ok) {
      results.push({
        hotelId,
        opportunityId: id,
        organizationName: opp.organizationName,
        disposition: "RETAIN_INTERNAL",
        applied: false,
        reason: promotion.reason,
        before,
        evidence,
      });
      continue;
    }

    // Preserve contact research on patched object
    const patched = {
      ...promotion.patched,
      primaryContact: opp.primaryContact || promotion.patched.primaryContact,
      primaryContactName:
        opp.primaryContactName || promotion.patched.primaryContactName,
      primaryContactEmail:
        opp.primaryContactEmail || promotion.patched.primaryContactEmail,
      primaryContactPhone:
        opp.primaryContactPhone || promotion.patched.primaryContactPhone,
      functionalContactEmail:
        opp.functionalContactEmail || promotion.patched.functionalContactEmail,
      namedContactResearchStatus: opp.namedContactResearchStatus,
      contactDepthResearch: opp.contactDepthResearch,
      contactResponsibilityClass: opp.contactResponsibilityClass,
      hotelFitScore: opp.hotelFitScore,
      fitExplanation: opp.fitExplanation,
      hotelOpportunityThesis: opp.hotelOpportunityThesis,
      sources: opp.sources,
      customerFeedback: opp.customerFeedback,
      hotelFeedback: opp.hotelFeedback,
      salesActions: opp.salesActions,
      decisionOutcomes: opp.decisionOutcomes,
    };

    let persist = { ok: true, dryRun: true };
    if (!dryRun) {
      persist = await upsertSingleOpportunity(hotelId, patched, {
        runId: "gdi_use_case_qualification_integrity_release_v1",
      });
      // Update in-memory doc for subsequent reads
      const doc = byHotel[hotelId];
      const idx = doc.opportunities.findIndex((o) => o.id === id);
      if (idx >= 0) doc.opportunities[idx] = { ...doc.opportunities[idx], ...patched };
    }

    const after = visibilitySnapshot(patched, hotelId);
    results.push({
      hotelId,
      opportunityId: id,
      organizationName: opp.organizationName,
      disposition: AUTHORIZED_IDS.basisRepair.includes(id)
        ? "BASIS_REPAIRED"
        : "RESTORED",
      applied: !dryRun,
      reason: promotion.event?.reason || promotion.reason,
      publicationEvent: promotion.event,
      contactFingerprintPreserved: contactFingerprint,
      before,
      after,
      evidence,
      persist: {
        ok: persist.ok !== false,
        dryRun: Boolean(persist.dryRun ?? dryRun),
        persistence: persist.persistence || null,
      },
    });
  }

  return results;
}

function correctExecutionHistory() {
  const histDir = path.join(ROOT, "data/group-demand-intelligence/execution-history");
  ensureDir(histDir);
  const reportPath = path.join(
    ROOT,
    "reports/gdi-use-case-qualification-integrity/three-hotel-execution-history.json"
  );
  const prior = fs.existsSync(reportPath)
    ? JSON.parse(fs.readFileSync(reportPath, "utf8"))
    : null;

  const corrected = {
    version: "gdi-execution-history-classification-v2",
    recordedAt: new Date().toISOString(),
    corrects: prior?.runId || "gdi_three_hotel_integrity_*",
    activeHotels: [
      { hotelId: "rec0eTEnCqlsGJJt9", role: "active_research_and_publication_repair" },
      { hotelId: "recnPB3Q02NO8Ma1o", role: "active_research_and_publication_repair" },
    ],
    referenceOnly: [
      { hotelId: "recLuxvwwxID7U2B8", role: "reference_regression_only" },
    ],
    queuedNotStarted: QUEUED_HOTEL_NAMES,
    events: [
      {
        classification: "actual_mayflower_sd_research",
        stage: "lmc_planner_expansion_paid_parallel",
        hotels: ACTIVE_HOTELS,
        note: "Fresh Parallel LMC/planner discovery — not a qualification repair.",
      },
      {
        classification: "contract_testing_or_preparation",
        stage: "use_case_qualification_contract",
        mode: "unpaid_code_fix",
        customerResearchHistory: false,
        note: "Code/contract change — not a research run; exclude from customer research history.",
      },
      {
        classification: "replay_simulation",
        stage: "pe_seven_individual_reassessment",
        mode: "unpaid_read_only_then_authorized_release",
        customerResearchHistory: false,
        note: "Reassessment + authorized restore — publication repair, not new discovery research.",
      },
      {
        classification: "publication_repair",
        stage: "wisedocs_shipyard_basis_repair",
        mode: "authorized_canonical_promotion",
        customerResearchHistory: false,
        note: "Basis repair via authorizeProspectPublicationPromotion — not a research run.",
      },
      {
        classification: "segment_coverage",
        status: "partial_segment_coverage_explicit",
        note: "LMC/planner + PE restore + scaling basis repair for May+SD only. Queued six-hotel batch not started. Task completion ≠ whole-market coverage.",
      },
    ],
    priorMislabelCorrections: [
      {
        prior: "pe_seven / wisedocs stages as publicationEvent without clarifying not research",
        correction:
          "Reclassified as publication_repair / replay_simulation; excluded from customer research history.",
      },
    ],
    exclusionsFromCustomerResearchHistory: [
      "dry_runs",
      "fixtures",
      "replay_only_simulations",
      "simulated_clocks",
      "contract_testing_or_preparation",
      "publication_repair",
      "qualification_deployment_without_new_research",
    ],
    finalRunStatus: "partial_segment_coverage_explicit",
    queuedBatchStatus: "not_started",
  };

  const outHist = path.join(
    histDir,
    `gdi_qualification_integrity_release_${corrected.recordedAt.replace(/[:.]/g, "-")}.json`
  );
  fs.writeFileSync(outHist, JSON.stringify(corrected, null, 2) + "\n");

  // Rename misleading report pointer
  const correctedReport = path.join(
    ROOT,
    "reports/gdi-use-case-qualification-integrity/execution-history-corrected.json"
  );
  fs.writeFileSync(correctedReport, JSON.stringify(corrected, null, 2) + "\n");

  // Annotate prior three-hotel file (do not fabricate timestamps)
  if (prior) {
    const annotated = {
      ...prior,
      supersededBy: "execution-history-corrected.json",
      classificationNote:
        "Superseded classification: prior pe_seven/wisedocs stages were preparation/unapplied — not live research on queued hotels. Queued hotels remain not_started.",
      annotatedAt: corrected.recordedAt,
    };
    fs.writeFileSync(
      path.join(
        ROOT,
        "reports/gdi-use-case-qualification-integrity/three-hotel-execution-history.annotated.json"
      ),
      JSON.stringify(annotated, null, 2) + "\n"
    );
  }

  return { outHist, correctedReport, corrected };
}

async function main() {
  ensureDir(OUT);
  ensureDir(path.join(OUT, "backups"));

  const hotelIds = listConfiguredHotelIds();
  console.log(JSON.stringify({ phase: "freeze", hotels: hotelIds.length, dryRun }));

  const baseline = await freezeBaseline(hotelIds);
  fs.writeFileSync(
    path.join(OUT, "baseline-freeze.json"),
    JSON.stringify(baseline, null, 2) + "\n"
  );

  const diffs = replayContractDiff(baseline);
  const authorizedDiffs = diffs.filter((d) => d.withinAuthorizedChangeSet);
  const heldDiffs = diffs.filter(
    (d) =>
      !d.withinAuthorizedChangeSet &&
      d.proposedVisibilityAuthorizedRelease?.action ===
        "additional_contract_effect_hold"
  );
  fs.writeFileSync(
    path.join(OUT, "all-hotel-contract-replay-diff.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        baselineAt: baseline.generatedAt,
        totalDiffRows: diffs.length,
        authorizedChangeSetCount: authorizedDiffs.length,
        additionalHeldUnappliedCount: heldDiffs.length,
        authorizedIds: [...AUTHORIZED_SET],
        rows: diffs,
        heldAdditional: heldDiffs,
      },
      null,
      2
    ) + "\n"
  );

  const nine = await recheckAndApplyNine();
  fs.writeFileSync(
    path.join(OUT, "nine-record-before-after.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        dryRun,
        applyAuthorized: apply,
        rows: nine,
      },
      null,
      2
    ) + "\n"
  );

  const hist = correctExecutionHistory();

  // Fresh post-apply baseline for active hotels only
  const post = await freezeBaseline(ACTIVE_HOTELS);
  fs.writeFileSync(
    path.join(OUT, "post-apply-active-hotels-freeze.json"),
    JSON.stringify(post, null, 2) + "\n"
  );

  const manifest = {
    version: "gdi-use-case-qualification-authorized-manifest-v1",
    generatedAt: new Date().toISOString(),
    applyAuthorized: apply,
    dryRun,
    authorizedIds: [...AUTHORIZED_SET],
    dispositions: nine.map((r) => ({
      id: r.opportunityId,
      disposition: r.disposition,
      applied: r.applied,
      reason: r.reason,
    })),
    additionalContractEffectsHeld: heldDiffs.map((d) => d.opportunityId),
    executionHistory: hist.correctedReport,
  };
  fs.writeFileSync(
    path.join(OUT, "authorized-change-manifest.json"),
    JSON.stringify(manifest, null, 2) + "\n"
  );

  const restored = nine.filter((r) =>
    ["RESTORED", "BASIS_REPAIRED"].includes(r.disposition)
  );
  const retained = nine.filter((r) => r.disposition === "RETAIN_INTERNAL");

  console.log(
    JSON.stringify(
      {
        dryRun,
        baselineAt: baseline.generatedAt,
        authorizedAppliedOrReady: restored.length,
        retainedInternal: retained.length,
        additionalHeld: heldDiffs.length,
        out: OUT,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
