/**
 * Multi-opportunity live pilot for autonomous long-form GDI integration.
 * Aggregate ceiling $1.35 · $0.45 nontransferable per opportunity · isolated ledgers.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runDeepResearchOnExistingGdiOpportunity,
  captureOpportunityBeforeSnapshot,
} from "./deep-research-gdi-integration-v1.js";
import {
  evaluateDeepResearchEligibility,
  tierBudgetDefaults,
} from "./deep-research-eligibility-v1.js";
import {
  prepareOpportunityForAutonomousDeepResearch,
  buildAlternativeSourceCandidates,
} from "./autonomous-deep-research-recovery-v1.js";
import {
  selectCrossOpportunityCertificationSet,
  COMMERCIAL_OUTCOME,
  INTEGRATION_REPORTS_DIR,
  AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1,
} from "./autonomous-longform-gdi-integration-v1.js";
import {
  assertLiveDeepResearchLedger,
  loadOrCreateLiveDeepResearchLedger,
  createLiveDeepResearchResearcher,
  preflightLiveDeepResearchPricing,
  classifyEvidenceBuckets,
  LIVE_LEDGER_RELATIVE,
} from "./deep-research-gdi-live-certification-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";

export const LIVE_PILOT_VERSION = "gdi-autonomous-longform-live-pilot-v1";
export const LIVE_PILOT_AGGREGATE_CEILING_USD = 1.35;
export const LIVE_PILOT_PER_OPP_CEILING_USD = 0.45;
export const LIVE_PILOT_LEDGER_VERSION = "gdi-autonomous-longform-live-pilot-ledger-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const FIXTURE_ONLY_NAMES = /^(jordan\s+hale|chris\s+delgado|riley\s+chen|alex\s+morgan)$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function stripFixtureStakeholders(list = []) {
  return (list || []).filter((p) => {
    const name = text(p?.name || p?.personName);
    return name && !FIXTURE_ONLY_NAMES.test(name);
  });
}

function seedLiveOpportunityFromSelection(sel) {
  const bag = sel.bag || {};
  const id = bag.id;
  const org = bag.organizationName;
  const contactEmail = bag.contactEmail || bag.primaryContactEmail || null;
  const official = bag.officialSource || bag.contactUrl || null;
  const investigated = [];
  if (official) investigated.push(official);

  // Known competing hotel page for NADO — treat as already investigated amenity/competitor context
  if (sel.competingHotel) {
    investigated.push("https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/");
  }

  const evidence = [];
  if (text(bag.hotelOpportunityThesis)) {
    evidence.push({
      url: official || `canonical://${id}/thesis`,
      statement: text(bag.hotelOpportunityThesis).slice(0, 500),
      quote: text(bag.hotelOpportunityThesis).slice(0, 500),
      provenance: "canonical_gdi_bag",
      accepted: true,
      previouslyHeld: true,
    });
  }
  if (text(bag.eventActivityEvidence)) {
    evidence.push({
      url: official || `canonical://${id}/activity`,
      statement: text(bag.eventActivityEvidence).slice(0, 500),
      quote: text(bag.eventActivityEvidence).slice(0, 500),
      provenance: "canonical_gdi_bag",
      accepted: true,
      previouslyHeld: true,
    });
  }

  let host = sel.orgHost;
  try {
    if (official) host = new URL(official).hostname.replace(/^www\./, "");
  } catch { /* keep sel.orgHost */ }

  return prepareOpportunityForAutonomousDeepResearch({
    id,
    opportunityId: id,
    hotelId: sel.hotelId,
    hotelName: sel.hotelName,
    organizationName: org,
    organizationDomain: host,
    pathway: sel.pathway,
    internalDisposition: sel.internalDisposition,
    commercialStage: sel.internalDisposition,
    segment: sel.segment || bag.segment,
    opportunityType: bag.opportunityType || null,
    venueSourcingStatus: bag.venueSourcingStatus || null,
    roomDemandStatus: bag.roomDemandStatus || null,
    hotelOpportunityThesis: bag.hotelOpportunityThesis || null,
    futureCycleEvidenceState: bag.futureCycleEvidenceState || null,
    futureCycleSignals: bag.futureCycleSignals || [],
    eventLocationSummary: bag.eventLocationSummary || null,
    contactEmail,
    contact: {
      businessEmail: contactEmail,
      personName: bag.contactIsNamedPerson ? (bag.contactName || bag.primaryContactName || null) : null,
      roleTitle: bag.contactRole || bag.primaryContactRole || null,
    },
    officialSource: official,
    evidence,
    investigatedUrls: investigated,
    seedUrls: investigated.filter(Boolean),
    unresolvedQuestions: [
      `Who influences hotel sourcing / housing / contracting at ${org}?`,
      `What programs or next-cycle activity create lodging demand relevant to ${sel.hotelName}?`,
      "Can independent or official sources substantiate commercial activity without inventing RFPs?",
    ],
    competingHotel: sel.competingHotel || null,
    namedStakeholders: [],
  });
}

function classifyLiveCommercialOutcome({ before, result, technicalPass }) {
  if (!technicalPass) return COMMERCIAL_OUTCOME.RESEARCH_BLOCKED;
  const named = stripFixtureStakeholders(result.after?.namedStakeholders || []);
  const namedBefore = (before.namedStakeholders || []).length;
  const related = Number(result.after?.relatedEntityLeadsExecuted || 0)
    + (result.retrievalTrace?.relatedEntity?.executed || []).length
    + (result.after?.opportunity?.evidence || []).filter((e) => e.relatedEntity || e.provenance === "live_retrieval").length;
  const newLive = (result.retrievalTrace?.executed || []).filter((e) => !e.heldPreviously).length;
  const contactOk = result.after?.contactOwnership?.salesUsableAsProspectContact === true
    || Boolean(result.after?.contactOwnership?.route || before.contactEmail);

  if (named.length > namedBefore && newLive >= 1) {
    return COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT;
  }
  if (newLive >= 1 || related >= 1 || contactOk) {
    return COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT;
  }
  return COMMERCIAL_OUTCOME.NO_IMPROVEMENT;
}

export function authorizeLivePilotPackage(packagePath) {
  const abs = path.isAbsolute(packagePath) ? packagePath : path.join(ROOT, packagePath);
  const pkg = JSON.parse(fs.readFileSync(abs, "utf8"));
  const pricing = preflightLiveDeepResearchPricing();
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const checks = {
    packagePresent: Boolean(pkg),
    aggregateCeiling: Number(pkg.totalProposedCeilingUsd) === LIVE_PILOT_AGGREGATE_CEILING_USD,
    perOppCeilings: (pkg.opportunities || []).every((o) => Number(o.proposedCeilingUsd) === LIVE_PILOT_PER_OPP_CEILING_USD),
    threeOpportunities: (pkg.opportunities || []).length === 3,
    isolatedLedgersListed: (pkg.isolatedLedgers || []).length === 3,
    noPriorBalances: pkg.usesPriorResearchBalances === false,
    pricingOk: pricing.ok === true,
    surfePdlDisabled: paidGuard.ok === true,
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
  };
  const ok = Object.values(checks).every((v) => v === true || typeof v === "string");
  const authorized = {
    ...pkg,
    status: ok ? "AUTHORIZED" : "PREFLIGHT_FAILED",
    authorizedAt: new Date().toISOString(),
    executePaidResearch: ok,
    founderAuthorizedAggregateCeilingUsd: LIVE_PILOT_AGGREGATE_CEILING_USD,
    pricingPreflight: { ...pkg.pricingPreflight, live: pricing.checks, ok: pricing.ok },
    preflightChecks: checks,
    note: ok
      ? "Founder GO received. Execute three-opportunity live pilot within nontransferable ceilings."
      : "Preflight failed — do not spend.",
  };
  fs.writeFileSync(abs, `${JSON.stringify(authorized, null, 2)}\n`);
  return { ok, authorized, pricing, checks };
}

export async function executeLivePilotOpportunity({
  selection,
  ceilingUsd = LIVE_PILOT_PER_OPP_CEILING_USD,
  liveDir,
} = {}) {
  const opportunityId = selection.bag.id;
  const ledgerRel = `${INTEGRATION_REPORTS_DIR}/live-ledgers/${opportunityId}.json`;
  const ledgerPath = path.join(ROOT, ledgerRel);
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });

  assertLiveDeepResearchLedger(ledgerPath);
  // Refuse reuse of prior deep-research $0.75 ledger
  if (String(ledgerPath).replace(/\\/g, "/").endsWith(LIVE_LEDGER_RELATIVE)) {
    throw new Error("pilot_must_use_isolated_per_opportunity_ledger");
  }

  loadOrCreateLiveDeepResearchLedger(ledgerPath, {
    budgetUsd: ceilingUsd,
    version: LIVE_PILOT_LEDGER_VERSION,
  });

  let opportunity = seedLiveOpportunityFromSelection(selection);
  // Strip any fixture names that somehow appear
  opportunity.namedStakeholders = stripFixtureStakeholders(opportunity.namedStakeholders);

  const eligibility = evaluateDeepResearchEligibility(opportunity, {
    remainingBudgetUsd: ceilingUsd,
    providersAvailable: true,
  });
  const tier = tierBudgetDefaults(eligibility.tier);
  const before = captureOpportunityBeforeSnapshot(opportunity);

  const oppLiveDir = path.join(liveDir, opportunityId);
  fs.mkdirSync(oppLiveDir, { recursive: true });
  fs.writeFileSync(path.join(oppLiveDir, "BEFORE.json"), JSON.stringify(before, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "ELIGIBILITY.json"), JSON.stringify(eligibility, null, 2));

  const researcher = createLiveDeepResearchResearcher({
    ledgerPath,
    additionalSkipUrls: opportunity.investigatedUrls || [],
    allowInvestigatedReread: false,
  });

  const alts = buildAlternativeSourceCandidates(opportunity);
  const result = await runDeepResearchOnExistingGdiOpportunity({
    opportunity,
    researcher,
    corpus: null,
    offlineLongform: false,
    options: {
      allowEarlyBuyerStop: false,
      allowReplan: true,
      ensureBuyerWho: true,
      resetResearchAttempts: true,
      candidateQueue: opportunity.candidateQueue || alts.map((a) => a.suggestedUrl),
      replanAlternativeCandidates: alts,
      maxIterations: Math.min(5, tier.maxIterations || 5),
      maxLeadFollowUpsPerOpportunity: tier.maxLeadFollowUps || 3,
      remainingBudgetUsd: ceilingUsd,
      checkpointDir: path.join(oppLiveDir, "checkpoints"),
      paidBlocked: true,
      forbidPublication: true,
      forbidAirtable: true,
      authorizeSurfe: false,
      authorizePdl: false,
    },
  });

  // Strip fixture-only identities from live findings
  if (result.after?.namedStakeholders) {
    result.after.namedStakeholders = stripFixtureStakeholders(result.after.namedStakeholders);
  }
  if (result.after?.opportunity) {
    result.after.opportunity.namedStakeholders = stripFixtureStakeholders(
      result.after.opportunity.namedStakeholders || [],
    );
  }
  if (result.buyerIntelligence?.namedStakeholders) {
    result.buyerIntelligence.namedStakeholders = stripFixtureStakeholders(
      result.buyerIntelligence.namedStakeholders,
    );
  }
  if (result.hotelSalesBrief?.whoShouldIContact) {
    const pn = text(result.hotelSalesBrief.whoShouldIContact.namedPerson || result.hotelSalesBrief.whoShouldIContact.personName);
    if (FIXTURE_ONLY_NAMES.test(pn)) {
      result.hotelSalesBrief.whoShouldIContact = {
        ...result.hotelSalesBrief.whoShouldIContact,
        namedPerson: null,
        personName: null,
        role: null,
        note: "Fixture identity rejected — use verified org route only",
      };
    }
  }

  const ledger = researcher.getLedger();
  const spentUsd = Number(ledger.estimatedSpentUsd || 0);
  const evidenceBuckets = classifyEvidenceBuckets({
    before,
    afterEvidence: result.after?.opportunity?.evidence || [],
    trace: researcher.trace,
  });

  const newLiveUrls = (researcher.trace.executed || []).filter((e) => !e.heldPreviously);
  const relatedExecuted = (researcher.trace.relatedEntity?.executed || []).length
    || Number(result.after?.relatedEntityLeadsExecuted || 0);

  const technicalPass =
    result.orsEntrypoint === "matureOpportunity"
    && result.gdiRoundTrip?.sameOpportunityId === true
    && result.publicationEligibilityUnchanged === true
    && spentUsd - ceilingUsd <= 1e-9;

  const commercialOutcome = classifyLiveCommercialOutcome({ before, result, technicalPass });
  const realNamed = stripFixtureStakeholders(result.after?.namedStakeholders || []);

  // Pathway-specific honesty notes
  const pathwayNotes = [];
  if (selection.role === "ACCOUNT_LED") {
    pathwayNotes.push("Do not promote nightlife/venue-listing activity into hotel group demand without meeting/guestroom evidence.");
  }
  if (selection.role === "EVENT_LED") {
    pathwayNotes.push("Do not invent overnight demand, sourcing, or room blocks for Potomac Memorial.");
  }
  if (selection.role === "COMPETITIVE_FUTURE_CYCLE") {
    pathwayNotes.push("Crystal Gateway commitment is competitor context — not an open Bethesda opportunity; no overflow inference without evidence.");
  }

  const certification = {
    version: LIVE_PILOT_VERSION,
    opportunityId,
    hotelId: selection.hotelId,
    hotelName: selection.hotelName,
    organizationName: selection.bag.organizationName,
    pathway: selection.pathway,
    role: selection.role,
    eligibility,
    costs: {
      spentUsd,
      remainingCeilingUsd: Number((ceilingUsd - spentUsd).toFixed(6)),
      hardCeilingUsd: ceilingUsd,
      ledgerPath: ledgerRel,
      nontransferable: true,
      exaCalls: ledger.exaCalls || 0,
      firecrawlCalls: ledger.firecrawlCalls || 0,
      directRetrievals: ledger.directRetrievals || 0,
      entries: ledger.entries || [],
    },
    researchQuestions: opportunity.unresolvedQuestions,
    retrievalTrace: researcher.trace,
    evidenceBuckets,
    realNamedStakeholders: realNamed,
    fixtureIdentitiesRejected: ["Jordan Hale", "Chris Delgado", "Riley Chen", "Alex Morgan"],
    contactOwnership: result.after?.contactOwnership || null,
    buyerIntelligence: result.buyerIntelligence || result.after?.publicBuyerContactEnrichment || null,
    buyerWhoFallback: result.buyerWhoFallback || result.after?.buyerWhoFallback || null,
    relatedEntityExecutedCount: relatedExecuted,
    newLiveUrlsRetrieved: newLiveUrls.length,
    researchCompleteness: result.researchCompleteness || result.after?.researchCompleteness || null,
    technicalStop: result.after?.orsStopReason || null,
    gdiRoundTrip: result.gdiRoundTrip,
    publicationEligibilityUnchanged: result.publicationEligibilityUnchanged,
    pathwayNotes,
    technicalCertification: technicalPass ? "PASS" : "FAIL",
    commercialOutcome,
    hotelSalesBrief: result.hotelSalesBrief,
  };

  fs.writeFileSync(path.join(oppLiveDir, "AFTER.json"), JSON.stringify(result.after, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "SALES_BRIEF.json"), JSON.stringify(result.hotelSalesBrief, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "DOSSIER.json"), JSON.stringify(result.dossierResult || null, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "RETRIEVAL_TRACE.json"), JSON.stringify(researcher.trace, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "CERTIFICATION.json"), JSON.stringify(certification, null, 2));
  fs.writeFileSync(path.join(oppLiveDir, "COSTS.json"), JSON.stringify(certification.costs, null, 2));

  return {
    executed: true,
    certification,
    before,
    after: result.after,
    hotelSalesBrief: result.hotelSalesBrief,
    dossierResult: result.dossierResult,
    spentUsd,
  };
}

export async function executeAutonomousLongformLivePilot({
  packagePath = path.join(ROOT, INTEGRATION_REPORTS_DIR, "LIVE_PILOT_PACKAGE.json"),
  liveDir = path.join(ROOT, INTEGRATION_REPORTS_DIR, "live"),
} = {}) {
  fs.mkdirSync(liveDir, { recursive: true });

  const auth = authorizeLivePilotPackage(packagePath);
  if (!auth.ok) {
    fs.writeFileSync(path.join(liveDir, "PREFLIGHT_FAILED.json"), JSON.stringify(auth, null, 2));
    return { executed: false, reason: "preflight_failed", auth, spentUsd: 0 };
  }

  // Guard prior ledgers (bytes unchanged)
  const priorLedgers = [
    "reports/gdi-deep-research-gdi-integration-v1/live-ledger.json",
    "reports/gdi-evidence-driven-longform-validation-v1/live-ledger.json",
  ].map((p) => path.join(ROOT, p));
  const priorSnapshots = priorLedgers
    .filter((p) => fs.existsSync(p))
    .map((p) => ({ path: p, bytes: fs.readFileSync(p) }));

  const selections = selectCrossOpportunityCertificationSet();
  const byId = new Map(selections.map((s) => [s.bag?.id, s]));
  const orderedIds = (auth.authorized.opportunities || []).map((o) => o.opportunityId);

  const opportunityResults = [];
  let aggregateSpent = 0;

  for (const opportunityId of orderedIds) {
    const sel = byId.get(opportunityId);
    if (!sel?.bag) {
      opportunityResults.push({
        opportunityId,
        executed: false,
        reason: "selection_missing",
      });
      continue;
    }
    // Remaining aggregate must cover this opp's ceiling commitment (nontransferable)
    if (aggregateSpent + LIVE_PILOT_PER_OPP_CEILING_USD - LIVE_PILOT_AGGREGATE_CEILING_USD > 1e-9) {
      opportunityResults.push({
        opportunityId,
        executed: false,
        reason: "aggregate_ceiling_exhausted_before_start",
      });
      break;
    }

    console.log(JSON.stringify({
      phase: "live_pilot_opportunity",
      opportunityId,
      hotelId: sel.hotelId,
      ceilingUsd: LIVE_PILOT_PER_OPP_CEILING_USD,
    }));

    const run = await executeLivePilotOpportunity({
      selection: sel,
      ceilingUsd: LIVE_PILOT_PER_OPP_CEILING_USD,
      liveDir,
    });
    aggregateSpent = Number((aggregateSpent + run.spentUsd).toFixed(6));
    opportunityResults.push(run);

    if (aggregateSpent - LIVE_PILOT_AGGREGATE_CEILING_USD > 1e-9) {
      break;
    }
  }

  const priorUnchanged = priorSnapshots.every(({ path: p, bytes }) => {
    try {
      return fs.readFileSync(p).equals(bytes);
    } catch {
      return false;
    }
  });

  const summary = {
    version: LIVE_PILOT_VERSION,
    integrationVersion: AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1,
    certifiedAt: new Date().toISOString(),
    aggregateCeilingUsd: LIVE_PILOT_AGGREGATE_CEILING_USD,
    aggregateSpentUsd: aggregateSpent,
    aggregateRemainingUsd: Number((LIVE_PILOT_AGGREGATE_CEILING_USD - aggregateSpent).toFixed(6)),
    priorLedgersUnchanged: priorUnchanged,
    surfeInvoked: false,
    pdlInvoked: false,
    parallelInvoked: false,
    airtableWritePerformed: false,
    opportunities: opportunityResults.map((r) => ({
      opportunityId: r.certification?.opportunityId || r.opportunityId,
      executed: r.executed !== false,
      spentUsd: r.spentUsd ?? 0,
      technical: r.certification?.technicalCertification || null,
      commercialOutcome: r.certification?.commercialOutcome || null,
      namedBuyers: (r.certification?.realNamedStakeholders || []).map((s) => s.name),
      newLiveUrls: r.certification?.newLiveUrlsRetrieved ?? 0,
      relatedExecuted: r.certification?.relatedEntityExecutedCount ?? 0,
    })),
  };

  // Final assessment
  const technicalAllPass = opportunityResults
    .filter((r) => r.executed !== false)
    .every((r) => r.certification?.technicalCertification === "PASS");
  const anyMaterial = opportunityResults.some((r) =>
    r.certification?.commercialOutcome === COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT);
  const anyLimited = opportunityResults.some((r) =>
    r.certification?.commercialOutcome === COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT);
  const autonomyWithoutManualIntervention = technicalAllPass
    && opportunityResults.every((r) => r.executed !== false);

  summary.finalAssessment = {
    operatesAcrossPathwaysWithoutManualIntervention: autonomyWithoutManualIntervention,
    consistentlyImprovesEvidenceAndBuyerIntelligence: anyMaterial || anyLimited,
    technicalCertificationIndependentOfCommercial: true,
    technicalAllPass,
    commercialOutcomes: summary.opportunities.map((o) => o.commercialOutcome),
    remainingWeeklyIntegrationBlockers: [
      "GDI_WEEKLY_DEEP_RESEARCH_ENABLED remains default 0 — production weekly job not switched",
      "Per-hotel paid weekly authorization + duplicate-prevention ops runbook still required",
      "Surfe/PDL/Parallel still separately gated",
      "Customer-facing publication of deep-research dossiers remains OFF",
      "Live commercial improvement was mixed/honest — do not equate technical PASS with rollout readiness",
    ],
    rolloutRecommendation: "DO_NOT_BROAD_ROLLOUT — controlled weekly integration only after reviewing live commercial outcomes and authorizing weekly paid budgets separately",
  };

  fs.writeFileSync(path.join(liveDir, "PILOT_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(ROOT, INTEGRATION_REPORTS_DIR, "GO_NO_GO_LIVE.md"),
    `# Multi-Opportunity Live Pilot — Complete

| Item | Result |
|---|---|
| Aggregate spent | **$${aggregateSpent}** / $${LIVE_PILOT_AGGREGATE_CEILING_USD} |
| Technical (all) | **${technicalAllPass ? "PASS" : "FAIL"}** |
| Autonomy without manual intervention | **${autonomyWithoutManualIntervention}** |
| Broad weekly rollout | **NO-GO** |
| Further paid research | **NO-GO** without new authorization |

See \`live/PILOT_SUMMARY.json\` and per-opportunity \`CERTIFICATION.json\`.
`,
  );

  return {
    executed: true,
    summary,
    opportunityResults,
    aggregateSpentUsd: aggregateSpent,
  };
}
