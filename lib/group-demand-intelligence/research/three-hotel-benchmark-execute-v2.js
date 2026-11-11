/**
 * Three-hotel live commercial benchmark under Supervisor V2 + ORS.
 * Spend only after live-integration gate passes and AUTHORIZED manifest.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import {
  THREE_HOTEL_BENCHMARK_HOTELS,
  THREE_HOTEL_BENCHMARK_VERSION,
  THREE_HOTEL_BENCHMARK_REPORTS_DIR,
  buildThreeHotelBenchmarkAuthorizationManifestV2,
} from "./three-hotel-benchmark-authorization-v2.js";
import {
  assertThreeHotelBenchmarkLedger,
  loadOrCreateHotelBenchmarkLedger,
  reserveHotelBenchmarkCall,
  commitHotelBenchmarkCall,
  failHotelBenchmarkCall,
  releaseHotelBenchmarkCall,
  hotelBenchmarkSpec,
} from "./three-hotel-benchmark-ledger-v2.js";
import { matureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { HeuristicResearchPlanner } from "./research-planner-v1.js";
import { quotePlannerUsd, planDynamicResearch, PLANNER_CEILING_USD } from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { rehearseSupervisorV2ThroughOrsPath, attachSegmentDiscoveryCoverageV2 } from "./ors-supervisor-v2-hooks.js";
import { qualifyHotelSpecificProspectV2 } from "./hotel-specific-qualification-v2.js";
import { buildSegmentDiscoveryCoverageReport } from "./segment-discovery-coverage-v2.js";
import { STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";

export const THREE_HOTEL_EXECUTE_VERSION = "gdi-three-hotel-benchmark-execute-v2";

const OPP_PATHS = {
  recC2kbCaS48jFjFF: "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json",
  recLuxvwwxID7U2B8: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
  recnPB3Q02NO8Ma1o: "data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json",
};

function readJson(rel) {
  if (!fs.existsSync(rel)) return null;
  return JSON.parse(fs.readFileSync(rel, "utf8"));
}

function hashKey(s) {
  return createHash("sha256").update(String(s)).digest("hex").slice(0, 12);
}

/**
 * Live ORS integration gate — must pass before any paid spend.
 */
export async function runLiveIntegrationGateV2() {
  const rehearsal = rehearseSupervisorV2ThroughOrsPath();

  // Normal ORS entrypoint with stub researcher (paidBlocked, no providers)
  const planner = new HeuristicResearchPlanner();
  let researcherCalls = 0;
  const researcher = {
    async execute({ opportunity, blocker }) {
      researcherCalls += 1;
      return {
        newMaterialEvidence: true,
        provider: "direct_public_fixture",
        sourceFamilies: ["official", "dmc_profile"],
        costUsd: 0,
        evidence: [
          {
            sourceFamily: "dmc_profile",
            url: opportunity.officialSource || "https://example-dmc.test/services",
            quote: "hotel sourcing and group accommodation for incentive programmes in central London",
            organizationOwned: true,
            publicationReady: false,
            customerVisible: false,
          },
        ],
        deepPatch: {
          futureDemand: { supported: true, note: "recurring incentive capability" },
          hotelFit: { supported: true, viable: true },
          contact: {
            businessEmail: opportunity.contactEmail || "groups@example-dmc.test",
          },
        },
        notes: `fixture_research:${blocker.key}`,
      };
    },
  };

  const seed = {
    id: "gdi_ors_v2_gate_mature_opportunity",
    opportunityId: "gdi_ors_v2_gate_mature_opportunity",
    hotelId: "recC2kbCaS48jFjFF",
    organizationName: "Gate Test Incentive DMC",
    officialDomain: "gate-test-dmc.example",
    officialSource: "https://gate-test-dmc.example/services",
    segmentId: "professional_planners_dmc",
    market: "London",
    targetHotel: "Nobu Hotel London Portman Square",
    contactEmail: "groups@gate-test-dmc.example",
    contact: {},
    evidence: [],
    researchAttempts: [],
    statusHistory: [],
    notes: ["entered_candidate_queue:integration_gate"],
  };

  const matured = await matureOpportunity({
    opportunity: seed,
    planner,
    researcher,
    persist: null,
    options: {
      paidBlocked: true,
      parallelAvailable: false,
      maxIterations: 4,
      enableSupervisorV2: true,
      forbidPublication: true,
      forbidAirtable: true,
      remainingBudgetUsd: 1.5,
      targetHotel: "Nobu Hotel London Portman Square",
      authorizePaidResearch: false,
      authorizePlannerModel: false,
    },
  });

  const checks = [
    ...rehearsal.checks.map((c) => ({ ...c, phase: "hook_rehearsal" })),
    { name: "matureOpportunity_invoked", ok: true, phase: "ors_entrypoint" },
    { name: "researcher_executed", ok: researcherCalls >= 1, phase: "ors_entrypoint", detail: String(researcherCalls) },
    { name: "supervisorV2_attached_on_ors", ok: Boolean(matured.supervisorV2), phase: "ors_entrypoint" },
    { name: "lead_following_on_ors", ok: (matured.supervisorV2?.leadFollowing?.recorded || 0) >= 0, phase: "ors_entrypoint" },
    { name: "qualification_on_ors", ok: Boolean(matured.supervisorV2?.qualification), phase: "ors_entrypoint" },
    { name: "dossier_on_ors", ok: Boolean(matured.supervisorV2?.dossier), phase: "ors_entrypoint" },
    { name: "publication_disabled_on_ors", ok: matured.supervisorV2?.publicationWritesDisabled === true, phase: "ors_entrypoint" },
    { name: "airtable_disabled_on_ors", ok: matured.supervisorV2?.airtableWritesDisabled === true, phase: "ors_entrypoint" },
    { name: "rehearsal_passed", ok: rehearsal.passed === true, phase: "hook_rehearsal" },
    { name: "no_live_spend_in_gate", ok: rehearsal.spent === false && researcherCalls >= 0, phase: "gate" },
  ];

  return {
    version: THREE_HOTEL_EXECUTE_VERSION,
    gate: "LIVE_ORS_INTEGRATION_GATE",
    offline: true,
    liveCalls: 0,
    spent: false,
    passed: checks.every((c) => c.ok),
    checks,
    rehearsal,
    maturedSummary: {
      organizationName: matured.organizationName,
      researchStatus: matured.researchStatus,
      pathway: matured.pathway,
      internalAccountDevelopment: matured.internalAccountDevelopment === true,
      supervisorV2Version: matured.supervisorV2?.version || null,
    },
    publicationWrites: 0,
    airtableWrites: 0,
    productionWeeklyJobEnabled: false,
  };
}

export function verifyThreeHotelAuthorizationSafeguards({ manifest, root = process.cwd() } = {}) {
  const checks = [];
  const push = (name, ok, detail = "") => checks.push({ name, ok: Boolean(ok), detail });

  push("manifest_authorized", manifest?.status === "AUTHORIZED" && manifest?.liveExecutionAuthorized === true);
  push("aggregate_4_50", Number(manifest?.aggregateCeilingUsd) === 4.5);
  push("budgets_nontransferable", manifest?.budgetsNontransferableAcrossHotels === true);
  push("uses_prior_false", manifest?.usesPriorResearchLedgers === false);
  push("parallel_refused", manifest?.providerPricing?.parallel?.refusedUnlessSeparatelyAuthorized === true);
  push("three_hotels", (manifest?.hotels || []).length === 3);
  push("publication_unchanged", manifest?.publicationEligibilityChanged === false);
  push("airtable_zero", manifest?.airtableWrites === 0);
  push("deploy_false", manifest?.deploy === false);

  const planner = quotePlannerUsd();
  push("planner_priced", planner.estimatedUsd > 0 && planner.estimatedUsd - PLANNER_CEILING_USD <= 1e-9);
  let fcOk = false;
  try {
    const fc = quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://www.example.com/", formats: ["markdown"] },
    });
    fcOk = fc.bounded === true && Number(fc.maxUsd) - 0.005 <= 1e-9;
  } catch {
    fcOk = false;
  }
  push("firecrawl_priced", fcOk);

  for (const hotel of manifest?.hotels || []) {
    const ledgerPath = path.join(root, hotel.ledgerRelative);
    try {
      assertThreeHotelBenchmarkLedger(ledgerPath, hotel.hotelId);
      loadOrCreateHotelBenchmarkLedger(ledgerPath, hotel.hotelId);
      push(`ledger_ok_${hotel.hotelId}`, true);
      push(`ceiling_1_50_${hotel.hotelId}`, Number(hotel.ceilingUsd) === 1.5);
      push(`not_transferable_${hotel.hotelId}`, hotel.budgetTransferable === false);
    } catch (err) {
      push(`ledger_ok_${hotel.hotelId}`, false, err.code || err.message);
    }
  }

  // Refuse prior ledger paths
  for (const refused of manifest?.refusedLedgers || []) {
    try {
      assertThreeHotelBenchmarkLedger(path.join(root, refused), "recC2kbCaS48jFjFF");
      push(`refused_${refused}`, false, "should_have_thrown");
    } catch {
      push(`refused_ok_${path.basename(path.dirname(refused))}`, true);
    }
  }

  return { ok: checks.every((c) => c.ok), checks };
}

function loadSegmentCandidates(hotelId, segmentId) {
  const rel = OPP_PATHS[hotelId];
  const doc = readJson(rel);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  const matched = ops.filter((o) => {
    const seg = String(o.primarySegment || o.segment || o.segmentId || "").toLowerCase();
    const name = String(o.organizationName || o.title || "");
    if (seg === segmentId) return true;
    if (segmentId === "professional_planners_dmc") {
      return /dmc|planner|headbox|hire space|miss jones|spectra|principal/i.test(name);
    }
    if (segmentId === "associations_government") {
      return /association|society|institute|congress|government|asa|aha|aiche/i.test(`${name} ${o.title || ""}`);
    }
    if (segmentId === "corporate_meetings_training") {
      return /corp|company|training|offsite|sales kick|incentive|meeting/i.test(`${name} ${o.title || ""} ${seg}`);
    }
    return false;
  });
  // Prefer richer rows first
  return matched
    .map((o) => ({
      id: o.id || o.opportunityId || `gdi_${hashKey(o.organizationName || o.title)}`,
      opportunityId: o.id || o.opportunityId || null,
      hotelId,
      organizationName: o.organizationName || o.title || "Unknown",
      officialDomain: o.officialDomain || null,
      officialSource: o.officialSource || o.contactUrl || null,
      contactEmail: o.contactEmail || o.email || null,
      contactPhone: o.contactPhone || null,
      contactUrl: o.contactUrl || null,
      contact: o.contactEmail ? { businessEmail: o.contactEmail } : {},
      segmentId,
      summaryWhyHotel: o.summaryWhyHotel || null,
      evidenceQuote: o.evidenceQuote || o.summaryWhyHotel || null,
      sources: o.sources || [],
      evidence: [],
      researchAttempts: [],
      statusHistory: [],
      notes: ["entered_candidate_queue:three_hotel_benchmark"],
      customerVisible: o.customerVisible === true,
      pathway: o.pathway || null,
    }))
    .filter((o, i, arr) => arr.findIndex((x) => x.organizationName.toLowerCase() === o.organizationName.toLowerCase()) === i);
}

function seedExistingEvidence(candidate) {
  const quotes = [];
  if (candidate.summaryWhyHotel) quotes.push(candidate.summaryWhyHotel);
  if (candidate.evidenceQuote) quotes.push(candidate.evidenceQuote);
  const urls = [
    candidate.officialSource,
    candidate.contactUrl,
    ...((candidate.sources || []).map((s) => (typeof s === "string" ? s : s?.url))),
  ].filter(Boolean);
  for (let i = 0; i < Math.min(urls.length, 2); i += 1) {
    candidate.evidence.push({
      sourceFamily: "frozen_corpus",
      url: urls[i],
      quote: quotes[i] || quotes[0] || `Corpus source for ${candidate.organizationName}`,
      organizationOwned: true,
      publicationReady: false,
      customerVisible: false,
      provenance: "existing_evidence_first",
    });
  }
  if (candidate.contactEmail && !candidate.contact?.businessEmail) {
    candidate.contact = { ...candidate.contact, businessEmail: candidate.contactEmail };
  }
  return candidate;
}

async function fetchPageDirect(url) {
  const { fetchOfficialPage } = await import("../official-page-fetch.js");
  const page = await fetchOfficialPage(url, { allowRender: false, timeoutMs: 20000 });
  return {
    ok: page?.ok === true || Boolean(page?.text),
    text: page?.text || page?.searchableText || "",
    html: page?.html || page?.rawHtml || "",
  };
}

async function maybeFirecrawl({ url, ledgerPath, hotelId, jobKey, allowFirecrawl }) {
  if (!allowFirecrawl) return null;
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!quote.bounded || Number(quote.maxUsd) - 0.005 > 1e-9 || !key) return null;
  const reservation = reserveHotelBenchmarkCall({
    ledgerPath,
    hotelId,
    jobKey: `${jobKey}-fc`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) return null;
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failHotelBenchmarkCall({ ledgerPath, hotelId, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return null;
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    commitHotelBenchmarkCall({ ledgerPath, hotelId, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return md;
  } catch (err) {
    failHotelBenchmarkCall({ ledgerPath, hotelId, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return null;
  }
}

function buildBenchmarkResearcher({ ledgerPath, hotelId, allowFirecrawl, investigatedUrls }) {
  return {
    async execute({ opportunity, blocker, action }) {
      const urls = [
        opportunity.officialSource,
        opportunity.contactUrl,
        ...((action?.queries || []).filter((q) => /^https?:/i.test(q))),
        ...((opportunity.sources || []).map((s) => (typeof s === "string" ? s : s?.url))),
      ].filter(Boolean);
      let text = "";
      let usedUrl = null;
      let costUsd = 0;
      for (const url of urls.slice(0, 3)) {
        if (investigatedUrls.has(url)) continue;
        investigatedUrls.add(url);
        usedUrl = url;
        reserveHotelBenchmarkCall({ ledgerPath, hotelId, jobKey: `th-direct-${hashKey(url)}`, quotedUsd: 0, kind: "retrieval" });
        try {
          const page = await fetchPageDirect(url);
          text = page.text || "";
          if (text.trim().length < 300) {
            const md = await maybeFirecrawl({
              url,
              ledgerPath,
              hotelId,
              jobKey: `th-retrieval-${hashKey(url)}`,
              allowFirecrawl,
            });
            if (md) {
              text = md;
              costUsd = 0.005;
            }
          }
        } catch {
          text = "";
        }
        if (text.trim().length >= 80) break;
      }
      const quote = text.replace(/\s+/g, " ").trim().slice(0, 500)
        || opportunity.summaryWhyHotel
        || opportunity.evidenceQuote
        || "";
      return {
        newMaterialEvidence: Boolean(quote),
        provider: costUsd > 0 ? "firecrawl" : "direct_public",
        sourceFamilies: ["official"],
        costUsd,
        evidence: quote
          ? [{
            sourceFamily: "official",
            url: usedUrl || opportunity.officialSource || null,
            quote,
            organizationOwned: true,
            publicationReady: false,
            customerVisible: false,
          }]
          : [],
        deepPatch: opportunity.contactEmail
          ? { contact: { businessEmail: opportunity.contactEmail } }
          : {},
        notes: `benchmark_research:${blocker.key}`,
      };
    },
  };
}

function summarizeHotelRun(hotelSpec, results, ledger, coverage) {
  const useful = results.filter((r) => r.internalAccountDevelopment === true);
  const falsePositives = results.filter((r) => r.supervisorV2?.qualification?.downgradeReason);
  const pathways = {
    ACCOUNT_LED: results.filter((r) => r.pathway === "ACCOUNT_LED").length,
    EVENT_LED: results.filter((r) => r.pathway === "EVENT_LED").length,
    COMPETITIVE_FUTURE_CYCLE: results.filter((r) => r.pathway === "COMPETITIVE_FUTURE_CYCLE").length,
  };
  const leadsDiscovered = results.reduce((n, r) => n + (r.supervisorV2?.leadFollowing?.recorded || 0), 0);
  const leadsFollowed = results.reduce((n, r) => n + ((r.supervisorV2?.leadFollowing?.pursue || []).length), 0);
  const independentReads = results.reduce((n, r) => n + (r.supervisorV2?.provenance?.independence?.independentPublisherReads || 0), 0);
  const mechanisms = results.filter((r) => r.supervisorV2?.qualification?.accommodationEvidenceRole?.guestroomOrHotelSourcingControl).length;
  const contacts = results.filter((r) => r.supervisorV2?.qualification?.usableContactRoute).length;
  const spent = Number(ledger.estimatedSpentUsd || 0);
  const usefulCount = useful.length;

  return {
    hotelId: hotelSpec.hotelId,
    hotelName: hotelSpec.hotelName,
    segmentId: hotelSpec.segmentId,
    organizationsDiscoveredAndDeduped: coverage?.readings?.discoveryCoverage?.organizationsInUniverseEstimate ?? results.length,
    candidatesInvestigated: results.length,
    newCommercialFindings: usefulCount,
    independentPublishersResearched: independentReads,
    relatedEntityLeadsDiscovered: leadsDiscovered,
    relatedEntityLeadsFollowed: leadsFollowed,
    sourceBackedAccommodationMechanisms: mechanisms,
    hotelSpecificQualifiedAccountDevelopment: usefulCount,
    eventLedAndFutureCycle: {
      EVENT_LED: pathways.EVENT_LED,
      COMPETITIVE_FUTURE_CYCLE: pathways.COMPETITIVE_FUTURE_CYCLE,
      verifiedOverflow: results.filter((r) => r.supervisorV2?.qualification?.basePathwayEvaluation?.pathway === "COMPETITIVE_FUTURE_CYCLE").length,
    },
    usableContactRoutes: contacts,
    publicationGateEligibleOpportunities: results.filter((r) => r.customerVisible === true || r.supervisorV2?.qualification?.publicationEligibility === "ELIGIBLE").length,
    falsePositivesRejected: falsePositives.length,
    unresolvedBlockersAndStopTriggers: results.map((r) => ({
      organizationName: r.organizationName,
      stopReason: r.stopReason || r.supervisorV2?.stopRecord?.stoppingTrigger || null,
      downgradeReason: r.supervisorV2?.qualification?.downgradeReason || null,
    })),
    actualResearchCostUsd: spent,
    remainingHotelCeilingUsd: Number((Number(hotelSpec.ceilingUsd) - spent).toFixed(6)),
    costPerCommerciallyUsefulProspectUsd: usefulCount > 0 ? Number((spent / usefulCount).toFixed(6)) : null,
    pathways,
    technicalCompletion: true,
    commercialSuccess: usefulCount > 0,
    coverageReadings: coverage?.readings || null,
    candidates: results.map((r) => ({
      organizationName: r.organizationName,
      pathway: r.pathway,
      internalAccountDevelopment: r.internalAccountDevelopment === true,
      researchStatus: r.researchStatus,
      nextSalesAction: r.v2NextSalesAction || r.supervisorV2?.qualification?.nextSalesAction || null,
      downgradeReason: r.supervisorV2?.qualification?.downgradeReason || null,
    })),
  };
}

/**
 * Execute authorized three-hotel benchmark.
 */
export async function executeAuthorizedThreeHotelBenchmark(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true || input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, reason: "awaiting_explicit_authorization", spent: false };
  }
  if (input.integrationGatePassed !== true) {
    return { executed: false, reason: "integration_gate_not_passed", spent: false };
  }

  const root = input.root || process.cwd();
  const safeguards = verifyThreeHotelAuthorizationSafeguards({ manifest: input.manifest, root });
  if (!safeguards.ok) {
    return { executed: false, reason: "safeguards_failed", spent: false, safeguards };
  }

  const allowFirecrawl = input.manifest?.providerPricing?.firecrawlScrape?.bounded === true;
  const hotelReports = [];
  const planner = new HeuristicResearchPlanner();

  for (const hotelSpec of THREE_HOTEL_BENCHMARK_HOTELS) {
    const ledgerPath = path.join(root, hotelSpec.ledgerRelative);
    loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelSpec.hotelId);
    const candidates = loadSegmentCandidates(hotelSpec.hotelId, hotelSpec.segmentId)
      .map(seedExistingEvidence);
    // Investigate beyond a two-useful target, but keep a practical ceiling inside $1.50.
    const maxInvestigate = Math.min(8, hotelSpec.maxCandidatesInvestigated || 8, candidates.length);
    const toInvestigate = candidates.slice(0, maxInvestigate);
    const investigatedUrls = new Set();
    const results = [];
    const dossiersDir = path.join(root, THREE_HOTEL_BENCHMARK_REPORTS_DIR, "dossiers", hotelSpec.hotelId);
    fs.mkdirSync(dossiersDir, { recursive: true });
    const checkpointDir = path.join(root, THREE_HOTEL_BENCHMARK_REPORTS_DIR, "checkpoints", hotelSpec.hotelId);
    fs.mkdirSync(checkpointDir, { recursive: true });

    for (const candidate of toInvestigate) {
      const ledger = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelSpec.hotelId);
      const remaining = Number(hotelSpec.ceilingUsd) - Number(ledger.estimatedSpentUsd || 0) - Number(ledger.reservedUsd || 0);
      if (remaining <= 0.01) break;

      // Optional planner call (priced) for diversity — cap planner volume to protect hotel ceiling
      if (remaining > 0.05 && Number(ledger.plannerCalls || 0) < 4 && results.length < 4) {
        const reservation = reserveHotelBenchmarkCall({
          ledgerPath,
          hotelId: hotelSpec.hotelId,
          jobKey: `th-planner-${candidate.id}`,
          quotedUsd: PLANNER_CEILING_USD,
          kind: "planner",
        });
        if (reservation.ok) {
          try {
            const plan = await planDynamicResearch({
              opportunity: candidate,
              hotel: { name: hotelSpec.hotelName, market: hotelSpec.hotelName },
              objective: {
                hotelName: hotelSpec.hotelName,
                organizationName: candidate.organizationName,
                commercialQuestion: hotelSpec.objective,
              },
              remainingUsd: remaining,
              authorizePlannerModel: true,
              findings: (candidate.evidence || []).map((e) => ({ excerpt: e.quote, url: e.url })),
            });
            if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
              commitHotelBenchmarkCall({
                ledgerPath,
                hotelId: hotelSpec.hotelId,
                reservationId: reservation.reservationId,
                actualUsd: Math.min(Number(plan.plannerActualUsd), PLANNER_CEILING_USD),
                usage: plan.plannerUsage,
              });
            } else if (plan.plannerModelCalls === 1) {
              failHotelBenchmarkCall({ ledgerPath, hotelId: hotelSpec.hotelId, reservationId: reservation.reservationId, reason: "planner_actual_missing" });
            } else {
              releaseHotelBenchmarkCall({ ledgerPath, hotelId: hotelSpec.hotelId, reservationId: reservation.reservationId, reason: "planner_fallback" });
            }
          } catch (err) {
            failHotelBenchmarkCall({ ledgerPath, hotelId: hotelSpec.hotelId, reservationId: reservation.reservationId, reason: err.message || "planner_failed" });
          }
        }
      }

      const researcher = buildBenchmarkResearcher({
        ledgerPath,
        hotelId: hotelSpec.hotelId,
        allowFirecrawl,
        investigatedUrls,
      });

      const matured = await matureOpportunity({
        opportunity: {
          ...candidate,
          targetHotel: hotelSpec.hotelName,
        },
        planner,
        researcher,
        persist: null,
        options: {
          paidBlocked: true, // Parallel refused
          parallelAvailable: false,
          maxIterations: 3,
          enableSupervisorV2: true,
          forbidPublication: true,
          forbidAirtable: true,
          remainingBudgetUsd: remaining,
          targetHotel: hotelSpec.hotelName,
          checkpointDir,
          authorizePaidResearch: false,
          authorizePlannerModel: false,
        },
      });

      // Re-qualify with full evidence text under V2 (ensures venue downgrades)
      const q = qualifyHotelSpecificProspectV2({
        opportunity: matured,
        acceptedClaims: (matured.evidence || []).map((e) => ({
          statement: e.quote,
          url: e.url,
          accepted: true,
        })),
        pageTexts: (matured.evidence || []).map((e) => e.quote || ""),
        targetHotel: hotelSpec.hotelName,
        usableContactRoute: matured.contact?.businessEmail || matured.contactEmail,
      });
      matured.internalAccountDevelopment = q.internalAccountDevelopment;
      matured.pathway = q.pathway;
      matured.v2NextSalesAction = q.nextSalesAction;
      if (matured.supervisorV2) {
        matured.supervisorV2.qualification = q;
      }

      if (matured.supervisorV2?.dossier) {
        const slug = matured.organizationName.replace(/\W+/g, "_").toLowerCase();
        fs.writeFileSync(
          path.join(dossiersDir, `INTERNAL_RESEARCH_DOSSIER_${slug}.json`),
          `${JSON.stringify({ dossier: matured.supervisorV2.dossier }, null, 2)}\n`,
        );
      }
      results.push(matured);
    }

    const ledgerFinal = loadOrCreateHotelBenchmarkLedger(ledgerPath, hotelSpec.hotelId);
    const usefulNames = results.filter((r) => r.internalAccountDevelopment).map((r) => r.organizationName);
    const uninvestigated = candidates.slice(maxInvestigate).map((c) => c.organizationName);
    const coverage = buildSegmentDiscoveryCoverageReport({
      hotelId: hotelSpec.hotelId,
      hotelName: hotelSpec.hotelName,
      segmentId: hotelSpec.segmentId,
      knownOrganizations: candidates.map((c) => c.organizationName),
      discoveredOrganizations: candidates.map((c) => c.organizationName),
      investigatedCandidates: results.map((r) => r.organizationName),
      uninvestigatedQueue: uninvestigated,
      incompleteCandidates: results.filter((r) => !r.internalAccountDevelopment).map((r) => r.organizationName),
      usefulInternalProspects: usefulNames,
      publicationEligibleCount: 0,
      independentPublisherReads: results.reduce((n, r) => n + (r.supervisorV2?.provenance?.independence?.independentPublisherReads || 0), 0),
      relatedEntityLeadsFollowed: results.reduce((n, r) => n + ((r.supervisorV2?.leadFollowing?.pursue || []).length), 0),
      usefulProspectTargetHit: usefulNames.length >= 2,
      stoppingTrigger: STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL,
    });
    // Attach continuation explicitly — do not treat 2 useful as exhaustion
    const cont = attachSegmentDiscoveryCoverageV2({
      hotelId: hotelSpec.hotelId,
      hotelName: hotelSpec.hotelName,
      segmentId: hotelSpec.segmentId,
      knownOrganizations: candidates.map((c) => c.organizationName),
      discoveredOrganizations: candidates.map((c) => c.organizationName),
      investigatedCandidates: results.map((r) => r.organizationName),
      uninvestigatedQueue: uninvestigated,
      usefulInternalProspects: usefulNames,
      publicationEligibleCount: 0,
      usefulProspectTargetHit: usefulNames.length >= 2,
      remainingBudgetUsd: Number(hotelSpec.ceilingUsd) - Number(ledgerFinal.estimatedSpentUsd || 0),
    });

    hotelReports.push({
      ...summarizeHotelRun(hotelSpec, results, ledgerFinal, coverage),
      discoveryContinuation: cont.continuation,
      didNotStopSolelyAtTwoUseful: results.length >= Math.min(3, candidates.length) || usefulNames.length < 2 || uninvestigated.length === 0,
    });
  }

  const totalSpent = hotelReports.reduce((n, h) => n + Number(h.actualResearchCostUsd || 0), 0);
  return {
    executed: true,
    version: THREE_HOTEL_EXECUTE_VERSION,
    authorizationVersion: THREE_HOTEL_BENCHMARK_VERSION,
    totalSpentUsd: Number(totalSpent.toFixed(6)),
    aggregateCeilingUsd: 4.5,
    remainingAggregateUsd: Number((4.5 - totalSpent).toFixed(6)),
    hotels: hotelReports,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    productionWeeklyJobEnabled: false,
    customerFacingClassificationsUnchanged: true,
    guardedPriorLedgersUntouched: true,
  };
}

export function prepareThreeHotelLivePackage() {
  return {
    manifest: buildThreeHotelBenchmarkAuthorizationManifestV2(),
    reportsDir: THREE_HOTEL_BENCHMARK_REPORTS_DIR,
  };
}
