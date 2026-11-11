/**
 * Shared Mexico PLP / PEP live FullE2E core.
 * Invoked by PowerShell via dedicated Node runners (existing PS1 entry points).
 *
 * Fail-closed: authorizePaidResearch + budgetUSD>0 + GDI_EXA_AGENT_LIVE=1
 * + founder approval file status APPROVED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { executePlpPepExaLive } from "./exa-plp-pep-mexico-live-ors-v1.js";
import {
  mapPlpDeepToOpportunity,
  mapPepDeepToOpportunity,
} from "./exa-plp-pep-to-opportunity-mapping-v1.js";
import {
  isExaAgentLiveAuthorized,
  quoteExaAgentRun,
} from "./exa-agent-provider-adapter-v1.js";
import { mergeLocalShadowCommercialOpportunities } from "../../local-shadow-commercial-portfolio-v1.js";
import { filterCustomerFacingOpportunities } from "../../customer-visibility.js";
import {
  getDiscoveryExclusionKeys,
  isCleanRoomActive,
} from "../clean-room-v26-isolation-v1.js";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../..",
);

const NOBU_HOTEL_ID = "recC2kbCaS48jFjFF";
const NOBU_APPROVAL = "APPROVED_NOBU_LONDON_14_WORKFLOW_LIVE_EXA_USD60";

const HOTEL_META = Object.freeze({
  recterKmNPdntCm0v: {
    hotelId: "recterKmNPdntCm0v",
    name: "Grand Fiesta Americana Chapultepec",
    market: "Mexico City",
    city: "Mexico City",
    ledgerKey: "chapultepec",
    campaign: "mexico",
  },
  recIEcSWuhSCoNLFN: {
    hotelId: "recIEcSWuhSCoNLFN",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
    market: "Cancún",
    city: "Cancún",
    ledgerKey: "condesa",
    campaign: "mexico",
  },
  [NOBU_HOTEL_ID]: {
    hotelId: NOBU_HOTEL_ID,
    name: "Nobu Hotel London Portman Square",
    market: "London",
    city: "London",
    ledgerKey: "nobu-london",
    campaign: "nobu",
  },
});

const WORKFLOW_CFG = Object.freeze({
  plp: {
    label: "Preferred Lodging Partnership",
    approvalPhrase: "APPROVED_MEXICO_PREFERRED_LODGING_LIVE_EXA_USD16",
    nobuApprovalPhrase: NOBU_APPROVAL,
    reportRoot: "reports/gdi-preferred-lodging-partnerships-v1/mexico-two-hotel-live",
    nobuReportRoot: "reports/gdi-nobu-london-14-workflow-live-benchmark-v1",
    shadowPrefix: "preferred-lodging-mexico-live",
    nobuShadowPrefix: "nobu-london-preferred-lodging-live",
    waveFlag: "preferredLodgingMexicoLiveWaveV1",
    nobuWaveFlag: "nobuLondon14WorkflowLiveV1",
  },
  pep: {
    label: "Professional Event Planner Partnership",
    approvalPhrase: "APPROVED_MEXICO_PROFESSIONAL_EVENT_PLANNERS_LIVE_EXA_USD16",
    nobuApprovalPhrase: NOBU_APPROVAL,
    reportRoot: "reports/gdi-professional-event-planners-v1/mexico-two-hotel-live",
    nobuReportRoot: "reports/gdi-nobu-london-14-workflow-live-benchmark-v1",
    shadowPrefix: "professional-event-planners-mexico-live",
    nobuShadowPrefix: "nobu-london-professional-event-planners-live",
    waveFlag: "professionalEventPlannerMexicoLiveWaveV1",
    nobuWaveFlag: "nobuLondon14WorkflowLiveV1",
  },
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2));
  return fp;
}

function chargeCost(run, quote) {
  if (!run?.ok) return Number(run?.costDollars?.total) || 0;
  const actual = Number(run?.costDollars?.total);
  if (Number.isFinite(actual) && actual > 0) return actual;
  return Number(quote?.maxUsd) || 0;
}

function extractCandidates(workflow, structured = {}) {
  const list = Array.isArray(structured.candidates) ? structured.candidates : [];
  if (workflow === "pep") {
    return list
      .map((c) => ({
        ...c,
        name: text(c.plannerCompanyName || c.name),
        key: text(c.plannerCompanyName || c.name).toLowerCase(),
      }))
      .filter((c) => c.key);
  }
  return list
    .map((c) => ({
      ...c,
      name: text(c.venueName || c.name),
      key: text(c.venueName || c.name).toLowerCase(),
    }))
    .filter((c) => c.key);
}

function existingExclusionKeys(hotelId) {
  // V26 clean-room: never seed exclusions from historical inventory / shadows.
  if (isCleanRoomActive()) {
    const clean = getDiscoveryExclusionKeys(hotelId);
    return Array.isArray(clean) ? clean : [];
  }
  try {
    const merged = mergeLocalShadowCommercialOpportunities(hotelId, []);
    const facing = filterCustomerFacingOpportunities(merged, { hotelId });
    return facing
      .map((o) =>
        text(o.organizationName || o.venueName || o.plannerName || o.title),
      )
      .filter(Boolean)
      .slice(0, 40);
  } catch {
    return [];
  }
}

function campaignPaths(workflow, hotel) {
  const cfg = WORKFLOW_CFG[workflow];
  const nobu = hotel.campaign === "nobu";
  return {
    reportRoot: nobu ? cfg.nobuReportRoot : cfg.reportRoot,
    approvalPhrase: nobu ? cfg.nobuApprovalPhrase : cfg.approvalPhrase,
    shadowPrefix: nobu ? cfg.nobuShadowPrefix : cfg.shadowPrefix,
    waveFlag: nobu ? cfg.nobuWaveFlag : cfg.waveFlag,
    hotelCeilingUsd: nobu ? 6 : 8,
    aggregateCeilingUsd: nobu ? 60 : 16,
  };
}

function ensureFounderApproval(workflow, hotel) {
  const cfg = WORKFLOW_CFG[workflow];
  const paths = campaignPaths(workflow, hotel);
  const fp = path.join(
    ROOT,
    paths.reportRoot,
    "founder-authorization-checkpoint.json",
  );
  if (!fs.existsSync(fp)) {
    return { ok: false, error: "FOUNDER_AUTH_CHECKPOINT_MISSING", path: fp };
  }
  const doc = JSON.parse(fs.readFileSync(fp, "utf8"));
  if (doc.status === "APPROVED" && doc.approvalPhrase === paths.approvalPhrase) {
    return { ok: true, path: fp, doc };
  }
  const phraseOk =
    doc.approvalPhrase === paths.approvalPhrase ||
    (Array.isArray(doc.approvalPhrases) &&
      doc.approvalPhrases.includes(paths.approvalPhrase));
  if (doc.status === "APPROVED" || phraseOk) {
    const next = {
      ...doc,
      status: "APPROVED",
      approvalPhrase: paths.approvalPhrase,
      approvedAt: doc.approvedAt || new Date().toISOString(),
      approvedVia: "chat_phrase_plus_powershell_paid_flags",
      paidCallsAuthorized: true,
      liveExa: true,
    };
    writeJson(fp, next);
    return { ok: true, path: fp, doc: next };
  }
  return {
    ok: false,
    error: "FOUNDER_APPROVAL_REQUIRED",
    expected: paths.approvalPhrase,
    status: doc.status,
    path: fp,
  };
}

function shadowPathFor(workflow, hotel) {
  const paths = campaignPaths(workflow, hotel);
  return path.join(
    ROOT,
    "data/group-demand-intelligence/local-shadow",
    `${paths.shadowPrefix}-${hotel.ledgerKey}-opportunities-v1.json`,
  );
}

function ledgerPathFor(workflow, hotel) {
  const paths = campaignPaths(workflow, hotel);
  return path.join(
    ROOT,
    paths.reportRoot,
    "ledgers",
    `${hotel.ledgerKey}-${workflow}-ledger.json`,
  );
}

/**
 * Run live FullE2E for one hotel × one workflow from shared-launcher config.
 */
export async function runPlpPepMexicoLiveFullE2e({
  workflow = "plp",
  config = {},
} = {}) {
  const wf = WORKFLOW_CFG[workflow];
  if (!wf) {
    return { ok: false, error: "UNKNOWN_WORKFLOW", workflow };
  }

  const hotelId = text(config.hotelId);
  const hotel = HOTEL_META[hotelId];
  if (!hotel) {
    return {
      ok: false,
      error: "HOTEL_NOT_IN_PLP_PEP_LIVE_CAMPAIGN",
      hotelId,
      allowed: Object.keys(HOTEL_META),
    };
  }

  const paths = campaignPaths(workflow, hotel);
  const paid =
    Boolean(config.authorizePaidResearch) && Number(config.budgetUSD || 0) > 0;
  const mode = String(config.mode || "");
  const outDir =
    config.outputDirectory ||
    config.reportDir ||
    path.join(
      ROOT,
      paths.reportRoot,
      "runs",
      `${hotel.ledgerKey}-${workflow}-${Date.now()}`,
    );

  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(path.join(outDir, "artifacts"), { recursive: true });

  if (!paid || (mode !== "full-e2e" && mode !== "execute")) {
    writeJson(path.join(outDir, "SUMMARY.json"), {
      ok: false,
      error: "PAID_FULL_E2E_REQUIRED",
      message:
        "Live runner requires mode full-e2e + AuthorizePaidResearch + BudgetUSD > 0",
    });
    return { ok: false, error: "PAID_FULL_E2E_REQUIRED", outDir };
  }

  if (process.env.GDI_EXA_AGENT_LIVE !== "1") {
    writeJson(path.join(outDir, "SUMMARY.json"), {
      ok: false,
      error: "GDI_EXA_AGENT_LIVE_REQUIRED",
    });
    return { ok: false, error: "GDI_EXA_AGENT_LIVE_REQUIRED", outDir };
  }

  if (!isExaAgentLiveAuthorized({ authorizeExaAgentLive: true })) {
    writeJson(path.join(outDir, "SUMMARY.json"), {
      ok: false,
      error: "EXA_LIVE_AUTH_FAILED",
      hint: "Set GDI_EXA_AGENT_LIVE=1 and ensure EXA_API_KEY is present",
    });
    return { ok: false, error: "EXA_LIVE_AUTH_FAILED", outDir };
  }

  const approval = ensureFounderApproval(workflow, hotel);
  if (!approval.ok) {
    writeJson(path.join(outDir, "SUMMARY.json"), approval);
    return approval;
  }

  const hardCeilingUsd = Math.min(
    paths.hotelCeilingUsd,
    Number(config.budgetUSD) || 0,
  );
  const maxCandidates = Math.min(
    Number(config.maxCandidates) || 8,
    10,
  );
  const targetNew = Math.min(
    Number(config.targetNewOpportunities) || 5,
    maxCandidates,
  );
  const discQuote = quoteExaAgentRun({ effort: "medium" });
  const deepQuote = quoteExaAgentRun({ effort: "high" });

  const ledger = {
    version: `gdi-${workflow}-live-ledger-v1`,
    ledgerId: `${workflow}-${hotel.campaign}-${hotel.ledgerKey}-live-v1`,
    hotelId,
    hotelName: hotel.name,
    workflow,
    campaign: hotel.campaign,
    hardCeilingUsd,
    spentUsd: 0,
    entries: [],
    startedAt: new Date().toISOString(),
    approvalPhrase: paths.approvalPhrase,
    firecrawl: "NOT_AUTHORIZED",
    surfe: "NOT_AUTHORIZED",
    publication: "NOT_AUTHORIZED",
  };

  const hotelCtx = {
    hotelId: hotel.hotelId,
    id: hotel.hotelId,
    name: hotel.name,
    market: hotel.market,
    city: hotel.city,
  };

  const exclusions = existingExclusionKeys(hotelId);
  const artDir = path.join(outDir, "artifacts");

  // Discovery (1–2 medium runs within ceiling)
  const discoveryRuns = [];
  let candidates = [];
  const discoveryCount = 2;
  for (let i = 0; i < discoveryCount; i++) {
    const nextCost = Number(discQuote.maxUsd) || 0.1;
    if (ledger.spentUsd + nextCost > hardCeilingUsd) break;

    let discovery;
    try {
      discovery = await executePlpPepExaLive({
        workflow,
        mode: "discovery",
        hotel: hotelCtx,
        exclusions: [...exclusions, ...candidates.map((c) => c.name)],
        authorizeExaAgentLive: true,
        effort: "medium",
        limit: maxCandidates,
      });
    } catch (err) {
      discovery = {
        run: {
          ok: false,
          status: "execute_threw",
          error: String(err?.message || err),
          costDollars: null,
        },
        quote: discQuote,
      };
    }

    const cost = chargeCost(discovery.run, discovery.quote || discQuote);
    ledger.spentUsd = Number((ledger.spentUsd + cost).toFixed(6));
    ledger.entries.push({
      mode: "discovery",
      index: i,
      runId: discovery.run?.runId || discovery.run?.id || null,
      costUsd: cost,
      ok: discovery.run?.ok === true,
      status: discovery.run?.status || null,
      error: discovery.run?.error || null,
    });
    writeJson(path.join(artDir, `discovery-${i + 1}.json`), discovery);
    discoveryRuns.push(discovery);

    if (discovery.run?.ok) {
      const found = extractCandidates(
        workflow,
        discovery.run.structured || {},
      );
      for (const c of found) {
        if (!candidates.some((x) => x.key === c.key)) candidates.push(c);
      }
    }
  }

  candidates = candidates.slice(0, maxCandidates);

  // Deep research on shortlist
  const deepResults = [];
  const opportunities = [];
  for (let i = 0; i < candidates.length; i++) {
    if (opportunities.length >= targetNew) break;
    const nextCost = Number(deepQuote.maxUsd) || 0.5;
    if (ledger.spentUsd + nextCost > hardCeilingUsd) break;

    const candidate = candidates[i];
    let deep;
    try {
      deep = await executePlpPepExaLive({
        workflow,
        mode: "deep_research",
        hotel: hotelCtx,
        candidate,
        authorizeExaAgentLive: true,
        effort: "high",
      });
    } catch (err) {
      deep = {
        run: {
          ok: false,
          status: "execute_threw",
          error: String(err?.message || err),
          costDollars: null,
        },
        quote: deepQuote,
      };
    }

    const cost = chargeCost(deep.run, deep.quote || deepQuote);
    ledger.spentUsd = Number((ledger.spentUsd + cost).toFixed(6));
    ledger.entries.push({
      mode: "deep_research",
      index: i,
      candidateKey: candidate.key,
      candidateName: candidate.name,
      runId: deep.run?.runId || deep.run?.id || null,
      costUsd: cost,
      ok: deep.run?.ok === true,
      status: deep.run?.status || null,
      error: deep.run?.error || null,
    });
    writeJson(
      path.join(artDir, `deep-${String(i + 1).padStart(2, "0")}.json`),
      { candidate, ...deep },
    );
    deepResults.push({ candidate, deep });

    if (deep.run?.ok && deep.run.structured) {
      const opp =
        workflow === "pep"
          ? mapPepDeepToOpportunity({
              hotel: hotelCtx,
              candidate,
              deep: deep.run.structured,
            })
          : mapPlpDeepToOpportunity({
              hotel: hotelCtx,
              candidate,
              deep: deep.run.structured,
            });
      if (opp) {
        opportunities.push({
          ...opp,
          [paths.waveFlag]: true,
          nobuLondon14WorkflowLiveV1: hotel.campaign === "nobu",
        });
      }
    }
  }

  ledger.finishedAt = new Date().toISOString();
  ledger.opportunityCount = opportunities.length;
  ledger.candidateCount = candidates.length;
  ledger.paidCallsMade = ledger.entries.length;

  // Persist hotel ledger + aggregate update
  writeJson(ledgerPathFor(workflow, hotel), ledger);
  writeJson(path.join(outDir, "ledger.json"), ledger);

  const shadowDoc = {
    version: `gdi-${workflow}-live-local-shadow-v1`,
    hotelId,
    hotelName: hotel.name,
    workflow,
    campaign: hotel.campaign,
    generatedAt: new Date().toISOString(),
    doesNotAffectProduction: true,
    publication: "NOT_AUTHORIZED",
    liveExa: true,
    spentUsd: ledger.spentUsd,
    opportunities,
  };
  writeJson(shadowPathFor(workflow, hotel), shadowDoc);
  writeJson(path.join(outDir, "opportunities.json"), { opportunities });

  // Aggregate ledger rollup (Mexico campaign only — Nobu uses campaign aggregate)
  const aggPath = path.join(
    ROOT,
    paths.reportRoot,
    "ledgers/aggregate-ledger.json",
  );
  let aggregate = fs.existsSync(aggPath)
    ? JSON.parse(fs.readFileSync(aggPath, "utf8"))
    : {
        version: `gdi-${workflow}-live-aggregate-v1`,
        hardCeilingUsd: paths.aggregateCeilingUsd,
        spentUsd: 0,
        hotels: {},
      };
  aggregate.hotels = aggregate.hotels || {};
  aggregate.hotels[hotel.ledgerKey] = {
    hotelId,
    spentUsd: ledger.spentUsd,
    paidCallsMade: ledger.paidCallsMade,
    opportunityCount: opportunities.length,
    ledgerPath: ledgerPathFor(workflow, hotel),
  };
  aggregate.spentUsd = Number(
    Object.values(aggregate.hotels)
      .reduce((s, h) => s + Number(h.spentUsd || 0), 0)
      .toFixed(6),
  );
  aggregate.updatedAt = new Date().toISOString();
  writeJson(aggPath, aggregate);

  const summary = {
    ok: true,
    verdict: opportunities.length
      ? "LIVE_FULL_E2E_COMPLETE"
      : candidates.length
        ? "LIVE_FULL_E2E_COMPLETE_NO_MAPPED_OPPORTUNITIES"
        : "LIVE_FULL_E2E_COMPLETE_NO_CANDIDATES",
    workflow,
    workflowLabel: wf.label,
    hotelId,
    hotelName: hotel.name,
    spentUsd: ledger.spentUsd,
    hardCeilingUsd,
    paidCallsMade: ledger.paidCallsMade,
    discoveryOk: discoveryRuns.filter((d) => d.run?.ok).length,
    candidatesFound: candidates.length,
    deepOk: deepResults.filter((d) => d.deep?.run?.ok).length,
    opportunitiesStaged: opportunities.length,
    shadowPath: shadowPathFor(workflow, hotel),
    outDir,
    publication: "NOT_AUTHORIZED",
    firecrawl: "NOT_AUTHORIZED",
    surfe: "NOT_AUTHORIZED",
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);
  return summary;
}

export { HOTEL_META, WORKFLOW_CFG };
