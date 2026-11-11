/**
 * Shared live FullE2E core for demand segments 3–12.
 * Invoked by PowerShell via gdi-demand-segment-live-fulle2e-v1.mjs.
 * Dispatch only through exa-agent-provider-adapter-v1 (ORS payloads).
 *
 * Fail-closed: authorizePaidResearch + budgetUSD>0 + GDI_EXA_AGENT_LIVE=1
 * + founder approval for Nobu campaign when hotel is Nobu.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  executeSegment4912ExaAgentOrsStrategy,
  getLiveSegmentContract,
} from "./exa-agent-segments-4-9-12-ors-strategy-v1.js";
import { mapSegment4912AgentResultToOpportunity } from "./exa-agent-segments-4-9-12-to-cib-mapping-v1.js";
import {
  isExaAgentLiveAuthorized,
  quoteExaAgentRun,
} from "./exa-agent-provider-adapter-v1.js";
import { resolveDemandSegment } from "../../demand-segments/eight-demand-segment-registry-v1.js";
import { mergeLocalShadowCommercialOpportunities } from "../../local-shadow-commercial-portfolio-v1.js";
import { filterCustomerFacingOpportunities } from "../../customer-visibility.js";
import { loadOpportunities } from "../../repository.js";
import {
  getDiscoveryExclusionKeys,
  isCleanRoomActive,
} from "../clean-room-v26-isolation-v1.js";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../..",
);

export const EXA_DEMAND_SEGMENT_LIVE_FULLE2E_CORE_V1 =
  "gdi-exa-demand-segment-live-fulle2e-core-v1";

const NOBU_HOTEL_ID = "recC2kbCaS48jFjFF";
const NOBU_APPROVAL = "APPROVED_NOBU_LONDON_14_WORKFLOW_LIVE_EXA_USD60";
const NOBU_REPORT =
  "reports/gdi-nobu-london-14-workflow-live-benchmark-v1";

const HOTEL_META = Object.freeze({
  [NOBU_HOTEL_ID]: {
    hotelId: NOBU_HOTEL_ID,
    name: "Nobu Hotel London Portman Square",
    market: "London",
    city: "London",
    ledgerKey: "nobu-london",
  },
  recterKmNPdntCm0v: {
    hotelId: "recterKmNPdntCm0v",
    name: "Grand Fiesta Americana Chapultepec",
    market: "Mexico City",
    city: "Mexico City",
    ledgerKey: "chapultepec",
  },
  recIEcSWuhSCoNLFN: {
    hotelId: "recIEcSWuhSCoNLFN",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
    market: "Cancún",
    city: "Cancún",
    ledgerKey: "condesa",
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

function extractCandidates(structured = {}) {
  const list = Array.isArray(structured.candidates) ? structured.candidates : [];
  return list
    .map((c) => ({
      ...c,
      organizationName: text(c.organizationName || c.companyName),
      programmeName: text(c.programmeName || c.title),
      key: text(c.organizationName || c.companyName).toLowerCase(),
    }))
    .filter((c) => c.key);
}

function existingExclusionKeys(hotelId) {
  // V26 clean-room: never seed exclusions from historical inventory / shadows.
  if (isCleanRoomActive()) {
    const clean = getDiscoveryExclusionKeys(hotelId);
    return Array.isArray(clean) ? clean : [];
  }
  const keys = [];
  try {
    const store = loadOpportunities(hotelId);
    for (const o of store.opportunities || []) {
      const k = text(
        o.organizationName || o.companyName || o.venueName || o.title,
      );
      if (k) keys.push(k);
    }
  } catch {
    /* store optional */
  }
  try {
    const merged = mergeLocalShadowCommercialOpportunities(hotelId, []);
    const facing = filterCustomerFacingOpportunities(merged, { hotelId });
    for (const o of facing) {
      const k = text(
        o.organizationName || o.companyName || o.venueName || o.title,
      );
      if (k) keys.push(k);
    }
  } catch {
    /* shadow optional */
  }
  return [...new Set(keys)].slice(0, 60);
}

function ensureNobuFounderApproval() {
  const fp = path.join(ROOT, NOBU_REPORT, "founder-authorization-checkpoint.json");
  if (!fs.existsSync(fp)) {
    return { ok: false, error: "FOUNDER_AUTH_CHECKPOINT_MISSING", path: fp };
  }
  const doc = JSON.parse(fs.readFileSync(fp, "utf8"));
  if (doc.status === "APPROVED" && doc.approvalPhrase === NOBU_APPROVAL) {
    return { ok: true, path: fp, doc };
  }
  if (
    doc.status === "APPROVED" ||
    (Array.isArray(doc.approvalPhrases) &&
      doc.approvalPhrases.includes(NOBU_APPROVAL))
  ) {
    const next = {
      ...doc,
      status: "APPROVED",
      approvalPhrase: NOBU_APPROVAL,
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
    expected: NOBU_APPROVAL,
    status: doc.status,
  };
}

function resolveHotel(config = {}) {
  const hotelId = text(config.hotelId);
  if (HOTEL_META[hotelId]) return HOTEL_META[hotelId];
  const name = text(config.hotelName) || hotelId;
  return {
    hotelId,
    name,
    market: text(config.market) || null,
    city: text(config.city) || null,
    ledgerKey: hotelId.slice(0, 12).toLowerCase(),
  };
}

function shadowPathFor(hotel, segmentId) {
  if (hotel.hotelId === NOBU_HOTEL_ID) {
    return path.join(
      ROOT,
      "data/group-demand-intelligence/local-shadow",
      `nobu-london-14-workflow-live-${segmentId}-opportunities-v1.json`,
    );
  }
  return path.join(
    ROOT,
    "data/group-demand-intelligence/local-shadow",
    `segment-live-${hotel.ledgerKey}-${segmentId}-opportunities-v1.json`,
  );
}

function workflowLedgerPath(hotel, segmentId) {
  if (hotel.hotelId === NOBU_HOTEL_ID) {
    return path.join(
      ROOT,
      NOBU_REPORT,
      "ledgers",
      `workflow-live-${segmentId}-ledger.json`,
    );
  }
  return null;
}

/**
 * Zero-paid readiness probe — never sets authorizeExaAgentLive.
 */
export async function probeDemandSegmentLiveBoundary({
  segmentId,
  hotel = {},
} = {}) {
  const contract = getLiveSegmentContract(segmentId);
  if (!contract) {
    return { ok: false, error: "UNKNOWN_LIVE_SEGMENT", segmentId };
  }
  const mock = {
    version: contract.discoveryContractId,
    segmentId,
    candidates: [
      {
        organizationName: "Boundary Probe Org",
        programmeName: `${contract.segmentLabel} boundary probe`,
        demandMechanismSummary: "Zero-paid adapter boundary probe only",
        evidenceUrls: ["https://example.invalid/probe"],
        uncertainties: ["NOT LIVE"],
      },
    ],
  };
  const result = await executeSegment4912ExaAgentOrsStrategy({
    segmentId,
    mode: "discovery",
    hotel,
    authorizeExaAgentLive: false,
    forceMockOnly: true,
    mockStructured: mock,
    effort: "medium",
    limit: 1,
  });
  const failClosedLive =
    result.run?.ok === true &&
    result.run?.mode !== "live" &&
    (result.run?.forceMockOnly === true ||
      result.requestPayloadMeta?.forceMockOnly === true ||
      result.run?.liveAuthorized !== true);
  return {
    ok: result.run?.ok === true && failClosedLive !== false,
    segmentId,
    contractId: contract.discoveryContractId,
    adapterOk: result.run?.ok === true,
    liveHttpDispatched: result.run?.mode === "live",
    forceMockOnly: result.requestPayloadMeta?.forceMockOnly === true,
    quote: result.quote,
    note: "Zero-paid boundary — live certification not claimed from mocks",
  };
}

/**
 * Run live FullE2E for one hotel × one demand segment from shared-launcher config.
 */
export async function runDemandSegmentLiveFullE2e({ config = {} } = {}) {
  const segment =
    resolveDemandSegment(config.demandSegmentId) ||
    resolveDemandSegment(config.researchSegment) ||
    resolveDemandSegment(config.researchAxis);
  if (!segment) {
    return { ok: false, error: "UNKNOWN_SEGMENT", demandSegmentId: config.demandSegmentId };
  }
  const segmentId = segment.id;
  const contract = getLiveSegmentContract(segmentId);
  if (!contract) {
    return { ok: false, error: "SEGMENT_NOT_LIVE_ORS_READY", segmentId };
  }

  const hotel = resolveHotel(config);
  const paid =
    Boolean(config.authorizePaidResearch) && Number(config.budgetUSD || 0) > 0;
  const mode = String(config.mode || "");
  const outDir =
    config.outputDirectory ||
    config.reportDir ||
    path.join(
      ROOT,
      "reports",
      `gdi-${segmentId}-live-runs`,
      `${hotel.ledgerKey}-${Date.now()}`,
    );

  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(path.join(outDir, "artifacts"), { recursive: true });

  if (!paid || (mode !== "full-e2e" && mode !== "execute")) {
    writeJson(path.join(outDir, "SUMMARY.json"), {
      ok: false,
      error: "PAID_FULL_E2E_REQUIRED",
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
    });
    return { ok: false, error: "EXA_LIVE_AUTH_FAILED", outDir };
  }

  if (hotel.hotelId === NOBU_HOTEL_ID) {
    const approval = ensureNobuFounderApproval();
    if (!approval.ok) {
      writeJson(path.join(outDir, "SUMMARY.json"), approval);
      return approval;
    }
  }

  const hardCeilingUsd = Math.min(
    Number(config.budgetUSD) || 0,
    hotel.hotelId === NOBU_HOTEL_ID ? 6 : Number(config.budgetUSD) || 0,
  );
  const maxCandidates = Math.min(Number(config.maxCandidates) || 8, 10);
  const targetNew = Math.min(
    Number(config.targetNewOpportunities) || 5,
    maxCandidates,
  );
  const discoveryOnly = config.discoveryOnly === true;
  const discQuote = quoteExaAgentRun({ effort: "medium" });
  const deepQuote = quoteExaAgentRun({ effort: "high" });

  const ledger = {
    version: EXA_DEMAND_SEGMENT_LIVE_FULLE2E_CORE_V1,
    ledgerId: `${hotel.ledgerKey}::${segmentId}`,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    segmentId,
    hardCeilingUsd,
    spentUsd: 0,
    entries: [],
    startedAt: new Date().toISOString(),
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

  const exclusions = existingExclusionKeys(hotel.hotelId);
  const artDir = path.join(outDir, "artifacts");
  const discoveryRuns = [];
  let candidates = [];

  const discoveryCount = discoveryOnly ? 1 : 1;
  for (let i = 0; i < discoveryCount; i++) {
    const nextCost = Number(discQuote.maxUsd) || 0.1;
    if (ledger.spentUsd + nextCost > hardCeilingUsd) break;

    let discovery;
    try {
      discovery = await executeSegment4912ExaAgentOrsStrategy({
        segmentId,
        mode: "discovery",
        hotel: hotelCtx,
        exclusions: [...exclusions, ...candidates.map((c) => c.organizationName)],
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
      const found = extractCandidates(discovery.run.structured || {});
      for (const c of found) {
        if (!candidates.some((x) => x.key === c.key)) candidates.push(c);
      }
    }
  }

  candidates = candidates.slice(0, maxCandidates);

  const deepResults = [];
  const opportunities = [];

  if (!discoveryOnly) {
    for (let i = 0; i < candidates.length; i++) {
      if (opportunities.length >= targetNew) break;
      const nextCost = Number(deepQuote.maxUsd) || 0.5;
      if (ledger.spentUsd + nextCost > hardCeilingUsd) break;

      const candidate = candidates[i];
      let deep;
      try {
        deep = await executeSegment4912ExaAgentOrsStrategy({
          segmentId,
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
        candidateName: candidate.organizationName,
        runId: deep.run?.runId || deep.run?.id || null,
        costUsd: cost,
        ok: deep.run?.ok === true,
        status: deep.run?.status || null,
        error: deep.run?.error || null,
      });
      writeJson(path.join(artDir, `deep-${String(i + 1).padStart(2, "0")}.json`), {
        candidate,
        ...deep,
      });
      deepResults.push({ candidate, deep });

      if (deep.run?.ok && deep.run.structured) {
        const opp = mapSegment4912AgentResultToOpportunity({
          hotel: hotelCtx,
          segmentId,
          candidate,
          deepResearch: deep.run.structured,
        });
        if (opp) {
          opportunities.push({
            ...opp,
            nobuLondon14WorkflowLiveV1: hotel.hotelId === NOBU_HOTEL_ID,
            segmentsLiveWaveV1: true,
          });
        }
      }
    }
  }

  ledger.finishedAt = new Date().toISOString();
  ledger.opportunityCount = opportunities.length;
  ledger.candidateCount = candidates.length;
  ledger.paidCallsMade = ledger.entries.length;

  writeJson(path.join(outDir, "ledger.json"), ledger);
  const wLedger = workflowLedgerPath(hotel, segmentId);
  if (wLedger) writeJson(wLedger, ledger);

  const shadowDoc = {
    version: EXA_DEMAND_SEGMENT_LIVE_FULLE2E_CORE_V1,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    segmentId,
    generatedAt: new Date().toISOString(),
    doesNotAffectProduction: true,
    publication: "NOT_AUTHORIZED",
    liveExa: true,
    spentUsd: ledger.spentUsd,
    opportunities,
    discoveryCandidates: candidates,
  };
  writeJson(shadowPathFor(hotel, segmentId), shadowDoc);
  writeJson(path.join(outDir, "opportunities.json"), {
    opportunities,
    candidates,
  });

  const summary = {
    ok: true,
    verdict: discoveryOnly
      ? "LIVE_DISCOVERY_COMPLETE"
      : opportunities.length
        ? "LIVE_FULL_E2E_COMPLETE"
        : candidates.length
          ? "LIVE_FULL_E2E_COMPLETE_NO_MAPPED_OPPORTUNITIES"
          : "LIVE_FULL_E2E_COMPLETE_NO_CANDIDATES",
    version: EXA_DEMAND_SEGMENT_LIVE_FULLE2E_CORE_V1,
    segmentId,
    segmentLabel: segment.label,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    spentUsd: ledger.spentUsd,
    hardCeilingUsd,
    paidCallsMade: ledger.paidCallsMade,
    discoveryOk: discoveryRuns.filter((d) => d.run?.ok).length,
    candidatesFound: candidates.length,
    deepOk: deepResults.filter((d) => d.deep?.run?.ok).length,
    opportunitiesStaged: opportunities.length,
    shadowPath: shadowPathFor(hotel, segmentId),
    outDir,
    publication: "NOT_AUTHORIZED",
    firecrawl: "NOT_AUTHORIZED",
    surfe: "NOT_AUTHORIZED",
    discoveryOnly,
  };
  writeJson(path.join(outDir, "SUMMARY.json"), summary);
  return summary;
}

export { HOTEL_META, NOBU_HOTEL_ID, NOBU_APPROVAL };
