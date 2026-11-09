#!/usr/bin/env node
/**
 * Zero-cost reprocess of already-paid Exa pilot structured outputs.
 * Fixes event/scaling schema extraction into durable research state.
 * Does NOT call Exa. Does NOT spend.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  persistDiscoveryTrace,
  loadDiscoveryState,
  shouldSkipLiveProviderInvocation,
} from "../lib/group-demand-intelligence/research/full-e2e-fourteen-segment-discovery-state-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const RAW_DIR = path.join(
  ROOT,
  "reports/gdi-live-discovery-pilot-v1-executed/raw-exa"
);

const RUN_MAP = [
  {
    runId: "agent_run_44defea7079746569baa8bc8f25930e0",
    hotelId: "recCAGqkERSXgiku4",
    segmentId: "associations_conferences_events",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_bbe46836323144e48c6952d2fb1d1012",
    hotelId: "recCAGqkERSXgiku4",
    segmentId: "corporate_accounts_scaling",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_7aaf010b0b3347a0a256f0bce394c426",
    hotelId: "recTk5I8VOzsPhxDz",
    segmentId: "associations_conferences_events",
    costUsd: 0.1,
  },
  {
    runId: "agent_run_e4d96ee5156f4a989ae806f9985ecfea",
    hotelId: "recTk5I8VOzsPhxDz",
    segmentId: "corporate_accounts_scaling",
    costUsd: 0.1,
  },
];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function mapRow(row, hotelId, segmentId) {
  const company =
    row.companyIdentity && typeof row.companyIdentity === "object"
      ? row.companyIdentity
      : null;
  const urls = [
    ...((row.supportingSources || []).map((s) => s?.url || s?.sourceUrl)),
    ...((row.supportingEvidence || []).map((s) => s?.url || s?.sourceUrl)),
    ...((row.evidenceClaims || []).map((s) => s?.url || s?.sourceUrl)),
    company?.website,
    company?.officialWebsite,
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
  const organizationName =
    text(
      row.organizingEntity ||
        row.corporateOrAssociationOwner ||
        company?.canonicalName ||
        company?.legalOrTradingName ||
        ""
    ) || null;
  const title =
    text(
      row.verifiedEventName ||
        row.eventIdentity ||
        row.verifiedBusinessActivity ||
        row.businessTrigger ||
        organizationName ||
        ""
    ) || null;
  const key = norm(`${organizationName || ""}|${title || ""}`);
  const candidateId = `live_${sha12(`${hotelId}|${segmentId}|${key}`)}`;
  return {
    candidateId,
    hotelId,
    segmentId,
    organizationName,
    title,
    stage: "A_DISCOVERY",
    sourceFamily: "FRESH_RETRIEVAL",
    evidenceUrls: [...new Set(urls)],
    hasSourceEvidence: urls.length > 0,
    unsupportedNameOnly: urls.length === 0,
    historical: false,
    provenance: {
      retrievalMode: "FRESH_RETRIEVAL",
      excerpt: text(
        row.rankScoreExplain ||
          row.hotelDemandHypothesis ||
          row.hotelRelevanceHypothesis ||
          ""
      ).slice(0, 280),
      demandFamily: row.demandFamily || row.growthSignalType || null,
      discoveryDisposition: row.discoveryDisposition || null,
      hotelMarketRelevance: row.hotelMarketRelevance || null,
    },
  };
}

function loadInventory(hotelId) {
  const map = {
    recCAGqkERSXgiku4:
      "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/sheraton-guadalajara-expo-inventory.json",
    recTk5I8VOzsPhxDz:
      "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/voco-cancun-zona-hotelera-inventory.json",
  };
  const p = path.join(ROOT, map[hotelId] || "");
  if (!fs.existsSync(p)) return [];
  return JSON.parse(fs.readFileSync(p, "utf8")).rows || [];
}

function fuzzy(a, b) {
  if (!a || !b) return false;
  if (a === b || a.includes(b) || b.includes(a)) return true;
  const at = new Set(a.split(" ").filter((t) => t.length > 3));
  const bt = b.split(" ").filter((t) => t.length > 3);
  if (!bt.length) return false;
  return bt.filter((t) => at.has(t)).length / bt.length >= 0.6;
}

function classify(c) {
  if (!c.organizationName && !c.title) {
    return { commercialClass: "REJECTED", disposition: "Rejected" };
  }
  if (!c.hasSourceEvidence) {
    return {
      commercialClass: "NEEDS_MORE_RESEARCH",
      disposition: "Needs More Research",
    };
  }
  return {
    commercialClass: "ACCOUNT_DEVELOPMENT",
    disposition: "Account Development",
    note: "Evidence-backed Stage A; not Sales Ready without commercial depth",
  };
}

const cells = [];
for (const meta of RUN_MAP) {
  const rawPath = path.join(RAW_DIR, `${meta.runId}.json`);
  const raw = JSON.parse(fs.readFileSync(rawPath, "utf8"));
  const rows = raw.structured?.candidates || [];
  const mapped = rows.map((r) => mapRow(r, meta.hotelId, meta.segmentId));
  const inventory = loadInventory(meta.hotelId);
  const retained = [];
  const duplicates = [];
  for (const c of mapped) {
    const org = norm(c.organizationName || c.title);
    const inv = inventory.find((row) =>
      fuzzy(org, norm(`${row.organizationName || ""} ${row.title || ""}`))
    );
    const commercial = classify(c);
    if (inv) {
      duplicates.push({
        candidateId: c.candidateId,
        matchedInventoryId: inv.candidateId,
      });
      retained.push({
        ...c,
        retainedKind: "ENRICHMENT_OF_EXISTING",
        matchedInventoryId: inv.candidateId,
        commercial,
      });
    } else {
      retained.push({
        ...c,
        retainedKind: c.hasSourceEvidence ? "NEW_OR_EVIDENCED" : "NEW_UNSUPPORTED_NAME",
        commercial,
      });
    }
  }

  // Persist without double-counting cost: set cost 0 on reprocess trace;
  // ensure liveCalls/cost already on state from live run remain.
  const prior = loadDiscoveryState(meta.hotelId, meta.segmentId);
  const trace = {
    hotelId: meta.hotelId,
    segmentId: meta.segmentId,
    researchRunId: prior?.researchRunId || "gdi_live_pilot_reprocess",
    executionStatus: DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED,
    retrievalMode: "FRESH_RETRIEVAL_REPROCESS",
    providerCallCount: 0,
    freshRetrievals: 0,
    cacheHits: mapped.length,
    researchCostUsd: 0,
    stopReason: "REPROCESS_STRUCTURED_OUTPUT_NO_NEW_SPEND",
    candidates: retained,
    rejections: [],
    providerRunIds: [meta.runId],
    querySpecs: [],
    discoveryCompleted: false,
  };
  const saved = persistDiscoveryTrace(trace);
  // Patch checkpoint kind + ensure live call accounting preserved
  const st = loadDiscoveryState(meta.hotelId, meta.segmentId);
  st.executionStatus = DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED;
  st.providerUsage = {
    ...(st.providerUsage || {}),
    liveCalls: Math.max(Number(st.providerUsage?.liveCalls || 0), 1),
    costUsd: Math.max(Number(st.providerUsage?.costUsd || 0), meta.costUsd),
  };
  st.lastSuccessfulCheckpoint = {
    ...(st.lastSuccessfulCheckpoint || {}),
    kind: "LIVE_PROVIDER_COMPLETE",
    providerRunIds: [meta.runId],
    at: new Date().toISOString(),
    reprocessedFromRaw: true,
  };
  fs.writeFileSync(saved.path, `${JSON.stringify(st, null, 2)}\n`);

  cells.push({
    hotelId: meta.hotelId,
    segmentId: meta.segmentId,
    runId: meta.runId,
    rawCount: rows.length,
    mappedCount: mapped.length,
    evidenceBacked: mapped.filter((c) => c.hasSourceEvidence).length,
    newlyRetained: retained.filter((c) => c.retainedKind !== "ENRICHMENT_OF_EXISTING")
      .length,
    enrichments: retained.filter((c) => c.retainedKind === "ENRICHMENT_OF_EXISTING")
      .length,
    duplicates: duplicates.length,
    sample: retained.slice(0, 3).map((c) => ({
      candidateId: c.candidateId,
      organizationName: c.organizationName,
      title: c.title,
      urls: c.evidenceUrls?.slice(0, 2),
      commercial: c.commercial?.commercialClass,
      retainedKind: c.retainedKind,
    })),
    skipAfter: shouldSkipLiveProviderInvocation(
      loadDiscoveryState(meta.hotelId, meta.segmentId)
    ),
    durableStatePath: saved.path,
  });
}

const out = {
  version: "gdi-live-pilot-exa-reprocess-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  additionalSpendUsd: 0,
  cells,
  totals: {
    raw: cells.reduce((n, c) => n + c.rawCount, 0),
    mapped: cells.reduce((n, c) => n + c.mappedCount, 0),
    evidenceBacked: cells.reduce((n, c) => n + c.evidenceBacked, 0),
    newlyRetained: cells.reduce((n, c) => n + c.newlyRetained, 0),
    enrichments: cells.reduce((n, c) => n + c.enrichments, 0),
  },
};
fs.writeFileSync(
  path.join(ROOT, "reports/gdi-live-discovery-pilot-v1-executed/reprocess-report.json"),
  `${JSON.stringify(out, null, 2)}\n`
);
console.log(JSON.stringify(out.totals, null, 2));
console.log(
  JSON.stringify(
    cells.map((c) => ({
      hotelId: c.hotelId,
      segmentId: c.segmentId,
      raw: c.rawCount,
      evidenced: c.evidenceBacked,
      new: c.newlyRetained,
      enrich: c.enrichments,
      skip: c.skipAfter.skip,
    })),
    null,
    2
  )
);
