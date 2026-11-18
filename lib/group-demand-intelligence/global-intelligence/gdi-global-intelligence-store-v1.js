/**
 * Local filesystem store for global discovered intelligence.
 * Canonical path under data/group-demand-intelligence/global-intelligence/.
 * Idempotent upsert by globalDemandId. No Airtable writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildGlobalDiscoveredIntelligenceRecord,
  buildHotelOpportunityMatchRecord,
  GLOBAL_INTELLIGENCE_STATE,
} from "./gdi-global-discovered-intelligence-contract-v1.js";
import {
  stripHotelPrivateFields,
  assertNoHotelPrivateLeak,
} from "./gdi-global-intelligence-privacy-v1.js";

export const GDI_GLOBAL_INTELLIGENCE_STORE_V1 =
  "gdi-global-intelligence-store-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const DEFAULT_ROOT = path.join(
  ROOT,
  "data/group-demand-intelligence/global-intelligence",
);

/** Bounded retention: keep evidence URLs capped; soft-archive unsupported after N days. */
export const GLOBAL_RETENTION_RULES = Object.freeze({
  maxEvidenceUrls: 20,
  maxIndexEntries: 50000,
  unsupportedAuditRetainDays: 90,
  verifiedRetainIndefinite: true,
});

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

export function resolveGlobalIntelligencePaths(root = DEFAULT_ROOT) {
  return {
    root,
    demandsDir: path.join(root, "demands"),
    matchesDir: path.join(root, "hotel-matches"),
    indexPath: path.join(root, "index.json"),
  };
}

function loadIndex(indexPath) {
  if (!fs.existsSync(indexPath)) {
    return {
      version: GDI_GLOBAL_INTELLIGENCE_STORE_V1,
      updatedAt: null,
      byId: {},
      byOrgKey: {},
      byMarket: {},
    };
  }
  try {
    return JSON.parse(fs.readFileSync(indexPath, "utf8"));
  } catch {
    return {
      version: GDI_GLOBAL_INTELLIGENCE_STORE_V1,
      updatedAt: null,
      byId: {},
      byOrgKey: {},
      byMarket: {},
      corrupt: true,
    };
  }
}

function saveIndex(indexPath, index) {
  ensureDir(path.dirname(indexPath));
  const next = {
    ...index,
    version: GDI_GLOBAL_INTELLIGENCE_STORE_V1,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(indexPath, `${JSON.stringify(next, null, 2)}\n`);
  return next;
}

function mergeEvidence(existing = [], incoming = []) {
  const map = new Map();
  for (const e of [...existing, ...incoming]) {
    const url = String(e?.url || "");
    if (!/^https?:\/\//i.test(url)) continue;
    if (!map.has(url)) map.set(url, e);
  }
  return [...map.values()].slice(0, GLOBAL_RETENTION_RULES.maxEvidenceUrls);
}

/**
 * Idempotent upsert of a global demand record.
 * Duplicate identity → merge evidence; do not create a second ID.
 */
export function upsertGlobalDemandRecord(record, opts = {}) {
  const paths = resolveGlobalIntelligencePaths(opts.root);
  ensureDir(paths.demandsDir);
  const cleaned = stripHotelPrivateFields(record);
  const privacy = assertNoHotelPrivateLeak(cleaned);
  if (!privacy.ok) {
    return {
      ok: false,
      blocked: true,
      reason: "PRIVACY_VIOLATION",
      privacy,
    };
  }
  const id = cleaned.globalDemandId;
  if (!id) {
    return { ok: false, blocked: true, reason: "MISSING_GLOBAL_DEMAND_ID" };
  }
  const filePath = path.join(paths.demandsDir, `${id}.json`);
  let prior = null;
  if (fs.existsSync(filePath)) {
    try {
      prior = JSON.parse(fs.readFileSync(filePath, "utf8"));
    } catch {
      prior = null;
    }
  }
  const now = opts.nowIso || new Date().toISOString();
  const merged = prior
    ? {
        ...prior,
        ...cleaned,
        evidence: mergeEvidence(prior.evidence, cleaned.evidence),
        firstDiscoveredAt: prior.firstDiscoveredAt || cleaned.firstDiscoveredAt || now,
        lastUpdatedAt: now,
        priorStates: [
          ...(prior.priorStates || []),
          prior.state !== cleaned.state
            ? { at: now, from: prior.state, to: cleaned.state }
            : null,
        ].filter(Boolean),
        // Never allow private fields to reappear via merge
        hotelSalesActivity: null,
        privateHotelFeedback: null,
      }
    : { ...cleaned, firstDiscoveredAt: cleaned.firstDiscoveredAt || now, lastUpdatedAt: now };

  // Do not demote VERIFIED to REJECTED_UNSUPPORTED on geographic hotel mismatch alone
  if (
    prior?.state === GLOBAL_INTELLIGENCE_STATE.VERIFIED_GLOBAL_SIGNAL &&
    cleaned.state === GLOBAL_INTELLIGENCE_STATE.REJECTED_UNSUPPORTED &&
    cleaned.classification?.hotelMatchReason === "OUT_OF_MARKET_FOR_CURRENT_HOTEL"
  ) {
    merged.state = GLOBAL_INTELLIGENCE_STATE.VERIFIED_GLOBAL_SIGNAL;
  }

  fs.writeFileSync(filePath, `${JSON.stringify(merged, null, 2)}\n`);

  const index = loadIndex(paths.indexPath);
  index.byId[id] = {
    globalDemandId: id,
    organizationKey: merged.organizationKey,
    marketKey: merged.marketLocation?.key || null,
    state: merged.state,
    path: path.relative(paths.root, filePath).replace(/\\/g, "/"),
    lastUpdatedAt: merged.lastUpdatedAt,
  };
  if (merged.organizationKey) {
    index.byOrgKey[merged.organizationKey] = [
      ...new Set([...(index.byOrgKey[merged.organizationKey] || []), id]),
    ];
  }
  const mk = merged.marketLocation?.key;
  if (mk) {
    index.byMarket[mk] = [...new Set([...(index.byMarket[mk] || []), id])];
  }
  saveIndex(paths.indexPath, index);

  return {
    ok: true,
    created: !prior,
    updated: Boolean(prior),
    globalDemandId: id,
    state: merged.state,
    path: filePath,
  };
}

export function upsertHotelOpportunityMatch(match, opts = {}) {
  const paths = resolveGlobalIntelligencePaths(opts.root);
  ensureDir(paths.matchesDir);
  const cleaned = stripHotelPrivateFields(match);
  const hotelId = cleaned.hotelId || "unknown_hotel";
  const id = cleaned.globalDemandId || "unknown";
  const filePath = path.join(paths.matchesDir, `${hotelId}__${id}.json`);
  let prior = null;
  if (fs.existsSync(filePath)) {
    try {
      prior = JSON.parse(fs.readFileSync(filePath, "utf8"));
    } catch {
      prior = null;
    }
  }
  const now = opts.nowIso || new Date().toISOString();
  const merged = {
    ...(prior || {}),
    ...cleaned,
    createdAt: prior?.createdAt || cleaned.createdAt || now,
    updatedAt: now,
  };
  fs.writeFileSync(filePath, `${JSON.stringify(merged, null, 2)}\n`);
  return { ok: true, path: filePath, created: !prior };
}

export function loadGlobalDemandRecord(globalDemandId, opts = {}) {
  const paths = resolveGlobalIntelligencePaths(opts.root);
  const filePath = path.join(paths.demandsDir, `${globalDemandId}.json`);
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function findGlobalDemandsByOrgKey(organizationKey, opts = {}) {
  const paths = resolveGlobalIntelligencePaths(opts.root);
  const index = loadIndex(paths.indexPath);
  const ids = index.byOrgKey?.[organizationKey] || [];
  return ids.map((id) => loadGlobalDemandRecord(id, opts)).filter(Boolean);
}

export function findGlobalDemandsByMarketKey(marketKey, opts = {}) {
  const paths = resolveGlobalIntelligencePaths(opts.root);
  const index = loadIndex(paths.indexPath);
  const ids = index.byMarket?.[marketKey] || [];
  return ids.map((id) => loadGlobalDemandRecord(id, opts)).filter(Boolean);
}

/**
 * Retain discovery candidates into the global store + hotel match rows.
 */
export function retainGlobalIntelligenceFromCandidates(candidates = [], context = {}) {
  const results = [];
  let retainedGlobal = 0;
  let outOfMarketRetained = 0;
  let unsupportedAudit = 0;
  let privacyBlocked = 0;

  for (const row of candidates) {
    const globalRecord = buildGlobalDiscoveredIntelligenceRecord(row, context);
    const match = buildHotelOpportunityMatchRecord(row, context, globalRecord);
    const upsert = upsertGlobalDemandRecord(globalRecord, {
      root: context.root,
      nowIso: context.nowIso,
    });
    if (!upsert.ok) {
      privacyBlocked += 1;
      results.push({ candidateId: row.id, upsert, match: null });
      continue;
    }
    upsertHotelOpportunityMatch(match, { root: context.root, nowIso: context.nowIso });
    retainedGlobal += 1;
    if (globalRecord.classification?.outOfMarketForCurrentHotel) outOfMarketRetained += 1;
    if (globalRecord.state === GLOBAL_INTELLIGENCE_STATE.REJECTED_UNSUPPORTED) {
      unsupportedAudit += 1;
    }
    results.push({
      candidateId: row.id || row.opportunityId,
      globalDemandId: upsert.globalDemandId,
      state: upsert.state,
      hotelMatchReason: match.matchReason,
      created: upsert.created,
    });
  }

  return {
    version: GDI_GLOBAL_INTELLIGENCE_STORE_V1,
    ok: true,
    candidateCount: candidates.length,
    retainedGlobal,
    outOfMarketRetained,
    unsupportedAudit,
    privacyBlocked,
    results,
  };
}

/**
 * Pull candidates from a fourteen-segment discovery execution object.
 */
export function extractCandidatesFromDiscoveryExecution(discoveryExecution = {}) {
  const out = [];
  for (const cell of discoveryExecution.cells || []) {
    for (const c of cell.candidates || []) {
      out.push({
        ...c,
        hotelId: c.hotelId || cell.hotelId,
        researchSegment: c.researchSegment || c.segmentId || cell.segmentId,
        segmentId: c.segmentId || cell.segmentId,
        providerRunId: c.providerRunId || cell.providerRunId,
      });
    }
  }
  return out;
}

export default {
  GDI_GLOBAL_INTELLIGENCE_STORE_V1,
  GLOBAL_RETENTION_RULES,
  resolveGlobalIntelligencePaths,
  upsertGlobalDemandRecord,
  upsertHotelOpportunityMatch,
  loadGlobalDemandRecord,
  findGlobalDemandsByOrgKey,
  findGlobalDemandsByMarketKey,
  retainGlobalIntelligenceFromCandidates,
  extractCandidatesFromDiscoveryExecution,
};
