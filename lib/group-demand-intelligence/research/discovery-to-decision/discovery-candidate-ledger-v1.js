/**
 * GDI V37 — durable hotel-generic discovery candidate ledger I/O.
 * Local filesystem only. Internal research use.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createDiscoveryLedgerRecord,
  emptyLedgerDocument,
  LEDGER_DOCUMENT_VERSION,
  validateLedgerRecord,
} from "./discovery-candidate-ledger-schema-v1.js";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../..",
);

export const DISCOVERY_LEDGER_ROOT = path.join(
  ROOT,
  "data/group-demand-intelligence/discovery-ledgers",
);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

export function hotelLedgerDir(hotelId) {
  return path.join(DISCOVERY_LEDGER_ROOT, String(hotelId || "").trim());
}

export function hotelLedgerPath(hotelId, campaignId = "default") {
  const safe = String(campaignId || "default")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .slice(0, 120);
  return path.join(hotelLedgerDir(hotelId), `${safe}.json`);
}

export function loadDiscoveryLedger(hotelId, campaignId = "default") {
  const fp = hotelLedgerPath(hotelId, campaignId);
  if (!fs.existsSync(fp)) {
    return emptyLedgerDocument({ hotelId, researchCampaignId: campaignId });
  }
  const doc = JSON.parse(fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, ""));
  if (!Array.isArray(doc.candidates)) doc.candidates = [];
  return doc;
}

export function saveDiscoveryLedger(doc, { campaignId = null } = {}) {
  const hotelId = doc.hotelId;
  const cid = campaignId || doc.researchCampaignId || "default";
  const fp = hotelLedgerPath(hotelId, cid);
  ensureDir(path.dirname(fp));
  const out = {
    ...doc,
    version: LEDGER_DOCUMENT_VERSION,
    researchCampaignId: cid,
    generatedAt: new Date().toISOString(),
    candidateCount: (doc.candidates || []).length,
  };
  fs.writeFileSync(fp, `${JSON.stringify(out, null, 2)}\n`);
  return { path: fp, count: out.candidateCount };
}

export function upsertDiscoveryCandidates(hotelId, records, opts = {}) {
  const campaignId = opts.campaignId || "default";
  const doc = loadDiscoveryLedger(hotelId, campaignId);
  doc.hotelId = hotelId;
  if (opts.hotelKey) doc.hotelKey = opts.hotelKey;
  if (opts.hotelName) doc.hotelName = opts.hotelName;
  doc.researchCampaignId = campaignId;
  const byId = new Map((doc.candidates || []).map((c) => [c.discoveryId, c]));
  for (const rec of records || []) {
    const v = validateLedgerRecord(rec);
    if (!v.ok) continue;
    byId.set(rec.discoveryId, rec);
  }
  doc.candidates = [...byId.values()];
  return saveDiscoveryLedger(doc, { campaignId });
}

export function listDiscoveryCandidates(hotelId, filters = {}) {
  const campaignId = filters.campaignId || "default";
  const doc = loadDiscoveryLedger(hotelId, campaignId);
  let rows = [...(doc.candidates || [])];
  if (filters.workflowId) {
    rows = rows.filter((r) => r.workflowId === filters.workflowId);
  }
  if (filters.researchStatus) {
    rows = rows.filter((r) => r.researchStatus === filters.researchStatus);
  }
  if (filters.dispositionCode) {
    rows = rows.filter((r) => r.disposition?.code === filters.dispositionCode);
  }
  if (filters.organization) {
    const q = String(filters.organization).toLowerCase();
    rows = rows.filter((r) =>
      String(r.organizationName || "")
        .toLowerCase()
        .includes(q),
    );
  }
  if (filters.q) {
    const q = String(filters.q).toLowerCase();
    rows = rows.filter((r) => {
      const hay = [
        r.organizationName,
        r.programmeName,
        r.opportunityId,
        r.discoveryId,
        ...(r.originalSourceUrls || []),
        r.originalResultTitle,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }
  if (filters.language) {
    rows = rows.filter((r) => r.queryLanguage === filters.language);
  }
  if (filters.from) {
    const t = Date.parse(filters.from);
    if (!Number.isNaN(t)) {
      rows = rows.filter((r) => Date.parse(r.queryTimestamp || 0) >= t);
    }
  }
  if (filters.to) {
    const t = Date.parse(filters.to);
    if (!Number.isNaN(t)) {
      rows = rows.filter((r) => Date.parse(r.queryTimestamp || 0) <= t);
    }
  }
  return { document: doc, candidates: rows };
}

/**
 * Persist a single discovery pass into the ledger (for future Full E2E hooks).
 */
export function persistDiscoveryPassToLedger({
  hotelId,
  hotelKey,
  hotelName,
  workflowId,
  powershell,
  pass,
  exaRunId,
  queryTimestamp,
  queryLanguage,
  candidates,
  discoveryCostUsd,
  artifactPath,
  campaignId = "default",
  researchRunId = null,
} = {}) {
  const n = Math.max(1, (candidates || []).length);
  const records = (candidates || []).map((c) =>
    createDiscoveryLedgerRecord({
      hotelId,
      hotelKey,
      hotelName,
      workflowId,
      powershell,
      pass,
      exaRunId,
      queryTimestamp,
      queryLanguage,
      candidate: c,
      discoveryCostUsd:
        discoveryCostUsd == null ? null : Number(discoveryCostUsd) / n,
      artifactPath,
      researchRunId,
      provenanceAvailable: true,
      provenanceGaps: [
        "raw_exa_agent_text_not_persisted",
        "raw_exa_grounding_not_persisted",
      ],
    }),
  );
  return {
    records,
    write: upsertDiscoveryCandidates(hotelId, records, {
      campaignId,
      hotelKey,
      hotelName,
    }),
  };
}
