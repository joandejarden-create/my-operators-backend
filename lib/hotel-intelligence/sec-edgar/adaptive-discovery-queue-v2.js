/**
 * Adaptive research expansion with hard caps and dedupe.
 * Directions:
 *  - SEC registrant → hotels → owners/operators → related registrants
 *  - HPC hotel → identities → companies → SEC filings
 */

import fs from "node:fs";
import path from "node:path";

export const ADAPTIVE_DISCOVERY_QUEUE_V2 = "sec-edgar-adaptive-discovery-queue-v2";

/** Prevent uncontrolled recursive expansion */
export const DEFAULT_QUEUE_CAPS = Object.freeze({
  maxCompanies: 40,
  maxDepth: 2,
  maxEnqueuePerFinding: 3,
  requireVerifiedEntity: true,
});

export function defaultQueuePath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "adaptive-queue", "queue-v2.json");
}

export function loadAdaptiveQueue(filePath = defaultQueuePath()) {
  if (!fs.existsSync(filePath)) {
    return {
      version: ADAPTIVE_DISCOVERY_QUEUE_V2,
      caps: { ...DEFAULT_QUEUE_CAPS },
      seedCompanies: [],
      companies: {},
      rejected: [],
      updatedAt: null,
    };
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return {
      version: ADAPTIVE_DISCOVERY_QUEUE_V2,
      caps: { ...DEFAULT_QUEUE_CAPS },
      seedCompanies: [],
      companies: {},
      rejected: [],
      error: "QUEUE_PARSE_FAILED",
    };
  }
}

export function saveAdaptiveQueue(queue, filePath = defaultQueuePath()) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const payload = { ...queue, version: ADAPTIVE_DISCOVERY_QUEUE_V2, updatedAt: new Date().toISOString() };
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

function companyKey(entry) {
  if (entry.cik) return `cik:${String(entry.cik).replace(/\D/g, "").padStart(10, "0")}`;
  if (entry.ticker) return `ticker:${String(entry.ticker).toUpperCase()}`;
  return `name:${String(entry.legalName || entry.name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}`;
}

/**
 * Seed queue from known research companies.
 */
export function seedAdaptiveQueue(queue, companies = [], opts = {}) {
  const next = {
    ...queue,
    caps: { ...DEFAULT_QUEUE_CAPS, ...(queue.caps || {}), ...(opts.caps || {}) },
    companies: { ...(queue.companies || {}) },
    seedCompanies: [...(queue.seedCompanies || [])],
  };
  for (const c of companies) {
    const key = companyKey(c);
    if (!next.seedCompanies.includes(key)) next.seedCompanies.push(key);
    if (!next.companies[key]) {
      next.companies[key] = {
        key,
        slug: c.slug || null,
        legalName: c.legalName || c.name || null,
        ticker: c.ticker || null,
        cik: c.cik || null,
        depth: 0,
        source: "SEED",
        status: "QUEUED",
        priority: c.priority || 2,
        enqueuedAt: new Date().toISOString(),
      };
    }
  }
  return next;
}

/**
 * Propose enqueue from a verified discovery (owner/operator/parent).
 * Rejects duplicates, over-depth, and unverified entities when required.
 */
export function proposeRelatedCompany(queue, proposal = {}, opts = {}) {
  const caps = { ...DEFAULT_QUEUE_CAPS, ...(queue.caps || {}), ...(opts.caps || {}) };
  const verified = proposal.verified === true;
  if (caps.requireVerifiedEntity && !verified) {
    return {
      ok: false,
      reason: "UNVERIFIED_ENTITY",
      message: "Adaptive enqueue requires verified owner/operator/parent evidence",
    };
  }
  const depth = Number(proposal.depth ?? (proposal.parentDepth || 0) + 1);
  if (depth > caps.maxDepth) {
    return { ok: false, reason: "MAX_DEPTH", depth, maxDepth: caps.maxDepth };
  }
  const activeCount = Object.keys(queue.companies || {}).length;
  if (activeCount >= caps.maxCompanies) {
    return { ok: false, reason: "MAX_COMPANIES", activeCount, maxCompanies: caps.maxCompanies };
  }
  const key = companyKey(proposal);
  if (!key || key.endsWith(":")) {
    return { ok: false, reason: "MISSING_IDENTITY" };
  }
  if (queue.companies?.[key]) {
    return { ok: false, reason: "DUPLICATE", key };
  }
  // Block obvious non-research noise
  if (/marriott international|hilton worldwide/i.test(proposal.legalName || "") && depth > 0) {
    // Already seeded typically — still allow if not present, but deprioritize
  }

  const entry = {
    key,
    slug: proposal.slug || null,
    legalName: proposal.legalName || proposal.name || null,
    ticker: proposal.ticker || null,
    cik: proposal.cik || null,
    depth,
    source: proposal.source || "ADAPTIVE",
    parentKey: proposal.parentKey || null,
    evidenceSnippet: proposal.evidenceSnippet
      ? String(proposal.evidenceSnippet).slice(0, 240)
      : null,
    status: "QUEUED",
    priority: proposal.priority || 3 + depth,
    enqueuedAt: new Date().toISOString(),
  };

  return { ok: true, key, entry };
}

export function applyEnqueue(queue, proposalResult) {
  if (!proposalResult?.ok) {
    const rejected = [...(queue.rejected || []), { ...proposalResult, at: new Date().toISOString() }];
    // Keep rejected list bounded
    return { ...queue, rejected: rejected.slice(-200) };
  }
  return {
    ...queue,
    companies: {
      ...(queue.companies || {}),
      [proposalResult.key]: proposalResult.entry,
    },
  };
}

/**
 * Reverse path: HPC hotel → identity hints → company research candidates.
 */
export function proposeFromHpcHotel(hotel = {}, opts = {}) {
  const names = [
    hotel.operator,
    hotel.managementCompany,
    hotel.owner,
    hotel.economicOwner,
    hotel.parentCompany,
    hotel.brand,
  ]
    .map((s) => String(s || "").trim())
    .filter((s) => s.length >= 3);

  const unique = [...new Set(names)];
  return unique.slice(0, opts.limit || DEFAULT_QUEUE_CAPS.maxEnqueuePerFinding).map((name) => ({
    legalName: name,
    verified: opts.verified === true,
    source: "HPC_REVERSE",
    parentKey: hotel.id || hotel.hpcRecordId || null,
    depth: 1,
    priority: 3,
    evidenceSnippet: `HPC hotel ${hotel.name || hotel.hotelName || ""} → entity ${name}`,
  }));
}

export function listQueuedCompanies(queue, opts = {}) {
  const rows = Object.values(queue.companies || {}).filter((c) => c.status === "QUEUED" || opts.includeAll);
  return rows.sort((a, b) => (a.priority || 99) - (b.priority || 99) || (a.depth || 0) - (b.depth || 0));
}

export function markCompanyStatus(queue, key, status, meta = {}) {
  if (!queue.companies?.[key]) return queue;
  return {
    ...queue,
    companies: {
      ...queue.companies,
      [key]: {
        ...queue.companies[key],
        status,
        ...meta,
        updatedAt: new Date().toISOString(),
      },
    },
  };
}
