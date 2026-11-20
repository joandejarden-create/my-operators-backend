#!/usr/bin/env node
/**
 * Contract Timing V2 — Exa + SEC-API.io controlled discovery benchmark.
 *
 *   node scripts/sec-edgar-contract-timing-v2-provider-pilot.mjs
 *   node scripts/sec-edgar-contract-timing-v2-provider-pilot.mjs --stage=calibration
 *   node scripts/sec-edgar-contract-timing-v2-provider-pilot.mjs --apply
 *
 * Ceiling: $25 incremental paid (Exa). SEC-API.io subscription calls tracked separately.
 * Baseline JPMCC 0001539497-20-000579 excluded from incremental yield.
 * Free SEC EDGAR retrieval continues. CUSTOMER_BLOCKED only. No HPC auto-create.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  createContractTimingV2Budget,
  CONTRACT_TIMING_V2_PAID_CEILING_USD,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createSecApiIoClient } from "../lib/hotel-intelligence/sec-edgar/sec-api-io-client-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import {
  listSecApiQueryPlan,
  EXA_QUERY_FAMILIES,
  isBaselineAccession,
  BASELINE_JPMCC_ACCESSION,
  BASELINE_JPMCC_DATE_COUNT,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-query-families-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { bridgeFranchiseScheduleToContractTiming } from "../lib/hotel-intelligence/sec-edgar/contract-timing-franchise-bridge-v1.js";
import { extractEx10AgreementTerms } from "../lib/hotel-intelligence/sec-edgar/ex10-agreement-term-extractor-v1.js";
import { verifyContractTimingFindings } from "../lib/hotel-intelligence/sec-edgar/contract-timing-verifier-v1.js";
import { SOURCE_FAMILY } from "../lib/hotel-intelligence/sec-edgar/contract-timing-types-v1.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
// Load both envs without override so SEC_API_IO_KEY (Integration) and EXA (either) are available.
for (const p of [
  "C:/Dev/dealality/my-operators-backend/.env",
  path.join(ROOT, ".env"),
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const APPLY = process.argv.includes("--apply");
const STAGE_ARG = process.argv.find((a) => a.startsWith("--stage="))?.split("=")[1] || "full";
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-contract-timing-v2");
fs.mkdirSync(OUT_DIR, { recursive: true });

const STAGE_DOC_CAPS = {
  calibration: 20,
  comparative: 100, // 20 + 80
  full: 200,
  scale: 200,
};

const DOC_CAP = STAGE_DOC_CAPS[STAGE_ARG] || 200;

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function loadTableIds() {
  return JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"),
      "utf8",
    ),
  );
}

function looksLikeYearBuiltCollateral(snippet) {
  const s = String(snippet || "");
  if (/Hospitality\s*\|\s*(Full|Limited|Select)\s+Service\s*\|\s*\d{4}/i.test(s)) return true;
  if (/\bYear\s*Built\b/i.test(s) && /\bRooms?\b/i.test(s)) return true;
  return false;
}

function parseAccessionFromUrl(url) {
  const u = String(url || "");
  const dashed = u.match(/(\d{10}-\d{2}-\d{6})/);
  if (dashed) return dashed[1];
  // /Archives/edgar/data/{cik}/{accession_nodash}/file
  const nodash = u.match(/\/(\d{18})\//);
  if (nodash) {
    const a = nodash[1];
    return `${a.slice(0, 10)}-${a.slice(10, 12)}-${a.slice(12)}`;
  }
  return null;
}

function extractFromDocument(text, meta) {
  const family = meta.sourceFamily || SOURCE_FAMILY.CMBS_424B;
  const cmbs = extractCmbsPropertySchedule({ text }, meta);
  let bridged = { hotelContractFindings: [], rejected: [] };
  let ex10 = { hotelContractFindings: [], rejected: [] };
  if (family === SOURCE_FAMILY.HOTEL_REIT_SCHEDULE || family === SOURCE_FAMILY.OTHER_SEC) {
    const sched = extractFranchiseExpirationSchedule({ text }, meta);
    bridged = bridgeFranchiseScheduleToContractTiming(sched, meta);
  }
  if (
    family === SOURCE_FAMILY.MANAGEMENT_EXHIBIT_EX10 ||
    /ex-?10|exhibit.?10|dex10/i.test(String(meta.documentUrl || meta.form || ""))
  ) {
    ex10 = extractEx10AgreementTerms({ text }, meta);
  }
  const byId = new Map();
  for (const f of [
    ...cmbs.hotelContractFindings,
    ...bridged.hotelContractFindings,
    ...ex10.hotelContractFindings,
  ]) {
    if (looksLikeYearBuiltCollateral(f.snippet)) continue;
    const year = Number(String(f.primaryDate || "").slice(0, 4));
    if (Number.isFinite(year) && year < 1995) continue;
    const key = f.findingId || sha1(`${f.hotelName}|${f.primaryDate}|${f.dateMeaning}`);
    if (!byId.has(key)) {
      byId.set(key, {
        ...f,
        findingId: String(key).startsWith("ctim_") ? key : `ctim_${String(key).slice(0, 20)}`,
      });
    }
  }
  return {
    hotelContractFindings: [...byId.values()],
    loanMaturityFindings: cmbs.loanMaturityFindings,
    rejected: [...cmbs.rejected, ...bridged.rejected, ...(ex10.rejected || [])],
  };
}

function inferSourceFamily(formType, url) {
  const f = String(formType || "").toUpperCase();
  const u = String(url || "").toLowerCase();
  if (f.startsWith("424B") || /424b/.test(u)) return SOURCE_FAMILY.CMBS_424B;
  if (/ex-?10|exhibit.?10/i.test(f) || /ex10|ex-10/.test(u)) return SOURCE_FAMILY.MANAGEMENT_EXHIBIT_EX10;
  if (f === "10-K" || f === "10-K/A" || f.startsWith("S-11")) return SOURCE_FAMILY.HOTEL_REIT_SCHEDULE;
  return SOURCE_FAMILY.OTHER_SEC;
}

function dealKey(hit) {
  // Approximate securitization deal identity by accession (primary filing package)
  return String(hit.accessionNumber || hit.url || "").trim() || null;
}

async function discoverSecApiIo(client, budget, opts = {}) {
  const plan = listSecApiQueryPlan().filter((p) => (opts.priorityMax || 3) >= p.priority);
  const hits = [];
  const byAccession = new Map();
  const queryStats = {};
  let queriesRun = 0;
  const maxQueries = opts.maxQueries || 36;

  for (const step of plan) {
    if (queriesRun >= maxQueries) break;
    if (budget.remaining() <= 0 && budget.snapshot().stoppedAtBudget) break;
    const res = await client.fullTextSearch({
      query: step.query,
      formTypes: step.formTypes?.slice(0, 6),
      startDate: step.startDate,
      endDate: step.endDate,
      page: "1",
    });
    queriesRun += 1;
    queryStats[step.queryFamily] = queryStats[step.queryFamily] || {
      queries: 0,
      hits: 0,
      uniqueAccessions: new Set(),
    };
    queryStats[step.queryFamily].queries += 1;
    if (!res.ok) {
      queryStats[step.queryFamily].lastError = res.error || res.status;
      continue;
    }
    for (const f of res.filings || []) {
      const hit = client.normalizeHit(f, {
        queryFamily: step.queryFamily,
        query: step.query,
        channel: "SEC_API_IO_FULL_TEXT",
      });
      if (!hit.accessionNumber) continue;
      queryStats[step.queryFamily].hits += 1;
      queryStats[step.queryFamily].uniqueAccessions.add(hit.accessionNumber);
      if (!byAccession.has(hit.accessionNumber)) {
        byAccession.set(hit.accessionNumber, hit);
        hits.push(hit);
      }
    }
    await sleep(120);
  }

  // Serialize Set for report
  const qOut = {};
  for (const [k, v] of Object.entries(queryStats)) {
    qOut[k] = {
      queries: v.queries,
      hits: v.hits,
      uniqueAccessions: v.uniqueAccessions.size,
      lastError: v.lastError || null,
    };
  }
  return { hits, byAccession, queryStats: qOut, queriesRun };
}

async function discoverExa(client, budget, opts = {}) {
  const hits = [];
  const byUrl = new Map();
  const queryStats = {};
  let queriesRun = 0;
  const maxQueries = opts.maxQueries || 12;

  for (const fam of EXA_QUERY_FAMILIES) {
    if (queriesRun >= maxQueries) break;
    if (budget.canSpend(0.005).ok === false && budget.remaining() <= 0) break;
    const res = await client.search({
      query: fam.query,
      type: "lite",
      numResults: 8,
    });
    queriesRun += 1;
    queryStats[fam.id] = { queries: 1, hits: 0, uniqueUrls: 0, lastError: null };
    if (!res.ok) {
      queryStats[fam.id].lastError = res.error || res.status;
      continue;
    }
    for (const r of res.results || []) {
      const hit = client.normalizeHit(r, { queryFamily: fam.id, query: fam.query });
      if (!hit.url) continue;
      queryStats[fam.id].hits += 1;
      if (!byUrl.has(hit.url)) {
        byUrl.set(hit.url, hit);
        hits.push(hit);
      }
    }
    queryStats[fam.id].uniqueUrls = [...byUrl.values()].filter((h) => h.queryFamily === fam.id).length;
    await sleep(150);
  }
  return { hits, byUrl, queryStats, queriesRun };
}

async function fetchSecDocument(edgarClient, hit) {
  const url = hit.filingUrl;
  if (url && /sec\.gov/i.test(url)) {
    const res = await edgarClient.request(url, {
      accept: "text/html, application/xhtml+xml, text/plain, */*",
      maxAgeMs: 30 * 24 * 60 * 60 * 1000,
    });
    if (res.ok && res.text) {
      return { ok: true, text: res.text, url, from: "filingUrl" };
    }
  }
  // Fallback: primary document via accession if we can resolve from package index
  if (hit.cik && hit.accessionNumber) {
    const idx = await edgarClient.getFilingPackageIndex(hit.cik, hit.accessionNumber);
    if (idx.ok) {
      let items = [];
      if (idx.body?.directory?.item) items = idx.body.directory.item;
      else if (idx.text) {
        try {
          items = JSON.parse(idx.text)?.directory?.item || [];
        } catch {
          items = [];
        }
      }
      const primary =
        items.find((it) => /\.htm$/i.test(it.name || "") && /424b|10-k|ex.?10/i.test(it.name || "")) ||
        items.find((it) => /\.htm$/i.test(it.name || ""));
      if (primary?.name) {
        const doc = await edgarClient.getFilingDocument(hit.cik, hit.accessionNumber, primary.name);
        if (doc.ok && doc.text) {
          return {
            ok: true,
            text: doc.text,
            url: edgarClient.filingDocumentUrl(hit.cik, hit.accessionNumber, primary.name),
            from: "packageIndex",
            primaryDocument: primary.name,
          };
        }
      }
    }
  }
  return { ok: false, error: "FETCH_FAILED" };
}

async function persistFindings(rows, opts = {}) {
  const token = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
  if (!token) return { ok: false, error: "NO_AIRTABLE_TOKEN", counts: {} };
  const ids = loadTableIds();
  const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
  const findTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
  const persisted = [];
  const skippedDup = [];
  const errors = [];

  async function at(table, method, body, qs = "") {
    const url = `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${table}${qs}`;
    const res = await fetch(url, {
      method,
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
  }

  async function findInv(propertyId) {
    const formula = encodeURIComponent(`{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${propertyId}'`);
    const r = await at(invTable, "GET", null, `?filterByFormula=${formula}&maxRecords=1`);
    return r.json.records?.[0] || null;
  }
  async function findFinding(findingId) {
    const formula = encodeURIComponent(`{${MAP_DISCOVERY_FINDINGS.findingId}}='${findingId}'`);
    const r = await at(findTable, "GET", null, `?filterByFormula=${formula}&maxRecords=1`);
    return r.json.records?.[0] || null;
  }

  for (const row of rows.slice(0, opts.limit || 60)) {
    const propertyId =
      row.discoveryPropertyId ||
      `disc_ctim_v2_${sha1(`${row.hotelName}|${row.city}|${row.stateRegion}`).slice(0, 18)}`;
    const findingId =
      row.findingId ||
      `find_ctim_v2_${sha1(`${propertyId}|${row.dateMeaning}|${row.primaryDate}|${row.source?.accessionNumber}`).slice(0, 16)}`;

    let invRec = await findInv(propertyId);
    if (!invRec && APPLY) {
      const fields = {
        [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: propertyId,
        [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.city]: row.city || null,
        [MAP_DISCOVERY_INVENTORY.stateRegion]: row.stateRegion || null,
        [MAP_DISCOVERY_INVENTORY.country]: "United States",
        [MAP_DISCOVERY_INVENTORY.identityClass]: row.identityClass || "PROBABLE_PROPERTY",
        [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "DISCOVERY_PERSISTED",
        [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "RESEARCH_ONLY",
        [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
        [MAP_DISCOVERY_INVENTORY.secRegistrant]: row.source?.companyName || null,
        [MAP_DISCOVERY_INVENTORY.registrantCik]: row.source?.cik || null,
        [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: row.source?.documentUrl || null,
        [MAP_DISCOVERY_INVENTORY.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
        [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
        [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
        [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "HTML_PROPERTY_TABLE",
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `Contract timing V2 — ${row.dateMeaning} — ${row.discoveryProvider || ""}`,
      };
      for (const k of Object.keys(fields)) if (fields[k] == null || fields[k] === "") delete fields[k];
      const created = await at(invTable, "POST", { fields, typecast: true });
      if (!created.ok) {
        errors.push({ propertyId, err: created.json });
        continue;
      }
      invRec = created.json;
    } else if (!invRec) {
      invRec = { id: `dry_${propertyId}` };
    } else {
      skippedDup.push({ type: "inventory", propertyId });
    }

    const existing = await findFinding(findingId);
    if (existing) {
      skippedDup.push({ type: "finding", findingId });
      persisted.push({ findingId, action: "SKIP_DUP", readBackOk: true });
      await sleep(180);
      continue;
    }

    const fFields = {
      [MAP_DISCOVERY_FINDINGS.findingId]: findingId,
      [MAP_DISCOVERY_FINDINGS.discoveryProperty]:
        invRec?.id && !String(invRec.id).startsWith("dry_") ? [invRec.id] : undefined,
      [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
      [MAP_DISCOVERY_FINDINGS.fieldName]: row.dateMeaning || "Contract Timing",
      [MAP_DISCOVERY_FINDINGS.valueObserved]: String(row.primaryDate || "").slice(0, 200),
      [MAP_DISCOVERY_FINDINGS.agreementType]:
        String(row.agreementType || "").includes("FRANCHISE")
          ? "FRANCHISE"
          : String(row.agreementType || "").includes("MANAGEMENT")
            ? "MANAGEMENT"
            : "UNKNOWN_AGREEMENT",
      [MAP_DISCOVERY_FINDINGS.historicalApplicability]:
        row.historicalApplicability || "HISTORICALLY_VERIFIED",
      [MAP_DISCOVERY_FINDINGS.effectiveDate]:
        row.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(row.primaryDate)
          ? row.primaryDate.slice(0, 10)
          : undefined,
      [MAP_DISCOVERY_FINDINGS.reportingDate]: row.source?.filingDate || undefined,
      [MAP_DISCOVERY_FINDINGS.sourceUrl]: row.source?.documentUrl || null,
      [MAP_DISCOVERY_FINDINGS.secAccession]: row.source?.accessionNumber || null,
      [MAP_DISCOVERY_FINDINGS.registrantCik]: row.source?.cik || null,
      [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(row.snippet || "").slice(0, 4000),
      [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
      [MAP_DISCOVERY_FINDINGS.verificationStatus]: "HISTORICALLY_VERIFIED",
      [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
      [MAP_DISCOVERY_FINDINGS.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
      [MAP_DISCOVERY_FINDINGS.notes]: [
        "CONTRACT_TIMING_V2_PROVIDER_PILOT",
        `PROVIDER=${row.discoveryProvider || ""}`,
        `DATE_MEANING=${row.dateMeaning}`,
        `BASELINE_EXCLUDED=${isBaselineAccession(row.source?.accessionNumber)}`,
      ]
        .join(" | ")
        .slice(0, 4000),
    };
    for (const k of Object.keys(fFields)) if (fFields[k] == null || fFields[k] === "") delete fFields[k];

    if (!APPLY) {
      persisted.push({ findingId, action: "DRY_RUN", readBackOk: false });
      continue;
    }
    const created = await at(findTable, "POST", { fields: fFields, typecast: true });
    if (!created.ok) {
      errors.push({ findingId, err: created.json });
      continue;
    }
    const rb = await findFinding(findingId);
    persisted.push({
      findingId,
      action: "CREATE",
      readBackOk: Boolean(rb),
      airtableFindingId: created.json.id,
    });
    await sleep(220);
  }

  return {
    ok: errors.length === 0,
    apply: APPLY,
    persisted,
    skippedDup,
    errors,
    counts: {
      persisted: persisted.filter((p) => p.action === "CREATE").length,
      dryRun: persisted.filter((p) => p.action === "DRY_RUN").length,
      duplicatesPrevented: skippedDup.length,
      readBackOk: persisted.filter((p) => p.readBackOk).length,
      errors: errors.length,
    },
  };
}

function findingKey(f) {
  return `${String(f.hotelName || "").toLowerCase()}|${f.city}|${f.stateRegion}|${f.dateMeaning}|${f.primaryDate}`;
}

function analyzeYield(findings, docs) {
  const byDeal = new Map();
  for (const f of findings) {
    const dk = f.source?.accessionNumber || "unknown";
    if (!byDeal.has(dk)) byDeal.set(dk, []);
    byDeal.get(dk).push(f);
  }
  const dealSizes = [...byDeal.entries()]
    .map(([accession, rows]) => ({ accession, count: rows.length }))
    .sort((a, b) => b.count - a.count);
  const top = dealSizes[0] || { accession: null, count: 0 };
  const withoutTop = findings.filter((f) => f.source?.accessionNumber !== top.accession);
  const docsOk = docs.filter((d) => d.ok).length;
  return {
    totalDates: findings.length,
    uniqueHotels: new Set(findings.map((f) => `${f.hotelName}|${f.city}|${f.stateRegion}`.toLowerCase())).size,
    uniqueDeals: byDeal.size,
    uniqueIssuers: new Set(findings.map((f) => f.source?.cik).filter(Boolean)).size,
    datesPer100Docs: docsOk ? Number(((findings.length / docsOk) * 100).toFixed(2)) : 0,
    datesPer100DocsExclTopDeal: docsOk
      ? Number(((withoutTop.length / docsOk) * 100).toFixed(2))
      : 0,
    topDeal: top,
    dealConcentration: dealSizes.slice(0, 10),
    byYear: findings.reduce((acc, f) => {
      const y = String(f.primaryDate || "").slice(0, 4);
      acc[y] = (acc[y] || 0) + 1;
      return acc;
    }, {}),
    byBrand: findings.reduce((acc, f) => {
      const b = f.brandFamily || "UNKNOWN";
      acc[b] = (acc[b] || 0) + 1;
      return acc;
    }, {}),
  };
}

async function main() {
  const started = new Date().toISOString();
  const budget = createContractTimingV2Budget({
    ceilingUsd: CONTRACT_TIMING_V2_PAID_CEILING_USD,
    cwd: ROOT,
  });
  const edgar = createSecEdgarClient();
  const secApi = createSecApiIoClient({ budget });
  const exa = createExaContractTimingClient({ budget });

  const providerAuth = {
    exa: { configured: exa.config.enabled, keyLen: exa.config.apiKeyLen },
    secApiIo: { configured: secApi.config.enabled, keyLen: secApi.config.apiKeyLen },
  };

  // --- Independent discovery (no cross-contamination before scoring) ---
  const secDiscovery = secApi.config.enabled
    ? await discoverSecApiIo(secApi, budget, {
        maxQueries: STAGE_ARG === "calibration" ? 12 : 30,
        priorityMax: STAGE_ARG === "calibration" ? 2 : 3,
      })
    : { hits: [], queryStats: {}, queriesRun: 0, error: "SEC_API_IO_KEY_ABSENT" };

  const exaDiscovery = exa.config.enabled
    ? await discoverExa(exa, budget, {
        maxQueries: STAGE_ARG === "calibration" ? 4 : 10,
      })
    : { hits: [], queryStats: {}, queriesRun: 0, error: "EXA_API_KEY_ABSENT" };

  // SEC-API document queue (authoritative for extraction)
  const secQueue = [...secDiscovery.hits].filter((h) => h.filingUrl || (h.cik && h.accessionNumber));
  // Prefer non-baseline first for incremental measurement, but still process baseline as regression
  secQueue.sort((a, b) => {
    const ab = isBaselineAccession(a.accessionNumber) ? 1 : 0;
    const bb = isBaselineAccession(b.accessionNumber) ? 1 : 0;
    return ab - bb;
  });
  // Force-include baseline accession for positive regression (not counted in incremental yield)
  if (!secQueue.some((h) => isBaselineAccession(h.accessionNumber))) {
    secQueue.push({
      provider: "SEC_API_IO",
      accessionNumber: BASELINE_JPMCC_ACCESSION,
      cik: "0001013611",
      companyName: "J.P. Morgan Chase Commercial Mortgage Securities Corp.",
      formType: "424B2",
      filedAt: "2020-03-24",
      filingUrl:
        "https://www.sec.gov/Archives/edgar/data/0001013611/000153949720000579/n2032_x17-424b2.htm",
      queryFamily: "baseline_regression",
      discoveryChannel: "BASELINE_FORCED",
    });
  }

  const docsResearched = [];
  const findingsByProvider = { SEC_API_IO: [], EXA: [], FREE_SEC: [], COMBINED: [] };
  const loanAll = [];
  let baselineRegressionDates = 0;

  const targetDocs = Math.min(DOC_CAP, secQueue.length || DOC_CAP);
  const queueForFetch = [
    ...secQueue.filter((h) => !isBaselineAccession(h.accessionNumber)).slice(0, targetDocs),
    ...secQueue.filter((h) => isBaselineAccession(h.accessionNumber)).slice(0, 1),
  ];
  for (const hit of queueForFetch) {
    const fetched = await fetchSecDocument(edgar, hit);
    const meta = {
      cik: hit.cik,
      accessionNumber: hit.accessionNumber,
      form: hit.formType,
      companyName: hit.companyName,
      filingDate: hit.filedAt ? String(hit.filedAt).slice(0, 10) : null,
      documentUrl: fetched.url || hit.filingUrl,
      sourceFamily: inferSourceFamily(hit.formType, hit.filingUrl),
    };
    if (!fetched.ok) {
      docsResearched.push({
        provider: "SEC_API_IO",
        ok: false,
        accessionNumber: hit.accessionNumber,
        error: fetched.error,
        queryFamily: hit.queryFamily,
        isBaseline: isBaselineAccession(hit.accessionNumber),
      });
      continue;
    }
    budget.record({
      provider: "FREE_SEC",
      operation: "document_fetch",
      usd: 0,
      note: hit.accessionNumber,
    });
    const extracted = extractFromDocument(fetched.text, meta);
    const tagged = extracted.hotelContractFindings.map((f) => ({
      ...f,
      discoveryProvider: "SEC_API_IO",
      queryFamily: hit.queryFamily,
    }));
    if (isBaselineAccession(hit.accessionNumber)) {
      baselineRegressionDates += tagged.length;
    } else {
      findingsByProvider.SEC_API_IO.push(...tagged);
    }
    findingsByProvider.COMBINED.push(...tagged);
    loanAll.push(...extracted.loanMaturityFindings);
    docsResearched.push({
      provider: "SEC_API_IO",
      ok: true,
      accessionNumber: hit.accessionNumber,
      cik: hit.cik,
      companyName: hit.companyName,
      formType: hit.formType,
      filedAt: hit.filedAt,
      url: fetched.url || hit.filingUrl,
      queryFamily: hit.queryFamily,
      bytes: fetched.text.length,
      hotelContractDates: tagged.length,
      loanMaturities: extracted.loanMaturityFindings.length,
      isBaseline: isBaselineAccession(hit.accessionNumber),
      dealKey: dealKey(hit),
    });
  }

  // Exa leads: optionally fetch sec.gov URLs only (authoritative); other URLs stay leads
  let exaLeads = 0;
  let exaSecDocs = 0;
  for (const hit of (exaDiscovery.hits || []).slice(0, 40)) {
    exaLeads += 1;
    if (!hit.url || !/sec\.gov/i.test(hit.url)) continue;
    if (docsResearched.some((d) => d.url === hit.url)) continue;
    if (docsResearched.filter((d) => d.ok).length >= DOC_CAP) break;
    const res = await edgar.request(hit.url, {
      accept: "text/html, text/plain, */*",
      maxAgeMs: 30 * 24 * 60 * 60 * 1000,
    });
    if (!res.ok || !res.text) continue;
    exaSecDocs += 1;
    const cikMatch = hit.url.match(/data\/(\d+)\//);
    const meta = {
      cik: cikMatch ? cikMatch[1].padStart(10, "0") : null,
      accessionNumber: parseAccessionFromUrl(hit.url),
      form: /ex.?10|dex10/i.test(hit.url) ? "EX-10" : null,
      companyName: null,
      filingDate: hit.publishedDate ? String(hit.publishedDate).slice(0, 10) : null,
      documentUrl: hit.url,
      sourceFamily: inferSourceFamily(
        /ex.?10|dex10/i.test(hit.url) ? "EX-10" : null,
        hit.url,
      ),
    };
    if (isBaselineAccession(meta.accessionNumber)) continue;
    const extracted = extractFromDocument(res.text, meta);
    const tagged = extracted.hotelContractFindings.map((f) => ({
      ...f,
      discoveryProvider: "EXA",
      queryFamily: hit.queryFamily,
    }));
    findingsByProvider.EXA.push(...tagged);
    findingsByProvider.COMBINED.push(...tagged);
    docsResearched.push({
      provider: "EXA",
      ok: true,
      accessionNumber: meta.accessionNumber,
      url: hit.url,
      queryFamily: hit.queryFamily,
      bytes: res.text.length,
      hotelContractDates: tagged.length,
      isBaseline: false,
      leadTitle: hit.title,
    });
  }

  // Deduplicate incremental findings (exclude baseline accession)
  function dedupe(rows) {
    const m = new Map();
    let dups = 0;
    for (const f of rows) {
      if (isBaselineAccession(f.source?.accessionNumber)) continue;
      const k = findingKey(f);
      if (m.has(k)) {
        dups += 1;
        continue;
      }
      m.set(k, f);
    }
    return { unique: [...m.values()], dups };
  }

  const secDedup = dedupe(findingsByProvider.SEC_API_IO);
  const exaDedup = dedupe(findingsByProvider.EXA);
  const combinedDedup = dedupe(findingsByProvider.COMBINED);

  // Independent incremental: Exa-only keys not in SEC-API set
  const secKeys = new Set(secDedup.unique.map(findingKey));
  const exaIncremental = exaDedup.unique.filter((f) => !secKeys.has(findingKey(f)));

  const now = started;
  const verifySec = verifyContractTimingFindings(secDedup.unique, { now });
  const verifyExa = verifyContractTimingFindings(exaDedup.unique, { now });
  const verifyCombined = verifyContractTimingFindings(combinedDedup.unique, { now });

  const persistSet = [...verifyCombined.verified, ...verifyCombined.probable].slice(0, 80);
  const persistence = await persistFindings(
    persistSet.map((r) => ({
      ...r,
      discoveryProvider: r.discoveryProvider || "COMBINED",
    })),
    { limit: 60 },
  );

  const docsOk = docsResearched.filter((d) => d.ok && !d.isBaseline);
  const docsOkAll = docsResearched.filter((d) => d.ok);
  const yieldSec = analyzeYield(secDedup.unique, docsOkAll.filter((d) => d.provider === "SEC_API_IO"));
  const yieldExa = analyzeYield(exaDedup.unique, docsOkAll.filter((d) => d.provider === "EXA"));
  const yieldCombined = analyzeYield(combinedDedup.unique, docsOk);

  const spent = budget.snapshot();
  const newDates = combinedDedup.unique.length;
  const costPerDate = newDates > 0 ? Number((spent.spentUsd / newDates).toFixed(4)) : null;

  const examples = [...verifyCombined.verified, ...verifyCombined.probable]
    .filter((f) => !isBaselineAccession(f.source?.accessionNumber))
    .slice(0, 8)
    .map((f) => ({
      hotelName: f.hotelName,
      city: f.city,
      stateRegion: f.stateRegion,
      brandFamily: f.brandFamily,
      dateMeaning: f.dateMeaning,
      primaryDate: f.primaryDate,
      agreementType: f.agreementType,
      accession: f.source?.accessionNumber,
      company: f.source?.companyName,
      url: f.source?.documentUrl,
      provider: f.discoveryProvider,
      snippet: String(f.snippet || "").slice(0, 180),
      horizon: f.horizon,
    }));

  const report = {
    module: "HOTEL_CONTRACT_TIMING_INTELLIGENCE_V2",
    version: "2.0.0",
    started,
    finished: new Date().toISOString(),
    stage: STAGE_ARG,
    docCap: DOC_CAP,
    mode: APPLY ? "APPLY" : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    mergeStatus: "DO_NOT_MERGE_WITHOUT_IC",
    providerAuth,
    baseline: {
      accession: BASELINE_JPMCC_ACCESSION,
      knownDates: BASELINE_JPMCC_DATE_COUNT,
      regressionDatesObserved: baselineRegressionDates,
      excludedFromIncrementalYield: true,
    },
    budget: {
      ceilingUsd: CONTRACT_TIMING_V2_PAID_CEILING_USD,
      spentUsd: spent.spentUsd,
      remainingUsd: spent.remaining,
      byProvider: spent.byProvider,
      requestCounts: spent.requestCounts,
      stoppedAtBudget: spent.stoppedAtBudget,
      note: "SEC-API.io billed as subscription (call count tracked; $0 incremental unless download overage). Exa charged from costDollars/reserves.",
    },
    discovery: {
      secApiIo: {
        queriesRun: secDiscovery.queriesRun,
        uniqueHits: secDiscovery.hits?.length || 0,
        queryStats: secDiscovery.queryStats,
        error: secDiscovery.error || null,
      },
      exa: {
        queriesRun: exaDiscovery.queriesRun,
        uniqueHits: exaDiscovery.hits?.length || 0,
        queryStats: exaDiscovery.queryStats,
        leads: exaLeads,
        secGovDocsFetched: exaSecDocs,
        error: exaDiscovery.error || null,
      },
    },
    documents: {
      researched: docsResearched.length,
      ok: docsOkAll.length,
      okExclBaseline: docsOk.length,
      byProvider: docsResearched.reduce((a, d) => {
        a[d.provider] = a[d.provider] || { total: 0, ok: 0 };
        a[d.provider].total += 1;
        if (d.ok) a[d.provider].ok += 1;
        return a;
      }, {}),
      detail: docsResearched,
    },
    incrementalYield: {
      note: "Excludes baseline JPMCC accession dates",
      secApiIo: {
        uniqueDates: secDedup.unique.length,
        dups: secDedup.dups,
        verification: verifySec.counts,
        yield: yieldSec,
      },
      exa: {
        uniqueDates: exaDedup.unique.length,
        dups: exaDedup.dups,
        incrementalVsSecApi: exaIncremental.length,
        verification: verifyExa.counts,
        yield: yieldExa,
      },
      combined: {
        uniqueDates: combinedDedup.unique.length,
        dups: combinedDedup.dups,
        verification: verifyCombined.counts,
        yield: yieldCombined,
      },
      horizons: {
        next6m: verifyCombined.counts.horizon6,
        next12m: verifyCombined.counts.horizon12,
        next24m: verifyCombined.counts.horizon24,
        next36m: verifyCombined.counts.horizon36,
        historicalNeedRevalidation: verifyCombined.counts.historicalNeedRevalidation,
      },
      agreementSplit: {
        franchise: verifyCombined.counts.franchise,
        management: verifyCombined.counts.management,
        terminationRights: verifyCombined.counts.terminationRights,
      },
      costPerNewVerifiedDateUsd: costPerDate,
    },
    persistence: persistence.counts,
    examples,
    scalability: {
      assessment:
        newDates >= 20
          ? "PROMISING — SEC-API.io full-text discovery finds additional Franchise Expiration schedules beyond the baseline deal; Exa contributes sec.gov leads and non-SEC context."
          : newDates > 0
            ? "MODERATE — incremental dates found; expand period/form partitions and high-yield CMBS issuers."
            : "LIMITED_IN_THIS_WAVE — providers authenticated but few new extractable hotel-level dates in completed sample.",
      recommendedMix:
        "Primary: SEC-API.io full-text for CMBS 424B Franchise Expiration columns → free SEC document fetch → existing HTML extractor. Secondary: Exa for non-SEC renewals/reflags (leads until verified). Keep free EDGAR for retrieval.",
    },
    clientStats: {
      secApiIo: secApi.stats(),
      exa: exa.stats(),
      edgar: edgar.stats(),
    },
  };

  fs.writeFileSync(path.join(OUT_DIR, "V2_PILOT_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "V2_FINDINGS_INCREMENTAL.json"),
    JSON.stringify(
      {
        secApiIo: secDedup.unique,
        exa: exaDedup.unique,
        exaIncrementalVsSecApi: exaIncremental,
        combined: combinedDedup.unique,
      },
      null,
      2,
    ),
  );

  const md = `# Hotel Contract Timing Intelligence V2 — Provider Pilot

**Stage:** ${STAGE_ARG} · **Mode:** ${report.mode} · **Publication:** CUSTOMER_BLOCKED  
**Ceiling:** $${CONTRACT_TIMING_V2_PAID_CEILING_USD} · **Spent:** $${spent.spentUsd} (Exa paid; SEC-API.io subscription calls=${spent.requestCounts.SEC_API_IO || 0})

## Provider auth
- Exa: ${providerAuth.exa.configured ? "OK" : "ABSENT"}
- SEC-API.io: ${providerAuth.secApiIo.configured ? "OK" : "ABSENT"}

## Baseline regression
- JPMCC \`${BASELINE_JPMCC_ACCESSION}\` known ${BASELINE_JPMCC_DATE_COUNT} dates · observed this run: **${baselineRegressionDates}**
- Excluded from incremental yield: **yes**

## Incremental yield (new unique dates)

| Provider | Unique dates | Dates/100 docs | Excl. top deal /100 |
|---|---:|---:|---:|
| SEC-API.io | ${secDedup.unique.length} | ${yieldSec.datesPer100Docs} | ${yieldSec.datesPer100DocsExclTopDeal} |
| Exa | ${exaDedup.unique.length} | ${yieldExa.datesPer100Docs} | ${yieldExa.datesPer100DocsExclTopDeal} |
| Combined (deduped) | ${combinedDedup.unique.length} | ${yieldCombined.datesPer100Docs} | ${yieldCombined.datesPer100DocsExclTopDeal} |
| Exa incremental vs SEC-API | ${exaIncremental.length} | — | — |

- Documents OK (excl. baseline): **${docsOk.length}**
- Horizons 6/12/24/36m: ${verifyCombined.counts.horizon6} / ${verifyCombined.counts.horizon12} / ${verifyCombined.counts.horizon24} / ${verifyCombined.counts.horizon36}
- Historical needing revalidation: **${verifyCombined.counts.historicalNeedRevalidation}**
- Cost per new date: **${costPerDate == null ? "n/a" : "$" + costPerDate}**

## Persistence
- Created: ${persistence.counts.persisted} · Read-back OK: ${persistence.counts.readBackOk} · Dupes prevented: ${persistence.counts.duplicatesPrevented}

## Scalability
${report.scalability.assessment}

Recommended: ${report.scalability.recommendedMix}

## Examples
${examples
  .slice(0, 5)
  .map(
    (e, i) =>
      `${i + 1}. **${e.hotelName}** — ${e.dateMeaning} ${e.primaryDate} · ${e.company || e.accession || ""} · ${e.provider}`,
  )
  .join("\n") || "_No incremental examples in this wave_"}
`;
  fs.writeFileSync(path.join(OUT_DIR, "V2_FOUNDER_REPORT.md"), md);

  console.log(
    JSON.stringify(
      {
        stage: STAGE_ARG,
        mode: report.mode,
        spentUsd: spent.spentUsd,
        docsOk: docsOkAll.length,
        incrementalDates: newDates,
        secApiDates: secDedup.unique.length,
        exaDates: exaDedup.unique.length,
        exaIncremental: exaIncremental.length,
        baselineRegression: baselineRegressionDates,
        persistence: persistence.counts,
        outDir: OUT_DIR,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
