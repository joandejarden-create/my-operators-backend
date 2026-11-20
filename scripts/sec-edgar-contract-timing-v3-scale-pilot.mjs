#!/usr/bin/env node
/**
 * Contract Timing V3 — SEC-API.io scale expansion with adaptive batches.
 *
 *   node scripts/sec-edgar-contract-timing-v3-scale-pilot.mjs
 *   node scripts/sec-edgar-contract-timing-v3-scale-pilot.mjs --batches=3 --batch-size=100
 *   node scripts/sec-edgar-contract-timing-v3-scale-pilot.mjs --apply
 *
 * Excludes JPMCC baseline 57 + V2's 19 from "new vs prior waves".
 * Checkpointed; CUSTOMER_BLOCKED; no HPC auto-create; $25 paid ceiling retained.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { createContractTimingV2Budget, CONTRACT_TIMING_V2_PAID_CEILING_USD } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createSecApiIoClient } from "../lib/hotel-intelligence/sec-edgar/sec-api-io-client-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import {
  listV3SecApiQueryPlan,
  adaptWeightsFromBatch,
  isPriorWaveAccession,
  V2_KNOWN_ACCESSIONS,
  AGREEMENT_DISCOVERY_FAMILY,
  V3_CMBS_ISSUER_CIKS,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-query-plan-v1.js";
import {
  loadV3Checkpoint,
  saveV3Checkpoint,
  markAccessionProcessed,
  markFindingKey,
  isAccessionProcessed,
  isFindingSeen,
  appendBatchSummary,
  defaultCheckpointPath,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-checkpoint-v1.js";
import {
  classifyContractCurrentStatus,
  isCommerciallyFutureWindow,
  CONTRACT_CURRENT_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v3-status-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { bridgeFranchiseScheduleToContractTiming } from "../lib/hotel-intelligence/sec-edgar/contract-timing-franchise-bridge-v1.js";
import { extractEx10AgreementTerms } from "../lib/hotel-intelligence/sec-edgar/ex10-agreement-term-extractor-v1.js";
import { verifyContractTimingFindings } from "../lib/hotel-intelligence/sec-edgar/contract-timing-verifier-v1.js";
import { SOURCE_FAMILY, DATE_MEANING } from "../lib/hotel-intelligence/sec-edgar/contract-timing-types-v1.js";
import { BASELINE_JPMCC_ACCESSION } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-query-families-v1.js";
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
for (const p of [
  "C:/Dev/dealality/my-operators-backend/.env",
  path.join(ROOT, ".env"),
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const APPLY = process.argv.includes("--apply");
const BATCH_SIZE = Math.max(
  20,
  Number(process.argv.find((a) => a.startsWith("--batch-size="))?.split("=")[1] || 100),
);
const MAX_BATCHES = Math.max(
  1,
  Number(process.argv.find((a) => a.startsWith("--batches="))?.split("=")[1] || 5),
);
const TARGET_DOCS = Math.min(1000, BATCH_SIZE * MAX_BATCHES);
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-contract-timing-v3");
fs.mkdirSync(OUT_DIR, { recursive: true });

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

function loadV2ExclusionKeys() {
  const p = path.join(ROOT, "reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json");
  const keys = new Set();
  if (!fs.existsSync(p)) return keys;
  try {
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    for (const f of j.combined || []) {
      keys.add(findingKey(f));
    }
  } catch {
    /* ignore */
  }
  return keys;
}

function findingKey(f) {
  return [
    String(f.hotelName || "").toLowerCase(),
    f.city,
    f.stateRegion,
    f.dateMeaning,
    f.primaryDate,
  ].join("|");
}

function findingKeyWithAcc(f) {
  return `${findingKey(f)}|${f.source?.accessionNumber || ""}`;
}

function looksLikeYearBuiltCollateral(snippet) {
  const s = String(snippet || "");
  return /Hospitality\s*\|\s*(Full|Limited|Select)\s+Service\s*\|\s*\d{4}/i.test(s);
}

function inferSourceFamily(formType, url) {
  const f = String(formType || "").toUpperCase();
  const u = String(url || "").toLowerCase();
  if (f.startsWith("424B") || /424b/.test(u)) return SOURCE_FAMILY.CMBS_424B;
  if (/ex-?10|exhibit.?10/i.test(f) || /ex10|ex-10|dex10/.test(u)) return SOURCE_FAMILY.MANAGEMENT_EXHIBIT_EX10;
  if (f === "10-K" || f === "10-K/A" || f.startsWith("S-11")) return SOURCE_FAMILY.HOTEL_REIT_SCHEDULE;
  return SOURCE_FAMILY.OTHER_SEC;
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
    /ex-?10|dex10/i.test(String(meta.documentUrl || meta.form || ""))
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
        hotelName: String(f.hotelName || "").replace(/&ndash;/g, "–").replace(/&amp;/g, "&"),
      });
    }
  }
  return {
    hotelContractFindings: [...byId.values()],
    loanMaturityFindings: cmbs.loanMaturityFindings || [],
  };
}

async function fetchSecDocument(edgarClient, hit) {
  const url = hit.filingUrl;
  // Prefer direct prospectus/annex HTML when SEC-API already points at it
  if (url && /sec\.gov/i.test(url) && /\.htm/i.test(url)) {
    const res = await edgarClient.request(url, {
      accept: "text/html, application/xhtml+xml, text/plain, */*",
      maxAgeMs: 30 * 24 * 60 * 60 * 1000,
    });
    if (res.ok && res.text && res.text.length > 5000) {
      return { ok: true, text: res.text, url, from: "filingUrl" };
    }
  }
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
      const ranked = [...items]
        .filter((it) => /\.htm$/i.test(it.name || ""))
        .sort((a, b) => {
          const score = (n) => {
            const s = String(n || "").toLowerCase();
            let sc = 0;
            if (/424b/.test(s)) sc += 50;
            if (/franchise|annex|schedule|x\d+/i.test(s)) sc += 20;
            if (/ex.?10|dex10/.test(s)) sc += 15;
            if (/10-k/.test(s)) sc += 8;
            return sc;
          };
          return score(b.name) - score(a.name);
        });
      for (const primary of ranked.slice(0, 3)) {
        const doc = await edgarClient.getFilingDocument(hit.cik, hit.accessionNumber, primary.name);
        if (doc.ok && doc.text && doc.text.length > 5000) {
          return {
            ok: true,
            text: doc.text,
            url: edgarClient.filingDocumentUrl(hit.cik, hit.accessionNumber, primary.name),
            from: "packageIndex",
          };
        }
      }
    }
  }
  // Last resort: filingUrl even if short
  if (url && /sec\.gov/i.test(url)) {
    const res = await edgarClient.request(url, {
      accept: "text/html, application/xhtml+xml, text/plain, */*",
      maxAgeMs: 30 * 24 * 60 * 60 * 1000,
    });
    if (res.ok && res.text) return { ok: true, text: res.text, url, from: "filingUrl_short" };
  }
  return { ok: false, error: "FETCH_FAILED" };
}

const HIGH_PRECISION_QUERY_FAMILIES = new Set([
  "franchise_expiration_date",
  "franchise_agreement_expiration_date",
  "management_agreement_expiration",
  "license_agreement_expiration",
  "hotel_collateral_schedule",
  "mortgaged_property_franchise_agreement",
]);

const CMBS_FORM_TYPES = ["424B2", "424B5", "424B3", "424B4", "424H"];

function hitPriorityScore(hit) {
  const form = String(hit.formType || "").toUpperCase();
  let score = 0;
  if (/^424B/.test(form) || form === "424H") score += 100;
  else if (form === "S-11" || form === "S-11/A") score += 40;
  else if (form === "10-K" || form === "10-K/A") score += 15;
  else if (form === "8-K" || form === "8-K/A") score += 5;
  else if (/ABS-15G|10-D|FWP/i.test(form)) score -= 40;
  if (HIGH_PRECISION_QUERY_FAMILIES.has(hit.queryFamily)) score += 35;
  if (hit.agreementFamily === AGREEMENT_DISCOVERY_FAMILY.FRANCHISE) score += 10;
  if (hit.agreementFamily === AGREEMENT_DISCOVERY_FAMILY.MANAGEMENT) score += 6;
  // Prefer older filing years that still disclose long-dated expirations
  const y = Number(String(hit.filedAt || "").slice(0, 4));
  if (y >= 2015 && y <= 2022) score += 8;
  return score;
}

function rankDiscoveryQueue(hits) {
  return [...hits].sort((a, b) => {
    const d = hitPriorityScore(b) - hitPriorityScore(a);
    if (d !== 0) return d;
    return String(b.filedAt || "").localeCompare(String(a.filedAt || ""));
  });
}

/**
 * Build a 424B-first discovery queue. Paginate high-precision families.
 * Exploration quota for EX-10 / 8-K is retained but ranked below CMBS prospectuses.
 */
async function discoverHits(secApi, budget, checkpoint, weights) {
  const plan = listV3SecApiQueryPlan({ weights });
  const byAccession = new Map();
  const queryStats = {};
  let queriesRun = 0;
  const maxQueries = 72;

  function recordHit(step, f) {
    const hit = secApi.normalizeHit(f, {
      queryFamily: step.queryFamily,
      query: step.query,
      channel: "SEC_API_IO_FULL_TEXT",
    });
    hit.agreementFamily = step.agreementFamily;
    if (!hit.accessionNumber) return;
    if (isAccessionProcessed(checkpoint, hit.accessionNumber)) return;
    if (isPriorWaveAccession(hit.accessionNumber)) return;
    const form = String(hit.formType || "").toUpperCase();
    // Drop known low-precision CMBS servicing noise unless exact franchise column phrase
    if (
      /ABS-15G|10-D|^FWP$/i.test(form) &&
      step.queryFamily !== "franchise_expiration_date" &&
      step.queryFamily !== "franchise_agreement_expiration_date"
    ) {
      return;
    }
    queryStats[step.queryFamily].hits += 1;
    const prev = byAccession.get(hit.accessionNumber);
    if (!prev || hitPriorityScore(hit) > hitPriorityScore(prev)) {
      byAccession.set(hit.accessionNumber, hit);
    }
  }

  for (const step of plan) {
    if (queriesRun >= maxQueries) break;
    const isHigh = HIGH_PRECISION_QUERY_FAMILIES.has(step.queryFamily);
    // Force CMBS prospectus forms for franchise/management expiration phrases
    let formTypes = step.formTypes?.slice(0, 6);
    if (
      step.queryFamily === "franchise_expiration_date" ||
      step.queryFamily === "franchise_agreement_expiration_date" ||
      step.queryFamily === "hotel_collateral_schedule" ||
      step.queryFamily === "mortgaged_property_franchise_agreement"
    ) {
      formTypes = CMBS_FORM_TYPES;
    }
    const pages = isHigh ? ["1", "2"] : ["1"];
    for (const page of pages) {
      if (queriesRun >= maxQueries) break;
      const res = await secApi.fullTextSearch({
        query: step.query,
        formTypes,
        startDate: step.startDate,
        endDate: step.endDate,
        page,
      });
      queriesRun += 1;
      queryStats[step.queryFamily] = queryStats[step.queryFamily] || {
        queries: 0,
        hits: 0,
        agreementFamily: step.agreementFamily,
      };
      queryStats[step.queryFamily].queries += 1;
      if (!res.ok) {
        queryStats[step.queryFamily].lastError = res.error || res.status;
        continue;
      }
      for (const f of res.filings || []) recordHit(step, f);
      await sleep(80);
      if ((res.filings || []).length < 100) break;
    }
  }

  // Extra CIK-partitioned franchise searches for independent CMBS issuers
  for (const issuer of V3_CMBS_ISSUER_CIKS) {
    if (queriesRun >= maxQueries) break;
    const step = {
      queryFamily: "franchise_expiration_date",
      agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
      query: '"Franchise Expiration Date" OR "Franchise Agreement Expiration"',
    };
    const res = await secApi.fullTextSearch({
      query: step.query,
      formTypes: CMBS_FORM_TYPES,
      ciks: [issuer.cik.replace(/^0+/, "")],
      startDate: "2012-01-01",
      endDate: "2026-12-31",
      page: "1",
    });
    queriesRun += 1;
    queryStats.cik_partitioned = queryStats.cik_partitioned || {
      queries: 0,
      hits: 0,
      agreementFamily: AGREEMENT_DISCOVERY_FAMILY.FRANCHISE,
    };
    queryStats.cik_partitioned.queries += 1;
    if (res.ok) {
      for (const f of res.filings || []) recordHit(step, f);
      queryStats.cik_partitioned.hits += (res.filings || []).length;
    }
    await sleep(80);
  }

  return { hits: rankDiscoveryQueue([...byAccession.values()]), queryStats, queriesRun };
}

/**
 * Lightweight current-status probe: later SEC full-text for hotel name + renewal/amendment/termination.
 * Does not invent continuity from silence.
 */
async function probeLaterStatus(secApi, finding, budget) {
  const name = String(finding.hotelName || "").trim();
  if (!name || name.length < 8) return { laterEvidence: [], queries: 0 };
  if (budget.remaining() < 0) return { laterEvidence: [], queries: 0 };
  const q = `"${name.replace(/"/g, "")}" (renewal OR amendment OR terminated OR reflag OR "management agreement" OR "franchise agreement")`;
  const filedAfter = finding.source?.filingDate
    ? String(finding.source.filingDate).slice(0, 10)
    : "2018-01-01";
  const res = await secApi.fullTextSearch({
    query: q,
    startDate: filedAfter,
    endDate: "2026-12-31",
    page: "1",
  });
  const laterEvidence = [];
  if (res.ok) {
    for (const f of (res.filings || []).slice(0, 5)) {
      if (f.accessionNo === finding.source?.accessionNumber) continue;
      const blob = `${f.description || ""} ${f.type || ""}`.toLowerCase();
      let kind = "LATER_DISCLOSURE";
      if (/terminat|reflag|conversion/.test(blob)) kind = "TERMINATION_OR_REFLAG";
      if (/amend|renew|extend/.test(blob)) kind = "AMENDMENT_OR_RENEWAL";
      laterEvidence.push({
        kind,
        accessionNumber: f.accessionNo,
        filedAt: f.filedAt,
        filingUrl: f.filingUrl,
        snippet: String(f.description || "").slice(0, 200),
      });
    }
  }
  return { laterEvidence, queries: 1, ok: res.ok };
}

async function selectiveExaStatus(exa, finding, budget) {
  // Only for future commercially actionable windows
  if (!isCommerciallyFutureWindow(finding.primaryDate)) return { laterEvidence: [], queries: 0 };
  if (!budget.canSpend(0.005).ok) return { laterEvidence: [], queries: 0 };
  const q = `${finding.hotelName} franchise OR management agreement renewal OR expiration OR reflag`;
  const res = await exa.search({ query: q, type: "lite", numResults: 3 });
  if (!res.ok) return { laterEvidence: [], queries: 1 };
  return {
    queries: 1,
    laterEvidence: (res.results || []).map((r) => ({
      kind: /renew|extend|amend/i.test(`${r.title} ${r.text || ""}`)
        ? "AMENDMENT_OR_RENEWAL"
        : /terminat|reflag|convert/i.test(`${r.title} ${r.text || ""}`)
          ? "TERMINATION_OR_REFLAG"
          : "NON_SEC_LEAD",
      url: r.url,
      snippet: String(r.title || "").slice(0, 180),
      leadOnly: true,
    })),
  };
}

async function persistFindings(rows) {
  const token = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
  if (!token) return { ok: false, error: "NO_AIRTABLE_TOKEN", counts: {} };
  const ids = loadTableIds();
  const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
  const findTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
  const persisted = [];
  const skippedDup = [];
  const errors = [];

  async function at(table, method, body, qs = "") {
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${table}${qs}`, {
      method,
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    return { ok: res.ok, json: await res.json().catch(() => ({})) };
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

  for (const row of rows.slice(0, 80)) {
    const propertyId = `disc_ctim_v3_${sha1(`${row.hotelName}|${row.city}|${row.stateRegion}`).slice(0, 18)}`;
    const findingId =
      row.findingId ||
      `find_ctim_v3_${sha1(`${propertyId}|${row.dateMeaning}|${row.primaryDate}|${row.source?.accessionNumber}`).slice(0, 16)}`;

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
        [MAP_DISCOVERY_INVENTORY.discoveryMethod]: row.discoveryMethod || "HTML_PROPERTY_TABLE",
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `Contract timing V3 — ${row.dateMeaning} — ${row.currentStatus || ""}`,
      };
      for (const k of Object.keys(fields)) if (fields[k] == null || fields[k] === "") delete fields[k];
      const created = await at(invTable, "POST", { fields, typecast: true });
      if (!created.ok) {
        errors.push(created.json);
        continue;
      }
      invRec = created.json;
    } else if (!invRec) invRec = { id: `dry_${propertyId}` };
    else skippedDup.push({ type: "inventory", propertyId });

    const existing = await findFinding(findingId);
    if (existing) {
      skippedDup.push({ type: "finding", findingId });
      persisted.push({ findingId, action: "SKIP_DUP", readBackOk: true });
      await sleep(160);
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
            : String(row.agreementType || "").includes("LEASE")
              ? "LEASE"
              : "UNKNOWN_AGREEMENT",
      [MAP_DISCOVERY_FINDINGS.historicalApplicability]:
        row.currentStatus === CONTRACT_CURRENT_STATUS.CURRENTLY_CORROBORATED
          ? "CURRENTLY_VERIFIED"
          : row.currentStatus === CONTRACT_CURRENT_STATUS.SUPERSEDED
            ? "SUPERSEDED"
            : row.currentStatus === CONTRACT_CURRENT_STATUS.EXPIRED_STATUS_UNCONFIRMED
              ? "CURRENT_STATUS_UNCONFIRMED"
              : "HISTORICALLY_VERIFIED",
      [MAP_DISCOVERY_FINDINGS.effectiveDate]:
        row.primaryDate && /^\d{4}-\d{2}-\d{2}/.test(row.primaryDate)
          ? row.primaryDate.slice(0, 10)
          : undefined,
      [MAP_DISCOVERY_FINDINGS.sourceUrl]: row.source?.documentUrl || null,
      [MAP_DISCOVERY_FINDINGS.secAccession]: row.source?.accessionNumber || null,
      [MAP_DISCOVERY_FINDINGS.registrantCik]: row.source?.cik || null,
      [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(row.snippet || "").slice(0, 4000),
      [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
      [MAP_DISCOVERY_FINDINGS.verificationStatus]: "HISTORICALLY_VERIFIED",
      [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
      [MAP_DISCOVERY_FINDINGS.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
      [MAP_DISCOVERY_FINDINGS.notes]: [
        "CONTRACT_TIMING_V3_SCALE",
        `STATUS=${row.currentStatus || ""}`,
        `AGREEMENT_FAMILY=${row.agreementFamily || ""}`,
        `FUTURE_2027_2035=${isCommerciallyFutureWindow(row.primaryDate)}`,
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
      errors.push(created.json);
      continue;
    }
    const rb = await findFinding(findingId);
    persisted.push({ findingId, action: "CREATE", readBackOk: Boolean(rb) });
    await sleep(200);
  }

  return {
    counts: {
      persisted: persisted.filter((p) => p.action === "CREATE").length,
      dryRun: persisted.filter((p) => p.action === "DRY_RUN").length,
      duplicatesPrevented: skippedDup.length,
      readBackOk: persisted.filter((p) => p.readBackOk).length,
      errors: errors.length,
    },
  };
}

function summarizeFindings(rows) {
  const byDeal = new Map();
  const byAgreement = {};
  const byBrand = {};
  const byYear = {};
  for (const f of rows) {
    const acc = f.source?.accessionNumber || "unknown";
    if (!byDeal.has(acc)) byDeal.set(acc, 0);
    byDeal.set(acc, byDeal.get(acc) + 1);
    const ag = f.agreementFamily || f.dateMeaning || "UNKNOWN";
    byAgreement[ag] = (byAgreement[ag] || 0) + 1;
    const b = f.brandFamily || "UNKNOWN";
    byBrand[b] = (byBrand[b] || 0) + 1;
    const y = String(f.primaryDate || "").slice(0, 4);
    byYear[y] = (byYear[y] || 0) + 1;
  }
  const dealSizes = [...byDeal.entries()]
    .map(([accession, count]) => ({ accession, count }))
    .sort((a, b) => b.count - a.count);
  const top = dealSizes[0] || { accession: null, count: 0 };
  const exclTop = rows.filter((f) => f.source?.accessionNumber !== top.accession);
  return {
    uniqueDates: rows.length,
    uniqueHotels: new Set(rows.map((f) => `${f.hotelName}|${f.city}|${f.stateRegion}`.toLowerCase())).size,
    uniqueDeals: byDeal.size,
    uniqueIssuers: new Set(rows.map((f) => f.source?.cik).filter(Boolean)).size,
    topDeal: top,
    dealConcentration: dealSizes.slice(0, 12),
    byAgreement,
    byBrand,
    byYear,
    future2027_2035: rows.filter((f) => isCommerciallyFutureWindow(f.primaryDate)).length,
    exclTopCount: exclTop.length,
  };
}

async function main() {
  const started = new Date().toISOString();
  const budget = createContractTimingV2Budget({
    ceilingUsd: CONTRACT_TIMING_V2_PAID_CEILING_USD,
    cwd: ROOT,
  });
  const checkpointPath = defaultCheckpointPath(ROOT);
  let checkpoint = loadV3Checkpoint(checkpointPath);
  const edgar = createSecEdgarClient();
  const secApi = createSecApiIoClient({ budget });
  const exa = createExaContractTimingClient({ budget });
  const v2Keys = loadV2ExclusionKeys();

  if (!secApi.config.enabled) {
    console.log(JSON.stringify({ error: "SEC_API_IO_KEY_ABSENT", stop: true }, null, 2));
    process.exitCode = 1;
    return;
  }

  const allNewFindings = [];
  const allDocs = [];
  const batchReports = [];
  let weights = checkpoint.weights || {};
  let totalNewVsV2 = 0;
  let statusQueries = 0;
  let exaStatusQueries = 0;
  let duplicatesPrevented = 0;

  // Seed discovery once (cached thereafter) — 424B / franchise-column first
  const discovery = await discoverHits(secApi, budget, checkpoint, weights);
  let queue = discovery.hits
    .filter((h) => !isPriorWaveAccession(h.accessionNumber))
    .filter((h) => !isAccessionProcessed(checkpoint, h.accessionNumber));
  queue = rankDiscoveryQueue(queue);

  // Per-batch mix: ~80% high-precision CMBS/franchise, ~20% exploration (mgmt/EX-10/lease)
  function takeMixedBatch(src, size) {
    const high = [];
    const explore = [];
    for (const h of src) {
      const form = String(h.formType || "").toUpperCase();
      const isCmbs = /^424B/.test(form) || form === "424H" || form.startsWith("S-11");
      const isHighFam = HIGH_PRECISION_QUERY_FAMILIES.has(h.queryFamily);
      if (isCmbs || isHighFam) high.push(h);
      else explore.push(h);
    }
    const highN = Math.min(high.length, Math.ceil(size * 0.8));
    const exploreN = Math.min(explore.length, size - highN);
    const picked = [...high.slice(0, highN), ...explore.slice(0, exploreN)];
    const pickedSet = new Set(picked.map((h) => h.accessionNumber));
    // If high pool thin, backfill from explore then remainder of high
    while (picked.length < size) {
      const next = high.find((h) => !pickedSet.has(h.accessionNumber))
        || explore.find((h) => !pickedSet.has(h.accessionNumber));
      if (!next) break;
      picked.push(next);
      pickedSet.add(next.accessionNumber);
    }
    const remaining = src.filter((h) => !pickedSet.has(h.accessionNumber));
    return { batchHits: picked, remaining };
  }

  let zeroYieldStreak = 0;
  let batchesWithCmbsDocs = 0;
  for (let batchNo = 1; batchNo <= MAX_BATCHES; batchNo++) {
    if (allDocs.filter((d) => d.ok).length >= TARGET_DOCS) break;
    if (!queue.length) break;
    const mixed = takeMixedBatch(queue, BATCH_SIZE);
    const batchHits = mixed.batchHits;
    queue = mixed.remaining;
    if (!batchHits.length) break;
    const cmbsInBatch = batchHits.filter((h) => /^424B/i.test(String(h.formType || ""))).length;
    if (cmbsInBatch > 0) batchesWithCmbsDocs += 1;

    const batchFindings = [];
    const yieldByQueryFamily = {};
    const docs = [];

    for (const hit of batchHits) {
      const fetched = await fetchSecDocument(edgar, hit);
      budget.record({ provider: "FREE_SEC", operation: "document_fetch", usd: 0, note: hit.accessionNumber });
      if (!fetched.ok) {
        docs.push({
          ok: false,
          accessionNumber: hit.accessionNumber,
          error: fetched.error,
          queryFamily: hit.queryFamily,
          agreementFamily: hit.agreementFamily,
        });
        markAccessionProcessed(checkpoint, hit.accessionNumber, { ok: false });
        continue;
      }
      const meta = {
        cik: hit.cik,
        accessionNumber: hit.accessionNumber,
        form: hit.formType,
        companyName: hit.companyName,
        filingDate: hit.filedAt ? String(hit.filedAt).slice(0, 10) : null,
        documentUrl: fetched.url || hit.filingUrl,
        sourceFamily: inferSourceFamily(hit.formType, hit.filingUrl),
      };
      const extracted = extractFromDocument(fetched.text, meta);
      const tagged = [];
      for (const f of extracted.hotelContractFindings) {
        const k = findingKey(f);
        const kAcc = findingKeyWithAcc(f);
        if (v2Keys.has(k) || isPriorWaveAccession(f.source?.accessionNumber)) {
          duplicatesPrevented += 1;
          continue;
        }
        if (isFindingSeen(checkpoint, kAcc)) {
          duplicatesPrevented += 1;
          continue;
        }
        // Current-status probe (bounded)
        let laterEvidence = [];
        if (isCommerciallyFutureWindow(f.primaryDate) || Number(String(f.primaryDate).slice(0, 4)) >= 2024) {
          const probe = await probeLaterStatus(secApi, f, budget);
          statusQueries += probe.queries;
          laterEvidence = probe.laterEvidence || [];
          if (isCommerciallyFutureWindow(f.primaryDate) && laterEvidence.length < 2) {
            const exaProbe = await selectiveExaStatus(exa, f, budget);
            exaStatusQueries += exaProbe.queries;
            laterEvidence = [...laterEvidence, ...(exaProbe.laterEvidence || [])];
          }
        }
        const status = classifyContractCurrentStatus(f, { now: started, laterEvidence });
        const row = {
          ...f,
          agreementFamily: hit.agreementFamily || null,
          queryFamily: hit.queryFamily,
          discoveryProvider: "SEC_API_IO",
          currentStatus: status.status,
          currentStatusNote: status.note,
          laterEvidenceCount: laterEvidence.length,
          commerciallyFuture2027_2035: isCommerciallyFutureWindow(f.primaryDate),
        };
        tagged.push(row);
        markFindingKey(checkpoint, kAcc);
        yieldByQueryFamily[hit.queryFamily] = (yieldByQueryFamily[hit.queryFamily] || 0) + 1;
      }
      batchFindings.push(...tagged);
      docs.push({
        ok: true,
        accessionNumber: hit.accessionNumber,
        cik: hit.cik,
        companyName: hit.companyName,
        formType: hit.formType,
        filedAt: hit.filedAt,
        url: fetched.url || hit.filingUrl,
        queryFamily: hit.queryFamily,
        agreementFamily: hit.agreementFamily,
        bytes: fetched.text.length,
        hotelContractDates: tagged.length,
        futureDates: tagged.filter((t) => t.commerciallyFuture2027_2035).length,
      });
      markAccessionProcessed(checkpoint, hit.accessionNumber, {
        ok: true,
        dates: tagged.length,
        queryFamily: hit.queryFamily,
      });
    }

    // Deduplicate within batch
    const dedup = new Map();
    for (const f of batchFindings) {
      const k = findingKey(f);
      if (!dedup.has(k)) dedup.set(k, f);
      else duplicatesPrevented += 1;
    }
    const uniqueBatch = [...dedup.values()];
    const verified = verifyContractTimingFindings(uniqueBatch, { now: started });
    const persistable = [...verified.verified, ...verified.probable];
    allNewFindings.push(...uniqueBatch);
    allDocs.push(...docs);
    totalNewVsV2 += uniqueBatch.length;

    weights = adaptWeightsFromBatch(weights, { yieldByQueryFamily });
    checkpoint.weights = weights;
    const summary = {
      batchNo,
      docsOk: docs.filter((d) => d.ok).length,
      docsTried: docs.length,
      newDates: uniqueBatch.length,
      future2027_2035: uniqueBatch.filter((f) => f.commerciallyFuture2027_2035).length,
      yieldByQueryFamily,
      verification: verified.counts,
      summarize: summarizeFindings(uniqueBatch),
    };
    appendBatchSummary(checkpoint, summary);
    saveV3Checkpoint(checkpoint, checkpointPath);
    batchReports.push(summary);

    console.error(
      JSON.stringify({
        batch: batchNo,
        docsOk: summary.docsOk,
        newDates: summary.newDates,
        future2027_2035: summary.future2027_2035,
        cumulativeNew: totalNewVsV2,
      }),
    );

    if (summary.newDates === 0) zeroYieldStreak += 1;
    else zeroYieldStreak = 0;

    // Only early-stop after we have actually sampled CMBS 424B docs and still get zeros
    if (zeroYieldStreak >= 2 && batchesWithCmbsDocs >= 1 && cmbsInBatch > 0) {
      console.error(JSON.stringify({ stop: "two_zero_yield_cmbs_batches", batchesWithCmbsDocs }));
      break;
    }
    // Re-rank remaining queue after a zero batch so exploration does not starve 424Bs
    if (summary.newDates === 0 && queue.length) {
      queue = rankDiscoveryQueue(queue);
    }
  }

  const verifyAll = verifyContractTimingFindings(allNewFindings, { now: started });
  const persistSet = [...verifyAll.verified, ...verifyAll.probable];
  const persistence = await persistFindings(persistSet);
  const spent = budget.snapshot();
  const docsOk = allDocs.filter((d) => d.ok);
  const summaryAll = summarizeFindings(allNewFindings);
  const datesPer100 = docsOk.length
    ? Number(((allNewFindings.length / docsOk.length) * 100).toFixed(2))
    : 0;
  const datesPer100ExclTop = docsOk.length
    ? Number(((summaryAll.exclTopCount / docsOk.length) * 100).toFixed(2))
    : 0;

  const horizons = {
    next6m: verifyAll.counts.horizon6,
    next12m: verifyAll.counts.horizon12,
    next24m: verifyAll.counts.horizon24,
    next36m: verifyAll.counts.horizon36,
    historicalNeedRevalidation: verifyAll.counts.historicalNeedRevalidation,
  };

  const statusCounts = allNewFindings.reduce((a, f) => {
    a[f.currentStatus || "UNKNOWN"] = (a[f.currentStatus || "UNKNOWN"] || 0) + 1;
    return a;
  }, {});

  const agreementSplit = {
    franchise: allNewFindings.filter((f) => f.dateMeaning === DATE_MEANING.FRANCHISE_EXPIRATION).length,
    management: allNewFindings.filter((f) => f.dateMeaning === DATE_MEANING.MANAGEMENT_EXPIRATION).length,
    license: allNewFindings.filter((f) => f.dateMeaning === DATE_MEANING.LICENSE_EXPIRATION).length,
    terminationRights: allNewFindings.filter((f) => f.dateMeaning === DATE_MEANING.TERMINATION_RIGHT_DATE)
      .length,
    byDiscoveryFamily: summaryAll.byAgreement,
  };

  const examples = allNewFindings
    .filter((f) => f.commerciallyFuture2027_2035)
    .slice(0, 8)
    .map((f) => ({
      hotelName: f.hotelName,
      city: f.city,
      stateRegion: f.stateRegion,
      primaryDate: f.primaryDate,
      dateMeaning: f.dateMeaning,
      currentStatus: f.currentStatus,
      company: f.source?.companyName,
      accession: f.source?.accessionNumber,
      url: f.source?.documentUrl,
      snippet: String(f.snippet || "").slice(0, 160),
    }));

  const report = {
    module: "HOTEL_CONTRACT_TIMING_INTELLIGENCE_V3",
    version: "3.0.0",
    started,
    finished: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    mergeStatus: "DO_NOT_MERGE_WITHOUT_IC",
    priorWavesExcluded: {
      baselineAccession: BASELINE_JPMCC_ACCESSION,
      v2Accessions: V2_KNOWN_ACCESSIONS,
      v2FindingKeysExcluded: v2Keys.size,
    },
    targets: { batchSize: BATCH_SIZE, maxBatches: MAX_BATCHES, targetDocs: TARGET_DOCS },
    discovery: {
      queriesRun: discovery.queriesRun,
      uniqueHits: discovery.hits.length,
      queryStats: discovery.queryStats,
    },
    documents: {
      processed: allDocs.length,
      ok: docsOk.length,
      independentDeals: summaryAll.uniqueDeals,
      byForm: docsOk.reduce((a, d) => {
        const f = d.formType || "UNKNOWN";
        a[f] = (a[f] || 0) + 1;
        return a;
      }, {}),
    },
    incrementalVsPriorWaves: {
      newUniqueDates: allNewFindings.length,
      newUniqueHotels: summaryAll.uniqueHotels,
      newFranchiseDates: agreementSplit.franchise,
      newManagementDates: agreementSplit.management,
      newLicenseDates: agreementSplit.license,
      future2027_2035: summaryAll.future2027_2035,
      datesPer100Docs: datesPer100,
      datesPer100DocsExclTopDeal: datesPer100ExclTop,
      topDeal: summaryAll.topDeal,
      dealConcentration: summaryAll.dealConcentration,
      byBrand: summaryAll.byBrand,
      byYear: summaryAll.byYear,
      duplicatesPrevented,
    },
    horizons,
    agreementSplit,
    currentStatusCounts: statusCounts,
    statusValidation: {
      secFollowUpQueries: statusQueries,
      exaSelectiveQueries: exaStatusQueries,
      note: "Silence is not corroboration; EXPIRED_STATUS_UNCONFIRMED / REQUIRES_REVALIDATION used when no later evidence",
    },
    batches: batchReports,
    adaptiveWeightsFinal: weights,
    budget: {
      ceilingUsd: CONTRACT_TIMING_V2_PAID_CEILING_USD,
      spentUsd: spent.spentUsd,
      remainingUsd: spent.remaining,
      byProvider: spent.byProvider,
      requestCounts: spent.requestCounts,
    },
    persistence: persistence.counts,
    examples,
    scalability: {
      assessment:
        allNewFindings.length >= 30 && summaryAll.uniqueDeals >= 5
          ? "PROMISING_REPEATABLE — multiple independent deals beyond V2 concentration"
          : allNewFindings.length >= 10
            ? "MODERATE — incremental dates across deals; continue adaptive batches on high-yield families while exploring management EX-10"
            : "LIMITED — expand issuer CIK filters and management-expiration columns; many 424Bs still lack filled franchise dates",
      recommendation:
        "Keep PR #495 mergeable for V1–V3 foundation; run further scale waves as sequential PRs after IC merge to avoid an indefinitely growing branch.",
    },
    clientStats: { secApiIo: secApi.stats(), exa: exa.stats(), edgar: edgar.stats() },
  };

  fs.writeFileSync(path.join(OUT_DIR, "V3_PILOT_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "V3_FINDINGS_NEW.json"),
    JSON.stringify({ findings: allNewFindings, verification: verifyAll.counts }, null, 2),
  );

  const md = `# Hotel Contract Timing Intelligence V3 — Scale Pilot

**Mode:** ${report.mode} · **CUSTOMER_BLOCKED** · Do not merge without IC

## Prior waves excluded
- Baseline JPMCC 57 + V2 accessions/findings (${v2Keys.size} keys)

## This wave (new only)
| Metric | Value |
|---|---:|
| Documents OK | ${docsOk.length} |
| Independent deals | ${summaryAll.uniqueDeals} |
| New unique dates | **${allNewFindings.length}** |
| New hotels | ${summaryAll.uniqueHotels} |
| Franchise / Management / License | ${agreementSplit.franchise} / ${agreementSplit.management} / ${agreementSplit.license} |
| Future 2027–2035 | **${summaryAll.future2027_2035}** |
| Dates/100 docs | ${datesPer100} |
| Excl. top deal /100 | ${datesPer100ExclTop} |
| Horizons 6/12/24/36m | ${horizons.next6m}/${horizons.next12m}/${horizons.next24m}/${horizons.next36m} |

## Current-status classes
${Object.entries(statusCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "- (none)"}

## Budget
Spent **$${spent.spentUsd}** of $${CONTRACT_TIMING_V2_PAID_CEILING_USD} (Exa metered; SEC-API.io subscription calls=${spent.requestCounts.SEC_API_IO || 0})

## Persistence
Created ${persistence.counts.persisted} · Read-back ${persistence.counts.readBackOk} · Dupes prevented ${persistence.counts.duplicatesPrevented}

## Scalability
${report.scalability.assessment}

${report.scalability.recommendation}

## Future examples
${examples
  .slice(0, 5)
  .map((e, i) => `${i + 1}. **${e.hotelName}** — ${e.dateMeaning} ${e.primaryDate} · ${e.currentStatus} · ${e.company || e.accession}`)
  .join("\n") || "_No 2027–2035 examples in completed batches_"}
`;
  fs.writeFileSync(path.join(OUT_DIR, "V3_FOUNDER_REPORT.md"), md);

  console.log(
    JSON.stringify(
      {
        mode: report.mode,
        docsOk: docsOk.length,
        newDates: allNewFindings.length,
        future2027_2035: summaryAll.future2027_2035,
        deals: summaryAll.uniqueDeals,
        datesPer100,
        datesPer100ExclTop,
        spentUsd: spent.spentUsd,
        persistence: persistence.counts,
        batches: batchReports.length,
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
