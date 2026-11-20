#!/usr/bin/env node
/**
 * Hotel Contract Timing Intelligence — CMBS & public disclosure pilot.
 *
 * Default: dry-run research (SEC free) + local report. No Airtable writes.
 *   node scripts/sec-edgar-contract-timing-cmbs-pilot-v1.mjs
 *   node scripts/sec-edgar-contract-timing-cmbs-pilot-v1.mjs --offline-fixture
 *   node scripts/sec-edgar-contract-timing-cmbs-pilot-v1.mjs --apply
 *
 * Cap: ≤100 documents. CUSTOMER_BLOCKED only. No HPC auto-create. No merge.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  CONTRACT_TIMING_MODULE_ID,
  CONTRACT_TIMING_MODULE_VERSION,
  SOURCE_FAMILY,
  DATE_MEANING,
  CONTRACT_TIMING_GUARANTEES,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-types-v1.js";
import {
  listSeedDocuments,
  CONTRACT_TIMING_PILOT_DOC_CAP,
  CMBS_ISSUER_REGISTRY,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-pilot-registry-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  flattenSubmissionFilings,
  listHistoricalFilings,
} from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v2.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { bridgeFranchiseScheduleToContractTiming } from "../lib/hotel-intelligence/sec-edgar/contract-timing-franchise-bridge-v1.js";
import { verifyContractTimingFindings } from "../lib/hotel-intelligence/sec-edgar/contract-timing-verifier-v1.js";
import {
  loadDiscoveryInventory,
  defaultInventoryRoot,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { parseHotelAgreementsFromExhibitIndex } from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const OFFLINE = process.argv.includes("--offline-fixture");
const DOC_LIMIT = Math.min(
  CONTRACT_TIMING_PILOT_DOC_CAP,
  Number(process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || CONTRACT_TIMING_PILOT_DOC_CAP),
);
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-contract-timing-cmbs-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function loadTableIds() {
  const p = path.join(
    ROOT,
    "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
  );
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function resolveSeedFiling(client, seed) {
  if (seed.accessionNumber && seed.primaryDocument) {
    return {
      ...seed,
      accessionNumber: seed.accessionNumber,
      primaryDocument: seed.primaryDocument,
      form: seed.form,
      filingDate: seed.filingDate,
    };
  }
  const sub = await client.getSubmissions(seed.cik);
  if (!sub.ok || !sub.body) return { ...seed, resolveError: sub.error || "submissions_failed" };

  // Prefer exact seed accession from full submission history (not latest-only).
  if (seed.accessionNumber) {
    const all = flattenSubmissionFilings(sub.body);
    const exact = all.find((f) => f.accessionNumber === seed.accessionNumber);
    if (exact?.primaryDocument) {
      return {
        ...seed,
        accessionNumber: exact.accessionNumber,
        primaryDocument: exact.primaryDocument,
        form: exact.form || seed.form,
        filingDate: exact.filingDate || seed.filingDate,
      };
    }
    // Try filing package index when accession known but not in submissions flatten
    const idx = await client.getFilingPackageIndex(seed.cik, seed.accessionNumber);
    if (idx.ok && idx.body?.directory?.item) {
      const items = idx.body.directory.item;
      const primary =
        items.find((it) => /\.htm$/i.test(it.name || "") && /10-?k|424b/i.test(it.name || "")) ||
        items.find((it) => /\.htm$/i.test(it.name || ""));
      if (primary?.name) {
        return {
          ...seed,
          primaryDocument: primary.name,
          form: seed.form,
          filingDate: seed.filingDate,
        };
      }
    }
  }

  const forms = seed.resolveLatestForm
    ? [seed.resolveLatestForm]
    : seed.form
      ? [seed.form]
      : ["10-K", "424B5", "424B2"];
  const listed = listHistoricalFilings(sub.body, {
    formTypes: forms,
    sinceYear: 2010,
    limit: 8,
    prioritizeHighYield: true,
  });
  const pick = listed.filings[0] || null;
  if (!pick) return { ...seed, resolveError: "no_filing" };
  return {
    ...seed,
    accessionNumber: pick.accessionNumber,
    primaryDocument: pick.primaryDocument,
    form: pick.form,
    filingDate: pick.filingDate,
  };
}

function looksLikeYearBuiltCollateral(snippet) {
  const s = String(snippet || "");
  // CMBS mortgaged-property tables: Year Built / Renovation / Rooms — not franchise dates
  if (/Hospitality\s*\|\s*(Full|Limited|Select)\s+Service\s*\|\s*\d{4}/i.test(s)) return true;
  if (/\bYear\s*Built\b/i.test(s) && /\bRooms?\b/i.test(s)) return true;
  if (/\|\s*\d{4}\s*\|\s*(?:20\d{2}|19\d{2})(?:-\d{4})?\s*\|\s*\d{2,4}\s*\|\s*Rooms/i.test(s)) return true;
  return false;
}

function extractFromDocument(text, meta) {
  const family = meta.sourceFamily || SOURCE_FAMILY.CMBS_424B;
  const cmbs = extractCmbsPropertySchedule({ text }, meta);
  let bridged = { hotelContractFindings: [], rejected: [] };
  let sched = { hotelCount: 0, managerRelationships: [] };

  // Franchise schedule bridge is for REIT property schedules only.
  // Never run it on CMBS 424B — year-built columns false-positive as "Franchise YYYY".
  if (family === SOURCE_FAMILY.HOTEL_REIT_SCHEDULE || family === SOURCE_FAMILY.OTHER_SEC) {
    sched = extractFranchiseExpirationSchedule({ text }, meta);
    bridged = bridgeFranchiseScheduleToContractTiming(sched, meta);
  }

  const byId = new Map();
  for (const f of [...cmbs.hotelContractFindings, ...bridged.hotelContractFindings]) {
    if (looksLikeYearBuiltCollateral(f.snippet)) continue;
    const year = Number(String(f.primaryDate || "").slice(0, 4));
    if (Number.isFinite(year) && year < 1995) continue; // absurd franchise/mgmt expirations
    const key = f.findingId || sha1(`${f.hotelName}|${f.primaryDate}|${f.dateMeaning}`);
    if (!byId.has(key)) {
      byId.set(key, {
        ...f,
        findingId:
          String(key).startsWith("ctim_") || String(key).startsWith("find_")
            ? key
            : `ctim_${String(key).slice(0, 20)}`,
        hotelName: String(f.hotelName || "").replace(/&amp;/g, "&").replace(/&#\d+;/g, " "),
      });
    }
  }
  return {
    hotelContractFindings: [...byId.values()],
    loanMaturityFindings: cmbs.loanMaturityFindings,
    rejected: [...cmbs.rejected, ...bridged.rejected],
    scheduleHotels: sched.hotelCount,
    managerRelationships: sched.managerRelationships || [],
  };
}

async function expandCmbsFilings(client, docsSoFar) {
  const extra = [];
  for (const issuer of CMBS_ISSUER_REGISTRY) {
    if (docsSoFar + extra.length >= DOC_LIMIT) break;
    const sub = await client.getSubmissions(issuer.cik);
    if (!sub.ok || !sub.body) continue;
    const listed = listHistoricalFilings(sub.body, {
      formTypes: issuer.forms,
      sinceYear: 2012,
      limit: 12,
      prioritizeHighYield: true,
    });
    for (const f of listed.filings) {
      if (docsSoFar + extra.length >= DOC_LIMIT) break;
      const id = `cmbs_${issuer.slug}_${f.accessionNumber}`;
      if (extra.some((e) => e.accessionNumber === f.accessionNumber)) continue;
      extra.push({
        id,
        sourceFamily: SOURCE_FAMILY.CMBS_424B,
        slug: issuer.slug,
        legalName: sub.body.name || issuer.slug,
        cik: issuer.cik,
        form: f.form,
        accessionNumber: f.accessionNumber,
        primaryDocument: f.primaryDocument,
        filingDate: f.filingDate,
        priority: 3,
      });
    }
  }
  return extra;
}

async function persistFindings(verifiedRows, opts = {}) {
  const token = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
  if (!token) return { ok: false, error: "NO_AIRTABLE_TOKEN" };
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
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const json = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, json };
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

  for (const row of verifiedRows.slice(0, opts.limit || 40)) {
    const propertyId =
      row.discoveryPropertyId ||
      `disc_ctim_${sha1(`${row.hotelName}|${row.city}|${row.stateRegion}`).slice(0, 20)}`;
    const findingId =
      row.findingId ||
      `find_ctim_${sha1(`${propertyId}|${row.dateMeaning}|${row.primaryDate}`).slice(0, 16)}`;

    let invRec = await findInv(propertyId);
    if (!invRec) {
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
        [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]:
          row.hpcMatchStatus === "MATCHED" ? "MATCHED" : "DISCOVERED_NOT_IN_HPC",
        [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
        [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "HTML_PROPERTY_TABLE",
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `Contract timing CMBS pilot — ${row.dateMeaning}`,
      };
      for (const k of Object.keys(fields)) {
        if (fields[k] == null || fields[k] === "") delete fields[k];
      }
      if (APPLY) {
        const created = await at(invTable, "POST", { fields, typecast: true });
        if (!created.ok) {
          errors.push({ propertyId, stage: "inventory", err: created.json });
          continue;
        }
        invRec = created.json;
      } else {
        invRec = { id: `dry_${propertyId}`, fields };
      }
    } else {
      skippedDup.push({ type: "inventory", propertyId });
    }

    const existingFinding = await findFinding(findingId);
    if (existingFinding) {
      skippedDup.push({ type: "finding", findingId });
      persisted.push({
        findingId,
        propertyId,
        action: "SKIP_DUP",
        readBackOk: true,
        airtableFindingId: existingFinding.id,
      });
      await sleep(200);
      continue;
    }

    const fFields = {
      [MAP_DISCOVERY_FINDINGS.findingId]: findingId,
      [MAP_DISCOVERY_FINDINGS.discoveryProperty]: invRec?.id?.startsWith("dry_")
        ? undefined
        : invRec?.id
          ? [invRec.id]
          : undefined,
      [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
      [MAP_DISCOVERY_FINDINGS.fieldName]: row.dateMeaning || "Contract Timing",
      [MAP_DISCOVERY_FINDINGS.valueObserved]: String(row.primaryDate || "").slice(0, 200),
      [MAP_DISCOVERY_FINDINGS.agreementType]:
        row.agreementType === "FRANCHISE_AGREEMENT"
          ? "FRANCHISE"
          : row.agreementType === "MANAGEMENT_AGREEMENT"
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
        "CONTRACT_TIMING_CMBS_PILOT_V1",
        `DATE_MEANING=${row.dateMeaning}`,
        `SOURCE_FAMILY=${row.source?.sourceFamily || ""}`,
        ...(row.notes || []),
      ]
        .filter(Boolean)
        .join(" | ")
        .slice(0, 4000),
    };
    for (const k of Object.keys(fFields)) {
      if (fFields[k] == null || fFields[k] === "") delete fFields[k];
    }

    if (!APPLY) {
      persisted.push({ findingId, propertyId, action: "DRY_RUN", readBackOk: false });
      continue;
    }

    const created = await at(findTable, "POST", { fields: fFields, typecast: true });
    if (!created.ok) {
      errors.push({ findingId, stage: "finding", err: created.json });
      continue;
    }
    const rb = await findFinding(findingId);
    persisted.push({
      findingId,
      propertyId,
      action: "CREATE",
      airtableFindingId: created.json.id,
      readBackOk: Boolean(rb),
      publicationState: "CUSTOMER_BLOCKED",
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

async function main() {
  const started = new Date().toISOString();
  const client = createSecEdgarClient();
  const inventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  const discoveryHotels = inventory.hotels || [];

  const docsResearched = [];
  const allFindings = [];
  const allLoan = [];
  const allRejected = [];
  const managers = [];
  const byFamily = {};
  let exhibitMentions = 0;

  if (OFFLINE) {
    const fixture = fs.readFileSync(
      path.join(ROOT, "fixtures/sec-edgar/contract-timing-cmbs-schedule-sample.txt"),
      "utf8",
    );
    const meta = {
      cik: "0001013611",
      accessionNumber: "0001013611-99-000001",
      form: "424B5",
      companyName: "Sample CMBS Depositor (fixture)",
      filingDate: "2024-06-01",
      documentUrl: "fixture://contract-timing-cmbs-schedule-sample.txt",
      sourceFamily: SOURCE_FAMILY.CMBS_424B,
    };
    const extracted = extractFromDocument(fixture, meta);
    docsResearched.push({
      id: "offline_fixture",
      sourceFamily: SOURCE_FAMILY.CMBS_424B,
      ok: true,
      bytes: fixture.length,
      hotelContractDates: extracted.hotelContractFindings.length,
      loanMaturities: extracted.loanMaturityFindings.length,
    });
    allFindings.push(...extracted.hotelContractFindings);
    allLoan.push(...extracted.loanMaturityFindings);
    allRejected.push(...extracted.rejected);
    byFamily[SOURCE_FAMILY.CMBS_424B] = 1;
  } else {
    let seeds = listSeedDocuments();
    const resolved = [];
    for (const seed of seeds) {
      if (resolved.length >= DOC_LIMIT) break;
      const r = await resolveSeedFiling(client, seed);
      resolved.push(r);
    }
    const extras = await expandCmbsFilings(client, resolved.length);
    const queue = [...resolved, ...extras].slice(0, DOC_LIMIT);

    for (const doc of queue) {
      byFamily[doc.sourceFamily] = (byFamily[doc.sourceFamily] || 0) + 1;
      if (doc.resolveError || !doc.accessionNumber || !doc.primaryDocument) {
        docsResearched.push({
          id: doc.id,
          sourceFamily: doc.sourceFamily,
          ok: false,
          error: doc.resolveError || "missing_accession_or_doc",
          slug: doc.slug,
        });
        continue;
      }
      const url = client.filingDocumentUrl(doc.cik, doc.accessionNumber, doc.primaryDocument);
      const fetched = await client.getFilingDocument(doc.cik, doc.accessionNumber, doc.primaryDocument);
      if (!fetched.ok || !fetched.text) {
        docsResearched.push({
          id: doc.id,
          sourceFamily: doc.sourceFamily,
          ok: false,
          error: fetched.error || `HTTP_${fetched.status}`,
          url,
          slug: doc.slug,
        });
        continue;
      }
      const meta = {
        cik: doc.cik,
        accessionNumber: doc.accessionNumber,
        form: doc.form,
        companyName: doc.legalName,
        filingDate: doc.filingDate,
        documentUrl: url,
        sourceFamily: doc.sourceFamily,
      };
      const extracted = extractFromDocument(fetched.text, meta);

      // Light EX-10 index scan for agreement exhibits (no deep download unless hotel hit)
      try {
        const idx = await client.getFilingPackageIndex(doc.cik, doc.accessionNumber);
        if (idx.ok && idx.text) {
          const agreements = parseHotelAgreementsFromExhibitIndex(idx.text, meta);
          exhibitMentions += agreements?.length || 0;
        }
      } catch {
        /* non-fatal */
      }

      docsResearched.push({
        id: doc.id,
        sourceFamily: doc.sourceFamily,
        ok: true,
        url,
        slug: doc.slug,
        form: doc.form,
        accessionNumber: doc.accessionNumber,
        filingDate: doc.filingDate,
        bytes: fetched.text.length,
        hotelContractDates: extracted.hotelContractFindings.length,
        loanMaturities: extracted.loanMaturityFindings.length,
        scheduleHotels: extracted.scheduleHotels,
      });
      allFindings.push(...extracted.hotelContractFindings);
      allLoan.push(...extracted.loanMaturityFindings);
      allRejected.push(...extracted.rejected);
      managers.push(...(extracted.managerRelationships || []));
    }
  }

  // Deduplicate findings
  const deduped = new Map();
  let dupsPrevented = 0;
  for (const f of allFindings) {
    const k = `${String(f.hotelName || "").toLowerCase()}|${f.city}|${f.stateRegion}|${f.dateMeaning}|${f.primaryDate}`;
    if (deduped.has(k)) {
      dupsPrevented += 1;
      continue;
    }
    deduped.set(k, f);
  }
  const uniqueFindings = [...deduped.values()];

  const nameHints = uniqueFindings.map((f) => f.hotelName).filter(Boolean);
  const hpcCandidates = [];
  // Prefer seed match for known fixtures; live HPC search is optional
  const token = (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
  const hpcBase = (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
  if (token && hpcBase && !OFFLINE) {
    const seen = new Set();
    for (const hint of nameHints.slice(0, 30)) {
      const safe = String(hint).replace(/'/g, "\\'").slice(0, 50);
      if (safe.length < 6) continue;
      const formula = encodeURIComponent(
        `SEARCH(LOWER('${safe.toLowerCase()}'), LOWER({Property Name}))`,
      );
      try {
        const url = `https://api.airtable.com/v0/${hpcBase}/tbl9aY5ijiuIzzWam?filterByFormula=${formula}&maxRecords=3`;
        const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
        const json = await res.json().catch(() => ({}));
        for (const rec of json.records || []) {
          if (seen.has(rec.id)) continue;
          seen.add(rec.id);
          hpcCandidates.push({
            id: rec.id,
            name: rec.fields?.["Property Name"] || rec.fields?.Name || null,
            city: rec.fields?.City || null,
            brand: rec.fields?.Brand || rec.fields?.Affiliation || null,
          });
        }
      } catch {
        /* continue */
      }
      await sleep(180);
    }
  }

  const verification = verifyContractTimingFindings(uniqueFindings, {
    now: started,
    hpcCandidates,
    discoveryInventoryHotels: discoveryHotels,
  });

  const persistSet = [
    ...verification.verified,
    ...verification.probable.filter((p) => p.identityClass === "VERIFIED_PROPERTY" || p.identityClass === "PROBABLE_PROPERTY"),
  ].slice(0, 50);

  const persistence = await persistFindings(persistSet, { limit: 40 });

  const docsOk = docsResearched.filter((d) => d.ok).length;
  const exactDates = verification.counts.verifiedHotelContractDates + verification.counts.probable;
  const spendUsd = 0; // SEC EDGAR fair-access only
  const per100 = docsOk ? (exactDates / docsOk) * 100 : 0;
  const perDollar = spendUsd > 0 ? exactDates / spendUsd : null;

  const report = {
    module: CONTRACT_TIMING_MODULE_ID,
    version: CONTRACT_TIMING_MODULE_VERSION,
    started,
    finished: new Date().toISOString(),
    mode: OFFLINE ? "OFFLINE_FIXTURE" : APPLY ? "APPLY" : "DRY_RUN",
    guarantees: CONTRACT_TIMING_GUARANTEES,
    documents: {
      researched: docsResearched.length,
      ok: docsOk,
      cap: DOC_LIMIT,
      bySourceFamily: byFamily,
      detail: docsResearched,
    },
    extraction: {
      hotelsMentioned: uniqueFindings.length + allRejected.filter((r) => r.hotelName).length,
      uniqueContractFindings: uniqueFindings.length,
      loanMaturitiesIsolated: allLoan.length,
      rejectedRows: allRejected.length,
      duplicateDiscoveriesPrevented: dupsPrevented,
      exhibitIndexMentions: exhibitMentions,
    },
    verification: verification.counts,
    horizons: {
      next6m: verification.counts.horizon6,
      next12m: verification.counts.horizon12,
      next24m: verification.counts.horizon24,
      next36m: verification.counts.horizon36,
      historicalNeedRevalidation: verification.counts.historicalNeedRevalidation,
    },
    agreementSplit: {
      franchise: verification.counts.franchise,
      management: verification.counts.management,
      terminationRights: verification.counts.terminationRights,
    },
    managersDisclosed: managers.slice(0, 50).map((m) => ({
      entityName: m.entityName,
      hotelName: m.attributedHotelName || m.hotelNameHints?.[0],
      city: m.city,
      stateRegion: m.stateRegion,
    })),
    hpc: {
      candidatesLoaded: hpcCandidates.length,
      matched: verification.counts.hpcMatched,
      autoCreate: false,
    },
    persistence: persistence.counts,
    persistenceDetail: persistence,
    metrics: {
      researchSpendUsd: spendUsd,
      exactHotelLevelContractDates: exactDates,
      datesPer100Documents: Number(per100.toFixed(2)),
      datesPerResearchDollar: perDollar,
      sourcePrecision:
        uniqueFindings.length > 0
          ? Number(
              (
                (verification.counts.verifiedHotelContractDates + verification.counts.probable) /
                uniqueFindings.length
              ).toFixed(3),
            )
          : 0,
    },
    scalability: {
      assessment:
        exactDates >= 5 || (OFFLINE && exactDates >= 3)
          ? "PROMISING — public CMBS/REIT schedules can yield hotel-level franchise/management dates when tables are HTML-parsable; loan maturity isolation is mandatory."
          : "LIMITED_IN_THIS_WAVE — many recent CMBS filings are image/XBRL-heavy; prioritize historical HTML 424B and REIT property schedules.",
      remainingCoverage:
        "Expand CMBS depositor CIKs, EX-10 hotel agreement exhibits, FDD term tables (selected brands), and non-SEC REIT supplements. Revalidate historical dates before CURRENTLY_VERIFIED.",
      recommendedNext:
        "Target high-yield HTML 424B property schedules with Franchise Expiration columns; keep loan maturity in a separate ledger.",
    },
    sampleVerified: [...verification.verified, ...verification.probable].slice(0, 25),
    sampleLoanIsolated: allLoan.slice(0, 10),
    publicationState: "CUSTOMER_BLOCKED",
    mergeStatus: "DO_NOT_MERGE_WITHOUT_IC",
  };

  fs.writeFileSync(path.join(OUT_DIR, "PILOT_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "VERIFIED_FINDINGS.json"),
    JSON.stringify({ verified: verification.verified, probable: verification.probable }, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "LOAN_MATURITY_ISOLATED.json"),
    JSON.stringify({ loanMaturityFindings: allLoan }, null, 2),
  );

  const md = `# Hotel Contract Timing Intelligence — CMBS Pilot V1

**Module:** ${CONTRACT_TIMING_MODULE_ID} ${CONTRACT_TIMING_MODULE_VERSION}  
**Mode:** ${report.mode}  
**Publication:** CUSTOMER_BLOCKED  
**Merge:** DO NOT MERGE without Integration Controller approval

## Documents researched

| Source family | Count |
|---|---|
${Object.entries(byFamily)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n") || "| (none) | 0 |"}

- Documents OK: **${docsOk}** / ${docsResearched.length} (cap ${DOC_LIMIT})
- Hotels / findings extracted (unique): **${uniqueFindings.length}**
- Exact hotel-level contract dates (verified+probable): **${exactDates}**
- Loan maturities isolated (not used as hotel timing): **${allLoan.length}**
- Duplicate discoveries prevented: **${dupsPrevented}**
- HPC matches: **${verification.counts.hpcMatched}** (auto-create: never)
- Historical dates needing revalidation: **${verification.counts.historicalNeedRevalidation}**

## Horizons (forward-looking)

| Window | Count |
|---|---|
| 6 months | ${verification.counts.horizon6} |
| 12 months | ${verification.counts.horizon12} |
| 24 months | ${verification.counts.horizon24} |
| 36 months | ${verification.counts.horizon36} |

## Agreement split

- Franchise: ${verification.counts.franchise}
- Management: ${verification.counts.management}
- Termination rights: ${verification.counts.terminationRights}

## Persistence

- Apply: ${APPLY}
- Created: ${persistence.counts.persisted}
- Dry-run planned: ${persistence.counts.dryRun}
- Duplicates prevented: ${persistence.counts.duplicatesPrevented}
- Read-back OK: ${persistence.counts.readBackOk}
- All research rows: CUSTOMER_BLOCKED

## Yield

- Dates per 100 documents: **${report.metrics.datesPer100Documents}**
- Research spend: **$${spendUsd}** (SEC EDGAR only)
- Source precision: **${report.metrics.sourcePrecision}**

## Scalability

${report.scalability.assessment}

Remaining: ${report.scalability.remainingCoverage}

## Guarantees

${CONTRACT_TIMING_GUARANTEES.map((g) => `- ${g}`).join("\n")}
`;
  fs.writeFileSync(path.join(OUT_DIR, "PILOT_REPORT.md"), md);

  console.log(JSON.stringify({
    mode: report.mode,
    docsOk,
    exactDates,
    datesPer100: report.metrics.datesPer100Documents,
    loanIsolated: allLoan.length,
    persistence: persistence.counts,
    outDir: OUT_DIR,
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
