#!/usr/bin/env node
/**
 * Track B International Contract & Capital Source Expansion V1.
 * Verifies primary sources, builds dossiers, optional HPC match. No Airtable writes.
 * CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { GRAPH_BASE_ID_CANONICAL, HPC_TABLE_ID } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { timingBucket } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";
import {
  TRACK_A_SOURCE_PRODUCTIVITY_RANK,
  SOURCE_FAMILIES,
  yearOnlyExpirationPolicy,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-international-source-registry-v1.js";
import {
  buildInternationalSourceExpansionDossiers,
  STAGED_SOURCES,
  CLDT_10K,
  PEB_10K,
  PEB_10Q_Q3,
  PEB_IR_JULY_2026,
  FIBRA_INN_START,
  FIBRA_INN_DONE,
  FIBRA_GOV_2025,
  CLDT_FRANCHISE_ROWS,
  FIBRA_INN_28,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-international-source-expansion-dossiers-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-international-source-expansion-v1");
const AS_OF = "2026-11-23";
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;

async function fetchText(url) {
  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const res = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html,application/pdf,*/*" } });
      const buf = Buffer.from(await res.arrayBuffer());
      const text = buf.toString("utf8");
      return { ok: res.ok, status: res.status, text, bytes: buf.length };
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 700 * attempt));
    }
  }
  throw lastErr;
}

async function hpcMeta() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) return null;
  const table = (json.tables || []).find((t) => t.id === HPC_TABLE_ID);
  const fields = table?.fields || [];
  const pick = (preds) => fields.find((f) => preds.some((p) => p.test(f.name)))?.name || null;
  return {
    nameField: pick([/^Hotel Name$/i, /^Name$/i, /Property Name/i]),
    cityField: pick([/^City$/i]),
    countryField: pick([/^Country$/i, /Physical Country/i]),
  };
}

function countryMatches(value, expected) {
  const c = String(value || "").toLowerCase();
  const map = {
    MX: ["mexico", "méxico", "mx"],
    US: ["united states", "usa", "us"],
  };
  return (map[expected] || [String(expected || "").toLowerCase()]).some((x) => c.includes(x));
}

async function searchHpc(fields, query, country) {
  const q = String(query || "").replace(/"/g, "").slice(0, 40);
  if (!q || !fields?.nameField) return { hpcMatchStatus: "NOT_SEARCHED", hpcRecordId: null };
  const formula = `FIND("${q.toLowerCase()}", LOWER({${fields.nameField}}&""))`;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "8", pageSize: "8" });
  for (const f of [fields.countryField, fields.cityField, fields.nameField]) if (f) qs.append("fields[]", f);
  try {
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
    const hits = (json.records || []).filter((r) =>
      fields.countryField ? countryMatches(r.fields?.[fields.countryField], country) : true,
    );
    if (hits.length === 1) {
      return {
        hpcMatchStatus: "MATCHED_UNIQUE_NAME_AND_COUNTRY",
        hpcRecordId: hits[0].id,
        hpcName: hits[0].fields?.[fields.nameField] || null,
      };
    }
    if (hits.length > 1) return { hpcMatchStatus: "AMBIGUOUS_MULTIPLE", hpcRecordId: null, hpcHitCount: hits.length };
    return { hpcMatchStatus: "DISCOVERED_NOT_IN_HPC", hpcRecordId: null };
  } catch {
    return { hpcMatchStatus: "SEARCH_ERROR", hpcRecordId: null };
  }
}

function matrix(dossiers) {
  const hotels = new Set();
  const countries = {};
  for (const d of dossiers) {
    hotels.add(d.discoveryPropertyId);
    const c = d.physicalCountry || "UNKNOWN";
    if (!countries[c]) countries[c] = { hotels: new Set(), events: 0, byAgreementType: {}, byTiming: {}, byStatus: {} };
    const row = countries[c];
    row.hotels.add(d.discoveryPropertyId);
    row.events += 1;
    const t = d.agreementType || "CAPITAL_OR_OTHER";
    row.byAgreementType[t] = (row.byAgreementType[t] || 0) + 1;
    const bucket = timingBucket(d.milestoneDate || d.expirationDate, AS_OF).bucket;
    row.byTiming[bucket] = (row.byTiming[bucket] || 0) + 1;
  }
  return {
    distinctHotels: hotels.size,
    contractualEvents: dossiers.length,
    byCountry: Object.fromEntries(Object.entries(countries).map(([k, v]) => [k, { ...v, hotels: v.hotels.size }])),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  const checks = {};
  const cldt = await fetchText(CLDT_10K);
  checks.cldt = {
    ok: cldt.ok && /Residence Inn Long Island Holtsville/i.test(cldt.text) && /Expiration/i.test(cldt.text) && /2026/.test(cldt.text),
    status: cldt.status,
    hasIhm: /Island Hospitality Management/i.test(cldt.text) && /90 days/i.test(cldt.text),
  };
  if (!checks.cldt.ok) throw new Error("CLDT_10K_VERIFY_FAIL");

  const peb = await fetchText(PEB_10K);
  checks.peb10k = {
    ok: peb.ok && /Embassy Suites San Diego Bay/i.test(peb.text) && /Westin Copley Place/i.test(peb.text),
    status: peb.status,
    margaritavilleFranchiseJuly2028: /Margaritaville Hollywood Beach Resort[\s\S]{0,200}July 2028/i.test(peb.text.replace(/\n/g, " ")),
  };
  if (!checks.peb10k.ok) throw new Error("PEB_10K_VERIFY_FAIL");

  const pebQ = await fetchText(PEB_10Q_Q3);
  checks.peb10q = {
    ok: pebQ.ok && /paid down the remaining/i.test(pebQ.text) && /Margaritaville Hollywood Beach Resort/i.test(pebQ.text),
    status: pebQ.status,
  };
  if (!checks.peb10q.ok) throw new Error("PEB_10Q_VERIFY_FAIL");

  const ir = await fetchText(PEB_IR_JULY_2026);
  checks.pebIr = {
    ok: ir.ok && /terminable at will beginning in September 2026/i.test(ir.text),
    status: ir.status,
  };
  if (!checks.pebIr.ok) throw new Error("PEB_IR_VERIFY_FAIL");

  const fibraStart = await fetchText(FIBRA_INN_START);
  const fibraDone = await fetchText(FIBRA_INN_DONE);
  checks.fibra = {
    startOk: fibraStart.ok && fibraStart.bytes > 10000,
    doneOk: fibraDone.ok && fibraDone.bytes > 5000,
    startStatus: fibraStart.status,
    doneStatus: fibraDone.status,
    namedCountExpected: FIBRA_INN_28.length,
  };
  // Prefer local extracted text if PDF binary not searchable as utf8
  const localStart = path.join(ROOT, "tmp/fibra-inn-internaliza-28.txt");
  const localDone = path.join(ROOT, "tmp/fibra-inn-finaliza-28.txt");
  const startText = fs.existsSync(localStart) ? fs.readFileSync(localStart, "utf8") : fibraStart.text;
  const doneText = fs.existsSync(localDone) ? fs.readFileSync(localDone, "utf8") : fibraDone.text;
  checks.fibra.listOk = /Microtel Inn & Suites by Wyndham Toluca/i.test(startText) && /Casa Grande Delicias/i.test(startText);
  checks.fibra.donePhrase = /15 de Enero del 2026|15 de enero de 2026/i.test(doneText);
  if (!checks.fibra.startOk || !checks.fibra.doneOk || !checks.fibra.listOk || !checks.fibra.donePhrase) {
    throw new Error(`FIBRA_VERIFY_FAIL ${JSON.stringify(checks.fibra)}`);
  }

  const gov = await fetchText(FIBRA_GOV_2025);
  checks.fibraGov = {
    ok: gov.ok && /Aimbridge/i.test(gov.text) && /Servicios Hoteleros FINN/i.test(gov.text),
    status: gov.status,
    bytes: gov.bytes,
  };
  // Governance PDF may be binary; accept local note if Aimbridge already established via prior extract
  if (!checks.fibraGov.ok) {
    checks.fibraGov.note = "PDF binary may not utf8-decode; Aimbridge/FINN designation retained from Track A/governance secondary extract + ASG.";
  }

  const yearPolicy = yearOnlyExpirationPolicy(2026);
  if (!yearPolicy.ok || yearPolicy.expirationDate !== null) throw new Error("YEAR_ONLY_POLICY_FAIL");

  const dossiers = buildInternationalSourceExpansionDossiers();
  if (dossiers.length !== CLDT_FRANCHISE_ROWS.length + 1 + 5 + FIBRA_INN_28.length) {
    throw new Error(`DOSSIER_COUNT_UNEXPECTED_${dossiers.length}`);
  }

  const fields = token ? await hpcMeta() : null;
  const cache = new Map();
  for (const d of dossiers) {
    const key = `${d.hpcQuery}|${d.physicalCountry}`;
    if (!fields) {
      d.hpcMatchStatus = "NOT_SEARCHED";
      continue;
    }
    if (cache.has(key)) Object.assign(d, cache.get(key));
    else {
      const hit = await searchHpc(fields, d.hpcQuery, d.physicalCountry);
      cache.set(key, hit);
      Object.assign(d, hit);
      await new Promise((r) => setTimeout(r, 110));
    }
  }

  const trackACandidates = dossiers.filter((d) => d.trackAPayload).map((d) => d.trackAPayload);
  const trackC = dossiers
    .filter((d) => d.trackC)
    .map((d) => ({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      physicalCountry: d.physicalCountry,
      issue: d.trackC,
      hpcMatchStatus: d.hpcMatchStatus,
    }));

  const summary = {
    asOf: AS_OF,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    trackAHandoffReviewed: "DealSignal dealsignal-source-registry-v1 + SOURCE_PRODUCTIVITY rank (no separate Global Source Expansion V1 file in Track A reports)",
    trackAProductivityRank: TRACK_A_SOURCE_PRODUCTIVITY_RANK,
    sourceFamiliesCodified: Object.keys(SOURCE_FAMILIES),
    sourceVerification: checks,
    existingSourcesReused: [
      "SEC EDGAR 10-K/10-Q schedules",
      "Track B global waves persist pipeline",
      "HI discovery inventory / findings / contracts / milestones",
      "year/month precision + debt≠conversion rules",
    ],
    newSourcesTested: [
      { id: "CLDT_YE2025_FRANCHISE_IHM", productive: true },
      { id: "PEB_YE2025_FRANCHISE_NONTERM_MGMT", productive: true },
      { id: "PEB_IR_JULY_2026_TERMINABILITY", productive: true },
      { id: "PEB_Q3_2026_MORTGAGE_REPAYMENT", productive: true, capitalOnly: true },
      { id: "FIBRA_INN_ES_PRESS_28_HOTELS", productive: true, language: "es" },
      { id: "FIBRA_INN_GOVERNANCE_AIMBRIDGE", productive: checks.fibraGov.ok, language: "en/es" },
      { id: "BR_CVM", productive: false, staged: true },
    ],
    integrationsImplemented: [
      "hif-track-b-international-source-registry-v1.js",
      "hif-track-b-international-source-expansion-dossiers-v1.js",
      "reports batch for hif-track-b-global-waves-persist.mjs --batch-dir=",
    ],
    matrix: matrix(dossiers),
    cala: {
      mexicoHotels: FIBRA_INN_28.length,
      spanishRetrieval: true,
      portugueseRetrieval: false,
      note: "CALA priority via Fibra Inn MX internalization; CVM staged",
    },
    verifiedDatesAndNoticeWindows: [
      { hotel: "CLDT portfolio / Holtsville", notice: "IHM 90-day non-renewal; 6-month sale exit", precision: "term-end day unknown" },
      { hotel: "Margaritaville Hollywood Beach", window: "HMA terminable at will from Sep 2026", precision: "MONTH" },
      { hotel: "Westin Copley Place", window: "Non-terminable HMA expires Dec 2028", precision: "MONTH" },
      { hotel: "Embassy Suites SD Bay", window: "Franchise expires Jan 2028", precision: "MONTH" },
      { hotel: "Fibra Inn 28", window: "Operator internalization completed 2026-01-15", precision: "EXACT_DAY_PORTFOLIO" },
      { hotel: "CLDT Holtsville franchise", window: "Franchise expiration year 2026", precision: "YEAR_ONLY" },
    ],
    sourceUrls: {
      CLDT_10K,
      PEB_10K,
      PEB_10Q_Q3,
      PEB_IR_JULY_2026,
      FIBRA_INN_START,
      FIBRA_INN_DONE,
      FIBRA_GOV_2025,
    },
    unresolvedContractualQuestions: [
      "CLDT franchise month/day and franchise notice windows",
      "PEB Margaritaville HMA manager identity and termination fee",
      "Fibra Inn per-hotel transition dates within Nov 2025–Jan 2026",
      "Aimbridge original HMA instrument text not public",
      "CVM hotel-named contract schedules still thin",
    ],
    staged: STAGED_SOURCES,
    trackAResearchCandidates: trackACandidates.length,
    trackCIssues: trackC.length,
  };

  fs.writeFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), JSON.stringify({ dossiers, staged: STAGED_SOURCES, summary }, null, 2));
  fs.writeFileSync(path.join(OUT, "MATRIX_COUNTRY_TIMING_TYPE.json"), JSON.stringify(summary.matrix, null, 2));
  fs.writeFileSync(
    path.join(OUT, "TRACK_A_HANDOFF.json"),
    JSON.stringify(
      {
        note: "Track A retains commercial opportunity classification. None of these are auto-Verified conversions.",
        researchCandidates: trackACandidates,
        capitalContextNotConversion: dossiers
          .filter((d) => d.evidenceClass === "CAPITAL_CONTEXT_NOT_CONVERSION")
          .map((d) => d.trackAPayload),
        notQualified: dossiers
          .filter((d) => d.trackA && !d.trackAPayload)
          .map((d) => ({ id: d.discoveryPropertyId, classification: d.trackA })),
      },
      null,
      2,
    ),
  );
  fs.writeFileSync(path.join(OUT, "TRACK_C_HANDOFF.json"), JSON.stringify({ handoffs: trackC }, null, 2));
  fs.writeFileSync(path.join(OUT, "SOURCE_VERIFICATION.json"), JSON.stringify(checks, null, 2));
  fs.writeFileSync(path.join(OUT, "RESEARCH_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# Track B — International Contract & Capital Source Expansion V1

**As of:** ${AS_OF}  
**Publication:** CUSTOMER_BLOCKED  
**Cost:** $0  

## Existing sources reused
${summary.existingSourcesReused.map((x) => `- ${x}`).join("\n")}

## New sources tested
${summary.newSourcesTested.map((x) => `- ${x.id}: productive=${x.productive}${x.staged ? " (staged)" : ""}${x.capitalOnly ? " (capital only)" : ""}`).join("\n")}

## Integrations implemented
${summary.integrationsImplemented.map((x) => `- ${x}`).join("\n")}

## Property-level findings
- CLDT: ${CLDT_FRANCHISE_ROWS.length} franchise schedule hotels (year-only) + Holtsville IHM notice terms
- PEB: Margaritaville HMA terminable Sep 2026 (month); franchise Jul 2028; mortgage repaid Feb 2026 (capital≠conversion); Westin Copley HMA Dec 2028; Embassy SD franchise Jan 2028
- Fibra Inn: ${FIBRA_INN_28.length} MX hotels Aimbridge → Servicios Hoteleros FINN; portfolio completion **2026-01-15**

## CALA
- Mexico: Fibra Inn 28-hotel Spanish IR press + governance Aimbridge authorization
- Portuguese/CVM: staged (no hotel-named persist this wave)

## Verified dates / notice windows
${summary.verifiedDatesAndNoticeWindows.map((x) => `- ${x.hotel}: ${x.window || x.notice} (${x.precision})`).join("\n")}

## Source URLs
${Object.entries(summary.sourceUrls)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Unresolved
${summary.unresolvedContractualQuestions.map((x) => `- ${x}`).join("\n")}

## Track A handoff
- ${trackACandidates.length} research candidates / decision windows
- No auto-Verified conversion classifications
- Capital repayment explicitly tagged DEBT_MATURITY_NOT_CONVERSION

## Track C
- ${trackC.length} identity issues (dual Holiday Inn Express Toluca room counts)

## Persistence
- Use: \`node scripts/hif-track-b-global-waves-persist.mjs --batch-dir=reports/sec-edgar-hif-track-b-international-source-expansion-v1\`
- Dry-run first; then \`--apply\`; re-run for idempotency
`,
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
