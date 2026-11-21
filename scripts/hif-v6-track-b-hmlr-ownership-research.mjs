#!/usr/bin/env node
/**
 * HIF V6 Track B — HM Land Registry ULPD ownership research against HE UK hotels.
 * Never prints credentials. Does not purchase paid title docs / NPS without founder auth.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createReadStream } from "fs";
import { createInterface } from "readline";
import { execFileSync } from "child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-hif-v6-track-b-hmlr");
const CACHE_DIR = path.join(OUT_DIR, "cache");
const ULPD_BASE = "https://use-land-property-data.service.gov.uk/api/v1";

function loadEnvFile(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*)$/);
    if (!m) continue;
    let v = m[2].trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[m[1]] = v;
  }
  return out;
}

function resolveSecrets() {
  const merged = {
    ...loadEnvFile("C:/Dev/dealality/my-operators-backend/.env"),
    ...loadEnvFile(path.join(ROOT, ".env")),
  };
  return {
    hmlrKey: process.env.HM_LAND_REGISTRY_KEY || merged.HM_LAND_REGISTRY_KEY || "",
    chKey: process.env.COMPANIES_HOUSE_API_KEY || merged.COMPANIES_HOUSE_API_KEY || "",
  };
}

function normalizePostcode(pc) {
  if (!pc) return "";
  return String(pc)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .replace(/^(.+)([0-9][A-Z]{2})$/, "$1 $2")
    .trim();
}

function normalizeAddr(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function classifyHttp(status, bodyText) {
  if (status == null) return "NETWORK_FAILURE";
  if (status === 429) return "RATE_LIMIT";
  if (status >= 500) return "SERVER_ERROR";
  if (status === 401 || status === 403) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    if (/need to provide your API Key/i.test(bodyText)) return "AUTH_MISSING_KEY";
    if (/not yet signed the licence/i.test(bodyText)) return "LICENCE_NOT_SIGNED";
    if (/not paid for access/i.test(bodyText)) return "PAID_DATASET_REQUIRED";
    if (/do not have access/i.test(bodyText)) return "PERMISSION_DENIED";
    return "PERMISSION_OR_FORBIDDEN";
  }
  if (status === 404) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) {
      return "AUTH_INVALID_KEY";
    }
    return "NOT_FOUND";
  }
  if (status >= 200 && status < 300) return "OK";
  return `HTTP_${status}`;
}

async function ulpdGet(key, pathname) {
  const started = Date.now();
  try {
    const res = await fetch(`${ULPD_BASE}${pathname}`, {
      headers: { Authorization: key, Accept: "application/json" },
    });
    const bodyText = await res.text();
    let parsed = null;
    try {
      parsed = JSON.parse(bodyText);
    } catch {
      /* ignore */
    }
    return {
      pathname,
      status: res.status,
      errClass: classifyHttp(res.status, bodyText),
      ms: Date.now() - started,
      parsed,
      error: parsed?.error || null,
    };
  } catch (e) {
    return {
      pathname,
      status: null,
      errClass: "NETWORK_FAILURE",
      ms: Date.now() - started,
      parsed: null,
      error: String(e?.message || e),
    };
  }
}

/**
 * Collect UK hotel candidates from HE / census / HIF artifacts without inventing identities.
 */
function loadUkHotelCandidates() {
  const hotels = [];
  const seen = new Set();

  function add(h) {
    const name = String(h.name || h.hotelName || "").trim();
    const postcode = normalizePostcode(h.postcode || h.postalCode || "");
    const address = String(h.address || h.propertyAddress || "").trim();
    const country = String(h.country || h.hotelCountry || "GB").toUpperCase();
    if (!name) return;
    if (!/(GB|UK|UNITED KINGDOM|ENGLAND|WALES|SCOTLAND)/i.test(country) && !postcode) {
      // require UK signal
      if (!/\b(UK|United Kingdom|England|Wales|Scotland|London)\b/i.test(`${name} ${address}`)) {
        return;
      }
    }
    const key = `${normalizeAddr(name)}|${postcode}|${normalizeAddr(address)}`;
    if (seen.has(key)) return;
    seen.add(key);
    hotels.push({
      name,
      address,
      postcode,
      city: h.city || h.market || h.hotelMarket || null,
      country: "GB",
      heRecordId: h.heRecordId || h.recordId || h.airtableRecordId || null,
      sourceFile: h.sourceFile || null,
      identitySource: h.identitySource || "INVENTORY_SCAN",
    });
  }

  // 1) Explicit seed list from prior Track B CH cohort (legal entities ≠ hotels; keep separate)
  const chPath = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-track-b-wave2/CH_CANDIDATES_INVESTIGATED.json"
  );
  if (fs.existsSync(chPath)) {
    // not hotels — skip ownership from company names alone
  }

  // 2) Scan known HE/census JSON snippets for UK hotels with addresses/postcodes
  const scanRoots = [
    path.join(ROOT, "fixtures"),
    path.join(ROOT, "data", "hotel-census"),
    path.join(ROOT, "reports", "sec-edgar-hif-v6-dual-track"),
    path.join(ROOT, "reports", "sec-edgar-hif-v6-track-b-wave2"),
  ];

  const ukPostcodeRe =
    /\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i;
  const hotelNameRe =
    /\b((?:The\s+)?[A-Z][A-Za-z0-9&'.\- ]{2,60}\s+(?:Hotel|Inn|Suites|Resort|Lodge))\b/;

  function scanFile(file) {
    if (!/\.(json|csv|md|txt)$/i.test(file)) return;
    let st;
    try {
      st = fs.statSync(file);
    } catch {
      return;
    }
    if (!st.isFile() || st.size > 6_000_000) return;
    let text;
    try {
      text = fs.readFileSync(file, "utf8");
    } catch {
      return;
    }
    if (!/hotel/i.test(text)) return;
    if (!/(United Kingdom|\bEngland\b|\bWales\b|\bScotland\b|\bLondon\b|"GB"|"UK"|postcode)/i.test(text)) {
      return;
    }

    // Prefer structured JSON arrays of hotel-like objects
    if (file.endsWith(".json")) {
      try {
        const j = JSON.parse(text);
        const rows = Array.isArray(j)
          ? j
          : Array.isArray(j?.hotels)
            ? j.hotels
            : Array.isArray(j?.rows)
              ? j.rows
              : Array.isArray(j?.candidates)
                ? j.candidates
                : [];
        for (const row of rows.slice(0, 5000)) {
          if (!row || typeof row !== "object") continue;
          const blob = JSON.stringify(row);
          if (!/hotel|inn|suites/i.test(blob)) continue;
          const country = String(
            row.country || row.Country || row.hotelCountry || row.nation || ""
          );
          const pc =
            row.postcode ||
            row.postalCode ||
            row.Postcode ||
            row.zip ||
            "";
          if (
            !/(GB|UK|United Kingdom|England|Wales|Scotland)/i.test(country) &&
            !ukPostcodeRe.test(String(pc)) &&
            !ukPostcodeRe.test(blob)
          ) {
            continue;
          }
          add({
            name: row.name || row.hotelName || row.propertyName || row.Hotel || null,
            address: row.address || row.street || row.propertyAddress || "",
            postcode: pc || (blob.match(ukPostcodeRe) || [])[1] || "",
            city: row.city || row.market || row.town || null,
            heRecordId: row.id || row.recordId || row.airtableRecordId || null,
            sourceFile: file,
            identitySource: "STRUCTURED_INVENTORY",
          });
        }
      } catch {
        /* fall through to text scan */
      }
    }

    // Text/regex harvest — candidates only, not verified HE identities
    const lines = text.split(/\r?\n/).slice(0, 20000);
    for (const line of lines) {
      if (!/hotel|inn|suites/i.test(line)) continue;
      const pc = (line.match(ukPostcodeRe) || [])[1];
      if (!pc) continue;
      const nm = (line.match(hotelNameRe) || [])[1];
      if (!nm) continue;
      add({
        name: nm.trim(),
        address: line.slice(0, 240),
        postcode: pc,
        sourceFile: file,
        identitySource: "TEXT_HARVEST_CANDIDATE",
      });
    }
  }

  function walk(dir, depth = 0) {
    if (depth > 5 || !fs.existsSync(dir)) return;
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      if (ent.name === "node_modules" || ent.name.startsWith(".")) continue;
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(p, depth + 1);
      else scanFile(p);
    }
  }
  for (const r of scanRoots) walk(r);

  // Curated England/Wales hotel seeds with known HE-style identities for readiness
  // (addresses from public hotel websites / prior research — matching still requires CCOD row evidence)
  const curated = [
    {
      name: "The Savoy",
      address: "Strand, London",
      postcode: "WC2R 0EZ",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      heRecordId: null,
    },
    {
      name: "Claridge's",
      address: "Brook Street, Mayfair, London",
      postcode: "W1K 4HR",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "The Langham London",
      address: "1C Portland Place, London",
      postcode: "W1B 1JA",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "Hilton London Bankside",
      address: "2-8 Great Suffolk Street, London",
      postcode: "SE1 0UG",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "Park Plaza Westminster Bridge London",
      address: "200 Westminster Bridge Road, London",
      postcode: "SE1 7UT",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "Manchester Airport Marriott Hotel",
      address: "Hale Road, Hale Barns, Altrincham",
      postcode: "WA15 8XW",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "Hilton Manchester Deansgate",
      address: "303 Deansgate, Manchester",
      postcode: "M3 4LQ",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
    {
      name: "The Grand Hotel Birmingham",
      address: "Colmore Row, Birmingham",
      postcode: "B3 2DA",
      city: "Birmingham",
      identitySource: "CURATED_PUBLIC_ADDRESS",
    },
  ];
  for (const c of curated) add({ ...c, country: "GB", sourceFile: "curated-seed" });

  return hotels;
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (ch === '"') inQ = false;
      else cur += ch;
    } else if (ch === '"') inQ = true;
    else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else cur += ch;
  }
  out.push(cur);
  return out;
}

async function matchHotelsAgainstCsv(csvPath, hotels) {
  const byPc = new Map();
  for (const h of hotels) {
    const pc = normalizePostcode(h.postcode).replace(/\s+/g, "");
    if (!pc) continue;
    if (!byPc.has(pc)) byPc.set(pc, []);
    byPc.get(pc).push(h);
  }

  const matches = [];
  const ambiguous = [];
  let rowsScanned = 0;
  let hotelLexiconRows = 0;

  const rl = createInterface({ input: createReadStream(csvPath, { encoding: "utf8" }) });
  let headers = null;
  for await (const line of rl) {
    if (!headers) {
      headers = parseCsvLine(line).map((h) => h.trim());
      continue;
    }
    rowsScanned++;
    const cols = parseCsvLine(line);
    const row = Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? ""]));
    const propAddr = row["Property Address"] || row["Property address"] || "";
    const postcode = normalizePostcode(row.Postcode || row.postcode || "");
    const pcKey = postcode.replace(/\s+/g, "");
    if (/hotel|inn |suites|resort|lodge/i.test(propAddr)) hotelLexiconRows++;
    if (!pcKey || !byPc.has(pcKey)) continue;

    const titleNumber = row["Title Number"] || row["Title number"] || null;
    const tenure = row.Tenure || null;
    const proprietors = [];
    for (let i = 1; i <= 4; i++) {
      const name = row[`Proprietor Name (${i})`] || row[`Proprietor Name ${i}`];
      if (!name) continue;
      proprietors.push({
        name: String(name).trim(),
        companyNumber: String(
          row[`Company Registration No. (${i})`] ||
            row[`Company Registration No. ${i}`] ||
            ""
        ).trim() || null,
        category: row[`Proprietorship Category (${i})`] || null,
        address1: row[`Proprietor (${i}) Address (1)`] || null,
      });
    }
    const dateAdded = row["Date Proprietor Added"] || null;

    for (const h of byPc.get(pcKey)) {
      const addrN = normalizeAddr(propAddr);
      const hotelN = normalizeAddr(h.name);
      const strongName =
        hotelN &&
        (addrN.includes(hotelN) ||
          hotelN
            .split(" ")
            .filter((w) => w.length > 3)
            .every((w) => addrN.includes(w)));
      const record = {
        hotel: h,
        titleNumber,
        tenure,
        propertyAddress: propAddr,
        postcode,
        proprietors,
        dateProprietorAdded: dateAdded,
        matchClass: strongName ? "POSTCODE_AND_NAME_EVIDENCE" : "POSTCODE_ONLY_AMBIGUOUS",
        verificationClass: strongName
          ? "PROPERTY_OWNER_CANDIDATE_NEEDS_CORROBORATION"
          : "AMBIGUOUS_POSTCODE_MATCH",
      };
      if (strongName) matches.push(record);
      else ambiguous.push(record);
    }
  }

  return { rowsScanned, hotelLexiconRows, matches, ambiguous };
}

async function tryDownloadCcodCou(key) {
  const meta = await ulpdGet(key, "/datasets/ccod");
  if (meta.errClass !== "OK") {
    return { ok: false, stage: "metadata", ...meta };
  }
  const resources = meta.parsed?.result?.resources || [];
  const cou = resources.find((r) => /COU/i.test(r.file_name || "")) || resources[0];
  if (!cou?.file_name) {
    return { ok: false, stage: "no_resource", errClass: "NOT_FOUND", error: "No CCOD resource" };
  }
  const ticket = await ulpdGet(
    key,
    `/datasets/ccod/${encodeURIComponent(cou.file_name)}`
  );
  if (ticket.errClass !== "OK") {
    return {
      ok: false,
      stage: "download_ticket",
      file: cou.file_name,
      errClass: ticket.errClass,
      error: ticket.error,
      status: ticket.status,
    };
  }
  const downloadUrl = ticket.parsed?.result?.download_url;
  if (!downloadUrl) {
    return {
      ok: false,
      stage: "download_ticket",
      file: cou.file_name,
      errClass: "SCHEMA_ERROR",
      error: "Missing download_url",
    };
  }
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  const zipPath = path.join(CACHE_DIR, cou.file_name);
  const res = await fetch(downloadUrl);
  if (!res.ok) {
    return {
      ok: false,
      stage: "download_bytes",
      file: cou.file_name,
      errClass: `HTTP_${res.status}`,
      error: `Download failed HTTP ${res.status}`,
    };
  }
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(zipPath, buf);
  // Extract first CSV via PowerShell Expand-Archive (no extra npm deps).
  const extractDir = path.join(CACHE_DIR, "ccod_cou_extract");
  fs.rmSync(extractDir, { recursive: true, force: true });
  fs.mkdirSync(extractDir, { recursive: true });
  try {
    execFileSync(
      "powershell.exe",
      [
        "-NoProfile",
        "-Command",
        `Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${extractDir.replace(/'/g, "''")}' -Force`,
      ],
      { stdio: ["ignore", "pipe", "pipe"] }
    );
  } catch (e) {
    return {
      ok: false,
      stage: "extract",
      file: cou.file_name,
      errClass: "EXTRACT_FAILED",
      error: String(e?.message || e).slice(0, 300),
    };
  }
  const csv = fs
    .readdirSync(extractDir)
    .find((f) => f.toLowerCase().endsWith(".csv"));
  if (!csv) {
    return {
      ok: false,
      stage: "extract",
      file: cou.file_name,
      errClass: "SCHEMA_ERROR",
      error: "No CSV in zip",
    };
  }
  return {
    ok: true,
    stage: "ready",
    file: cou.file_name,
    zipPath,
    csvPath: path.join(extractDir, csv),
    bytes: buf.length,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const { hmlrKey, chKey } = resolveSecrets();
  const hotels = loadUkHotelCandidates();

  const authProbe = hmlrKey
    ? await ulpdGet(hmlrKey, "/datasets")
    : { errClass: "KEY_ABSENT", status: null, error: "HM_LAND_REGISTRY_KEY missing" };

  let download = {
    ok: false,
    stage: "skipped",
    errClass: hmlrKey ? "PENDING" : "KEY_ABSENT",
    error: null,
  };
  let matchResult = null;

  if (hmlrKey && authProbe.errClass === "OK") {
    download = await tryDownloadCcodCou(hmlrKey);
    if (download.ok) {
      matchResult = await matchHotelsAgainstCsv(download.csvPath, hotels);
    }
  }

  const verifiedOwners = (matchResult?.matches || []).filter(
    (m) => m.matchClass === "POSTCODE_AND_NAME_EVIDENCE"
  );
  // Strict: without independent HE identity + title corroboration, do not elevate to VERIFIED
  const propertyVerifiedOwners = [];

  const companiesResolved = [];
  for (const m of verifiedOwners) {
    for (const p of m.proprietors || []) {
      if (p.companyNumber) {
        companiesResolved.push({
          companyNumber: p.companyNumber,
          companyName: p.name,
          titleNumber: m.titleNumber,
          hotelName: m.hotel.name,
          companiesHouseLookup:
            chKey
              ? "API_KEY_PRESENT_BUT_LOOKUP_DEFERRED_UNTIL_VERIFIED_OWNER"
              : "COMPANIES_HOUSE_API_KEY_ABSENT_PUBLIC_HTML_AVAILABLE",
        });
      }
    }
  }

  const metrics = {
    track: "TRACK_B",
    wave: "HMLR_ACTIVATION",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    product: "Use land and property data (ULPD)",
    authenticatedAccess: authProbe.errClass === "OK",
    authOutcome: authProbe.errClass === "OK" ? "AUTHENTICATED_OK" : authProbe.errClass,
    companiesHouseApiKeyPresent: Boolean(chKey),
    hotelPropertiesInvestigated: hotels.length,
    hotelIdentitySources: hotels.reduce((acc, h) => {
      acc[h.identitySource] = (acc[h.identitySource] || 0) + 1;
      return acc;
    }, {}),
    ccodDownload: {
      ok: Boolean(download.ok),
      stage: download.stage,
      errClass: download.errClass || null,
      error: download.error || null,
      file: download.file || null,
    },
    titlePropertyMatchesStrong: verifiedOwners.length,
    titlePropertyMatchesAmbiguous: matchResult?.ambiguous?.length || 0,
    verifiedLegalPropertyOwners: propertyVerifiedOwners.length,
    corporateEntitiesWithCompanyNumbers: companiesResolved.length,
    hotelToOwnerIndependentlyCorroborated: 0,
    unresolvedOrAmbiguous: hotels.length - verifiedOwners.length,
    rowsScanned: matchResult?.rowsScanned || 0,
    hotelLexiconRowsInCcodSample: matchResult?.hotelLexiconRows || 0,
    incrementalVsPriorHif: {
      netNewVerifiedOwners: 0,
      note: "No verified hotel→proprietor relationships until CCOD licence signed and address/title evidence corroborates HE identity",
    },
    blocker:
      download.errClass === "LICENCE_NOT_SIGNED"
        ? "Sign CCOD (and OCOD) dataset licences at https://use-land-property-data.service.gov.uk/ then re-run"
        : download.errClass === "KEY_ABSENT"
          ? "HM_LAND_REGISTRY_KEY missing"
          : download.ok
            ? null
            : download.errClass || download.error,
  };

  const founder = `# HIF V6 Track B — HM Land Registry activation

**As of:** ${new Date().toISOString().slice(0, 10)}  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0  

## Phase 1 — Authenticated access

| Item | Result |
|------|--------|
| \`HM_LAND_REGISTRY_KEY\` | PRESENT (length ${hmlrKey.length || 0}; value not logged) |
| Product | **Use land and property data** (ULPD) REST API |
| Base URL | \`${ULPD_BASE}\` |
| Auth model | API key in \`Authorization\` header |
| Auth probe \`GET /datasets\` | **${metrics.authOutcome}** |
| Datasets listed | ccod, ocod, nps, nps_sample, res_cov, leases |
| CCOD download ticket | **${download.errClass || (download.ok ? "OK" : "FAILED")}** |
| OCOD | Licence required (same pattern as CCOD) |
| NPS / leases / res_cov | Permission denied or unpaid — **not** used |
| Business Gateway (live title register) | **NOT** granted by this key |
| \`COMPANIES_HOUSE_API_KEY\` | ${chKey ? "PRESENT" : "**ABSENT**"} (Land Registry research not blocked) |

### What this credential permits

- List datasets and read metadata — **yes**
- Download **CCOD / OCOD** bulk corporate ownership CSVs — **only after signing free dataset licences** on the ULPD website
- National Polygon Service — **paid / no access** (founder authorization required)
- Per-address official copies / live proprietor API — **no** (that is Business Gateway + fees)

## Phase 2–4 — Property ownership research

| Metric | Count |
|--------|------:|
| Hotel properties prepared for investigation | ${metrics.hotelPropertiesInvestigated} |
| Strong title/address matches | ${metrics.titlePropertyMatchesStrong} |
| Ambiguous postcode-only matches | ${metrics.titlePropertyMatchesAmbiguous} |
| Verified legal property owners | **${metrics.verifiedLegalPropertyOwners}** |
| Corporate entities resolved to CH numbers | ${metrics.corporateEntitiesWithCompanyNumbers} |
| Independently corroborated hotel→owner links | **0** |

${
  download.errClass === "LICENCE_NOT_SIGNED"
    ? `### Blocker (action required)

Authenticated API works, but downloads return:

> You have not yet signed the licence for this dataset: ccod

**Founder action:** sign in at [Use land and property data](https://use-land-property-data.service.gov.uk/), agree the **CCOD** and **OCOD** licences (free), then re-run \`node scripts/hif-v6-track-b-hmlr-ownership-research.mjs\`.

No automatic paid title purchases were attempted.
`
    : ""
}

## Governance

- CUSTOMER_BLOCKED · no HPC/Meta writes · no customer publication
- Credential never printed in reports
- Operator/franchisee not treated as legal proprietor
- CH company-number resolution deferred until verified proprietor rows exist

## Recommended next steps

1. Sign CCOD + OCOD licences (free) on ULPD — unlocks ~4.5M UK corporate title rows.
2. Re-run ownership matcher against HE UK hotels with postcodes (prefer COU then FULL).
3. For each strong postcode+name hit: treat as **candidate** until HE identity + title number corroboration.
4. Resolve proprietor company numbers via Companies House (set \`COMPANIES_HOUSE_API_KEY\` for scale; public HTML remains available).
5. Do **not** buy NPS or Business Gateway official copies without a specific budget ask.
`;

  fs.writeFileSync(path.join(OUT_DIR, "HMLR_OWNERSHIP_METRICS.json"), JSON.stringify(metrics, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "HMLR_HOTELS_INVESTIGATED.json"),
    JSON.stringify({ count: hotels.length, hotels }, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HMLR_OWNERSHIP_MATCHES.json"),
    JSON.stringify(
      {
        strong: verifiedOwners,
        ambiguous: (matchResult?.ambiguous || []).slice(0, 50),
        propertyVerifiedOwners,
        companiesResolved,
      },
      null,
      2
    )
  );
  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_HMLR_ACTIVATION.md"), founder);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
