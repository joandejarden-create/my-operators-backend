#!/usr/bin/env node
/**
 * HIF V6 Track B — Licensed CCOD + OCOD activation (November 2026 full baselines).
 * Downloads FULL datasets via ULPD (licence-accepted account). Streams CSV matching.
 * Never prints API keys or signed download URLs. Raw zips stay in local gitignored cache.
 *
 * Usage:
 *   node scripts/hif-v6-track-b-ccod-ocod-activation.mjs --run
 *   node scripts/hif-v6-track-b-ccod-ocod-activation.mjs --run --pilot-only
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createReadStream, createWriteStream } from "node:fs";
import { createInterface } from "node:readline";
import { execFileSync } from "node:child_process";
import { pipeline } from "node:stream/promises";
import { createHash } from "node:crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-hmlr-ccod-ocod");
const CACHE = path.join(ROOT, "data/hmlr-ulpd"); // gitignored — licensed raw data
const ULPD = "https://use-land-property-data.service.gov.uk/api/v1";
const RUN = process.argv.includes("--run");
const PILOT_ONLY = process.argv.includes("--pilot-only");
const REUSE_CACHE = process.argv.includes("--reuse-cache");
const asOf = new Date().toISOString().slice(0, 10);

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

function loadEnvFile(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*)$/);
    if (!m) continue;
    let v = m[2].trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
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

function classifyHttp(status, bodyText) {
  if (status == null) return "NETWORK_FAILURE";
  if (status === 429) return "RATE_LIMIT";
  if (status >= 500) return "SERVER_ERROR";
  if (status === 401 || status === 403) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) return "AUTH_INVALID_KEY";
    if (/need to provide your API Key/i.test(bodyText)) return "AUTH_MISSING_KEY";
    if (/not yet signed the licence/i.test(bodyText)) return "LICENCE_NOT_SIGNED";
    if (/not paid for access/i.test(bodyText)) return "PAID_DATASET_REQUIRED";
    if (/do not have access/i.test(bodyText)) return "PERMISSION_DENIED";
    return "PERMISSION_OR_FORBIDDEN";
  }
  if (status === 404) {
    if (/API Key provided is incorrect|does not exist/i.test(bodyText)) return "AUTH_INVALID_KEY";
    return "NOT_FOUND";
  }
  if (status >= 200 && status < 300) return "OK";
  return `HTTP_${status}`;
}

async function ulpdGet(key, pathname) {
  const started = Date.now();
  try {
    const res = await fetch(`${ULPD}${pathname}`, {
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

function normalizePostcode(pc) {
  if (!pc) return "";
  const raw = String(pc)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  if (raw.length < 5) return raw;
  return `${raw.slice(0, -3)} ${raw.slice(-3)}`;
}

function pcKey(pc) {
  return normalizePostcode(pc).replace(/\s+/g, "");
}

function normalizeAddr(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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

async function sha256File(fp) {
  const h = createHash("sha256");
  for await (const chunk of createReadStream(fp)) h.update(chunk);
  return h.digest("hex");
}

/** Eight curated England/Wales HE-style seeds + optional expansion from prior artifacts. */
function loadHotels() {
  const pilot = [
    {
      name: "The Savoy",
      address: "Strand, London",
      postcode: "WC2R 0EZ",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "Claridge's",
      address: "Brook Street, Mayfair, London",
      postcode: "W1K 4HR",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "The Langham London",
      address: "1C Portland Place, London",
      postcode: "W1B 1JA",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "Hilton London Bankside",
      address: "2-8 Great Suffolk Street, London",
      postcode: "SE1 0UG",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "Park Plaza Westminster Bridge London",
      address: "200 Westminster Bridge Road, London",
      postcode: "SE1 7UT",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "Manchester Airport Marriott Hotel",
      address: "Hale Road, Hale Barns, Altrincham",
      postcode: "WA15 8XW",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "Hilton Manchester Deansgate",
      address: "303 Deansgate, Manchester",
      postcode: "M3 4LQ",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
    {
      name: "The Grand Hotel Birmingham",
      address: "Colmore Row, Birmingham",
      postcode: "B3 2DA",
      city: "Birmingham",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: true,
    },
  ];

  const expansion = [
    {
      name: "The Ritz London",
      address: "150 Piccadilly, London",
      postcode: "W1J 9BR",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Shangri-La The Shard London",
      address: "31 St Thomas Street, London",
      postcode: "SE1 9QU",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Hotel Café Royal",
      address: "68 Regent Street, London",
      postcode: "W1B 4DY",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "The Midland Hotel Manchester",
      address: "16 Peter Street, Manchester",
      postcode: "M60 2DS",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Radisson Blu Edwardian Manchester",
      address: "Peter Street, Manchester",
      postcode: "M2 5GP",
      city: "Manchester",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Hilton London Metropole",
      address: "225 Edgware Road, London",
      postcode: "W2 1JU",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Novotel London West",
      address: "1 Shortlands, Hammersmith, London",
      postcode: "W6 8DR",
      city: "London",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
    {
      name: "Jury's Inn Birmingham",
      address: "245 Broad Street, Birmingham",
      postcode: "B1 2HQ",
      city: "Birmingham",
      identitySource: "CURATED_PUBLIC_ADDRESS",
      pilot: false,
    },
  ];

  const hotels = PILOT_ONLY ? pilot : [...pilot, ...expansion];
  return hotels.map((h) => ({
    ...h,
    country: "GB",
    postcodeNorm: normalizePostcode(h.postcode),
    postcodeKey: pcKey(h.postcode),
  }));
}

function nameTokens(hotelName) {
  return normalizeAddr(hotelName)
    .replace(/^(the|hotel|hilton|marriott|park plaza|radisson blu|novotel|jurys inn|jury s inn)\s+/g, "")
    .split(" ")
    .filter((w) => w.length > 2 && !/^(the|and|hotel|inn|suites|london|manchester|birmingham)$/.test(w));
}

function streetTokens(address) {
  return normalizeAddr(address)
    .split(" ")
    .filter((w) => w.length > 2 && !/^(london|manchester|birmingham|mayfair|altrincham|hammersmith)$/.test(w));
}

function isAncillaryTitle(propAddr) {
  return /\b(room|apartment|flat|unit|suite\s+\d|transformer|chamber|car\s*park|parking|kiosk|shop|store|office|floor premises|fifth floor|ground floor restaurant|restaurant (unit|premises)|basement|communications?\s+site|telecommunications?\s+apparatus|mast|antenna)\b/i.test(
    propAddr,
  );
}

function isPrimaryHotelBuilding(propAddr, hotel) {
  const addrN = normalizeAddr(propAddr);
  if (isAncillaryTitle(propAddr)) return false;
  const hotelCore = normalizeAddr(hotel.name)
    .replace(/^the\s+/, "")
    .replace(/\b(london|manchester|birmingham|uk|england)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  // Full trading name (with city) in property description
  const hotelFull = normalizeAddr(hotel.name).replace(/^the\s+/, "");
  if (hotelFull.length >= 8 && addrN.includes(hotelFull)) return true;
  // Core name + hotel lexicon (e.g. "Langham" + "Hotel")
  if (hotelCore.length >= 5 && addrN.includes(hotelCore) && /\b(hotel|inn|suites|resort)\b/i.test(propAddr)) {
    return true;
  }
  // Distinctive multi-word core without city (e.g. "hilton metropole") even if "Hotel" omitted on title
  if (hotelCore.split(" ").length >= 2 && hotelCore.length >= 10 && addrN.includes(hotelCore)) {
    return true;
  }
  // e.g. "Claridge's Hotel", "The Langham Hotel", "Park Plaza Westminster Bridge"
  const tokens = nameTokens(hotel.name).filter(
    (t) => !/^(london|manchester|birmingham|bridge|airport)$/i.test(t),
  );
  if (tokens.length >= 1 && tokens.every((t) => addrN.includes(t)) && /\bhotel\b/i.test(propAddr)) {
    // Single distinctive token (Langham, Claridge) + "Hotel" in property description
    if (tokens.length === 1 && tokens[0].length >= 6) return true;
    if (tokens.length >= 2) return true;
  }
  return false;
}

function classifyMatch(hotel, propAddr, tenure = null) {
  const addrN = normalizeAddr(propAddr);
  const hotelN = normalizeAddr(hotel.name).replace(/^the\s+/, "");
  const tokens = nameTokens(hotel.name);
  const streets = streetTokens(hotel.address);
  const streetHit = streets.some((s) => addrN.includes(s) && s.length >= 4);
  const hotelWord = /\bhotel\b/i.test(propAddr);
  const primary = isPrimaryHotelBuilding(propAddr, hotel);
  const weakName =
    tokens.some((t) => t.length >= 5 && addrN.includes(t)) ||
    (hotelN.length >= 5 && addrN.includes(hotelN.split(" ")[0]));

  if (primary) {
    return {
      matchClass: "VERIFIED",
      verificationClass: "VERIFIED_PRIMARY_HOTEL_TITLE",
      primaryHotelTitle: true,
      note: `Primary hotel building description matched (${tenure || "tenure n/a"}); proprietor ≠ operator unless separately evidenced`,
    };
  }
  if (isAncillaryTitle(propAddr) && (weakName || hotelWord)) {
    return {
      matchClass: "REVIEW_REQUIRED",
      verificationClass: "REVIEW_REQUIRED_ANCILLARY_UNIT_TITLE",
      primaryHotelTitle: false,
      note: "Ancillary unit/room/floor title at hotel postcode — not the primary hotel freehold/leasehold",
    };
  }
  if (weakName && streetHit && hotelWord) {
    return {
      matchClass: "REVIEW_REQUIRED",
      verificationClass: "REVIEW_REQUIRED_ADDRESS_PARTIAL",
      primaryHotelTitle: false,
      note: "Partial name/street/hotel evidence — corroborate before treating as primary ownership",
    };
  }
  if (streetHit || weakName) {
    return {
      matchClass: "REVIEW_REQUIRED",
      verificationClass: "REVIEW_REQUIRED_POSTCODE_WEAK",
      primaryHotelTitle: false,
      note: "Weak postcode-area hit — insufficient alone for ownership verification",
    };
  }
  return {
    matchClass: "REVIEW_REQUIRED",
    verificationClass: "REVIEW_REQUIRED_POSTCODE_ONLY",
    primaryHotelTitle: false,
    note: "Postcode-only — insufficient alone for ownership verification",
  };
}

async function downloadFullDataset(key, dataset, preferredFile) {
  const meta = await ulpdGet(key, `/datasets/${dataset}`);
  if (meta.errClass !== "OK") {
    return { ok: false, dataset, stage: "metadata", errClass: meta.errClass, error: meta.error };
  }
  const result = meta.parsed?.result || {};
  const resources = result.resources || [];
  const full =
    resources.find((r) => r.file_name === preferredFile) ||
    resources.find((r) => /FULL_2026_11/i.test(r.file_name || "")) ||
    resources.find((r) => /FULL/i.test(r.file_name || ""));
  if (!full?.file_name) {
    return {
      ok: false,
      dataset,
      stage: "no_full_resource",
      errClass: "NOT_FOUND",
      error: `Preferred ${preferredFile} not listed`,
      available: resources.map((r) => r.file_name),
    };
  }

  const ticket = await ulpdGet(key, `/datasets/${dataset}/${encodeURIComponent(full.file_name)}`);
  if (ticket.errClass !== "OK") {
    return {
      ok: false,
      dataset,
      stage: "download_ticket",
      file: full.file_name,
      errClass: ticket.errClass,
      error: ticket.error,
      status: ticket.status,
    };
  }
  const downloadUrl = ticket.parsed?.result?.download_url;
  if (!downloadUrl) {
    return {
      ok: false,
      dataset,
      stage: "download_ticket",
      file: full.file_name,
      errClass: "SCHEMA_ERROR",
      error: "Missing download_url",
    };
  }

  fs.mkdirSync(CACHE, { recursive: true });
  const zipPath = path.join(CACHE, full.file_name);
  const extractDir = path.join(CACHE, `${dataset}_full_extract`);
  if (REUSE_CACHE && fs.existsSync(zipPath)) {
    let csv = null;
    if (fs.existsSync(extractDir)) {
      csv = fs.readdirSync(extractDir).find((f) => f.toLowerCase().endsWith(".csv"));
    }
    if (!csv) {
      fs.rmSync(extractDir, { recursive: true, force: true });
      fs.mkdirSync(extractDir, { recursive: true });
      execFileSync(
        "powershell.exe",
        [
          "-NoProfile",
          "-Command",
          `Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${extractDir.replace(/'/g, "''")}' -Force`,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      csv = fs.readdirSync(extractDir).find((f) => f.toLowerCase().endsWith(".csv"));
    }
    if (csv) {
      return {
        ok: true,
        dataset,
        stage: "ready_cached",
        file: full.file_name,
        zipPathRel: path.relative(ROOT, zipPath).replace(/\\/g, "/"),
        csvPath: path.join(extractDir, csv),
        csvName: csv,
        bytes: fs.statSync(zipPath).size,
        sha256: await sha256File(zipPath),
        reportingPeriod: "2026-11",
        licenceId: result.licence_id || dataset,
        fee: result.fee || null,
        title: result.title || null,
        lastUpdated: result.last_updated || null,
        reusedCache: true,
      };
    }
  }
  // Stream immediately — signed URL expires ~10s
  const res = await fetch(downloadUrl);
  if (!res.ok || !res.body) {
    return {
      ok: false,
      dataset,
      stage: "download_bytes",
      file: full.file_name,
      errClass: `HTTP_${res.status}`,
      error: `Download failed HTTP ${res.status}`,
    };
  }
  // Node fetch may return a Web ReadableStream — convert for pipeline
  const nodeBody =
    typeof res.body?.getReader === "function"
      ? (await import("node:stream")).Readable.fromWeb(res.body)
      : res.body;
  await pipeline(nodeBody, createWriteStream(zipPath));
  const bytes = fs.statSync(zipPath).size;
  const sha256 = await sha256File(zipPath);

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
      { stdio: ["ignore", "pipe", "pipe"] },
    );
  } catch (e) {
    return {
      ok: false,
      dataset,
      stage: "extract",
      file: full.file_name,
      errClass: "EXTRACT_FAILED",
      error: String(e?.message || e).slice(0, 300),
    };
  }
  const csv = fs.readdirSync(extractDir).find((f) => f.toLowerCase().endsWith(".csv"));
  if (!csv) {
    return {
      ok: false,
      dataset,
      stage: "extract",
      file: full.file_name,
      errClass: "SCHEMA_ERROR",
      error: "No CSV in zip",
    };
  }

  return {
    ok: true,
    dataset,
    stage: "ready",
    file: full.file_name,
    zipPathRel: path.relative(ROOT, zipPath).replace(/\\/g, "/"),
    csvPath: path.join(extractDir, csv),
    csvName: csv,
    bytes,
    sha256,
    reportingPeriod: "2026-11",
    licenceId: result.licence_id || dataset,
    fee: result.fee || null,
    title: result.title || null,
    lastUpdated: result.last_updated || null,
    // never persist download_url
  };
}

function extractProprietors(row) {
  const proprietors = [];
  for (let i = 1; i <= 4; i++) {
    const name = row[`Proprietor Name (${i})`] || row[`Proprietor Name ${i}`];
    if (!name) continue;
    proprietors.push({
      name: String(name).trim(),
      companyNumber:
        String(
          row[`Company Registration No. (${i})`] || row[`Company Registration No. ${i}`] || "",
        ).trim() || null,
      category: row[`Proprietorship Category (${i})`] || null,
      address1: row[`Proprietor (${i}) Address (1)`] || null,
    });
  }
  return proprietors;
}

async function streamMatch(csvPath, hotels, dataset) {
  const byPc = new Map();
  for (const h of hotels) {
    if (!h.postcodeKey) continue;
    if (!byPc.has(h.postcodeKey)) byPc.set(h.postcodeKey, []);
    byPc.get(h.postcodeKey).push(h);
  }

  const hits = [];
  let rowsScanned = 0;
  let hotelLexiconRows = 0;
  const rl = createInterface({ input: createReadStream(csvPath, { encoding: "utf8" }) });
  let headers = null;
  for await (const line of rl) {
    if (!headers) {
      headers = parseCsvLine(line).map((h) => h.trim());
      continue;
    }
    rowsScanned += 1;
    if (rowsScanned % 500000 === 0) {
      process.stderr.write(`[${dataset}] scanned ${rowsScanned} rows...\n`);
    }
    const cols = parseCsvLine(line);
    const row = Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? ""]));
    const propAddr = row["Property Address"] || row["Property address"] || "";
    if (/hotel|inn |suites|resort|lodge/i.test(propAddr)) hotelLexiconRows += 1;
    const postcode = normalizePostcode(row.Postcode || row.postcode || "");
    const key = pcKey(postcode);
    if (!key || !byPc.has(key)) continue;

    const titleNumber = row["Title Number"] || row["Title number"] || null;
    const tenure = row.Tenure || null;
    const proprietors = extractProprietors(row);
    const dateAdded = row["Date Proprietor Added"] || null;

    for (const h of byPc.get(key)) {
      const cls = classifyMatch(h, propAddr, tenure);
      hits.push({
        hotelName: h.name,
        hotelAddress: h.address,
        hotelPostcode: h.postcodeNorm,
        hotelPilot: Boolean(h.pilot),
        identitySource: h.identitySource,
        dataset,
        titleNumber,
        tenure,
        propertyAddress: propAddr,
        postcode,
        proprietors,
        dateProprietorAdded: dateAdded,
        matchClass: cls.matchClass,
        verificationClass: cls.verificationClass,
        primaryHotelTitle: Boolean(cls.primaryHotelTitle),
        matchNote: cls.note,
        proprietorIsNotOperator:
          "Registered proprietor is legal title holder — not automatically the hotel operator, franchisor, franchisee, or UBO",
      });
    }
  }

  return { rowsScanned, hotelLexiconRows, hits };
}

function consolidateByHotel(hotels, allHits) {
  const byHotel = new Map();
  for (const h of hotels) {
    byHotel.set(h.name, {
      hotel: h,
      titles: [],
      primaryTitles: [],
      matchClass: "UNRESOLVED",
      verificationClass: "UNRESOLVED_NO_TITLE_HIT",
    });
  }
  for (const hit of allHits) {
    const rec = byHotel.get(hit.hotelName);
    if (!rec) continue;
    rec.titles.push(hit);
    if (hit.primaryHotelTitle && hit.matchClass === "VERIFIED") rec.primaryTitles.push(hit);
  }
  for (const rec of byHotel.values()) {
    if (!rec.titles.length) continue;
    // Drop residual ancillary titles that slipped classification
    rec.primaryTitles = (rec.primaryTitles || []).filter(
      (t) => !isAncillaryTitle(t.propertyAddress || ""),
    );
    const primary = rec.primaryTitles;
    const freeholdPrimary = primary.filter((t) => /freehold/i.test(String(t.tenure || "")));
    if (primary.length === 1) {
      rec.matchClass = "VERIFIED";
      rec.verificationClass = "VERIFIED_SINGLE_PRIMARY_HOTEL_TITLE";
      rec.primaryProprietors = primary[0].proprietors;
    } else if (freeholdPrimary.length === 1) {
      rec.matchClass = "VERIFIED";
      rec.verificationClass = "VERIFIED_PRIMARY_FREEHOLD_AMONG_RELATED_TITLES";
      rec.primaryProprietors = freeholdPrimary[0].proprietors;
      rec.note = "Primary freehold hotel title identified; additional related titles retained for review";
    } else if (primary.length > 1) {
      rec.matchClass = "REVIEW_REQUIRED";
      rec.verificationClass = "REVIEW_REQUIRED_MULTIPLE_PRIMARY_TITLES";
      rec.note = "Multiple primary hotel-building titles (e.g. freehold + headlease) — review before single-owner claim";
      rec.primaryProprietors = primary.flatMap((t) => t.proprietors || []);
    } else if (rec.titles.some((t) => t.matchClass === "REVIEW_REQUIRED")) {
      rec.matchClass = "REVIEW_REQUIRED";
      rec.verificationClass = "REVIEW_REQUIRED_NO_PRIMARY_HOTEL_TITLE";
      rec.note = "Postcode/area hits without a primary hotel-building title description";
    }
  }
  return [...byHotel.values()];
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.mkdirSync(CACHE, { recursive: true });

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, cache: CACHE }, null, 2));
    return;
  }

  const { hmlrKey, chKey } = resolveSecrets();
  const hotels = loadHotels();

  const auth = {
    keyPresent: Boolean(hmlrKey),
    keyLength: hmlrKey.length,
    companiesHouseApiKeyPresent: Boolean(chKey),
    keySourceHint: process.env.HM_LAND_REGISTRY_KEY ? "process.env" : hmlrKey ? "dotenv_file" : "absent",
  };

  if (!hmlrKey) {
    writeJson(path.join(OUT, "ACTIVATION_METRICS.json"), {
      ...auth,
      outcome: "KEY_ABSENT",
      researchCostUsd: 0,
    });
    process.exit(2);
  }

  const list = await ulpdGet(hmlrKey, "/datasets");
  auth.datasetsListOk = list.errClass === "OK";
  auth.outcome = list.errClass === "OK" ? "AUTHENTICATED_OK" : list.errClass;

  // Explicit FULL Nov 2026 downloads (do not fall back to example.csv)
  const ccod = await downloadFullDataset(hmlrKey, "ccod", "CCOD_FULL_2026_11.zip");
  const ocod = await downloadFullDataset(hmlrKey, "ocod", "OCOD_FULL_2026_11.zip");

  writeJson(path.join(OUT, "DOWNLOAD_MANIFEST.json"), {
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    note: "Raw licensed ZIPs/CSVs stored only under data/hmlr-ulpd (gitignored). Signed URLs never persisted.",
    ccod: ccod.ok
      ? {
          ok: true,
          file: ccod.file,
          bytes: ccod.bytes,
          sha256: ccod.sha256,
          csvName: ccod.csvName,
          reportingPeriod: ccod.reportingPeriod,
          licenceId: ccod.licenceId,
          fee: ccod.fee,
          localCacheRelative: ccod.zipPathRel,
        }
      : {
          ok: false,
          errClass: ccod.errClass,
          error: ccod.error,
          stage: ccod.stage,
          available: ccod.available || null,
        },
    ocod: ocod.ok
      ? {
          ok: true,
          file: ocod.file,
          bytes: ocod.bytes,
          sha256: ocod.sha256,
          csvName: ocod.csvName,
          reportingPeriod: ocod.reportingPeriod,
          licenceId: ocod.licenceId,
          fee: ocod.fee,
          localCacheRelative: ocod.zipPathRel,
        }
      : {
          ok: false,
          errClass: ocod.errClass,
          error: ocod.error,
          stage: ocod.stage,
          available: ocod.available || null,
        },
    licenceNotes: {
      internalResearch: "Authorized for internal Dealality research under accepted ULPD CCOD/OCOD licences",
      customerFacingRedistribution: "PROHIBITED without separate licence review — CUSTOMER_BLOCKED",
      derivedData: "Aggregated match reports OK internally; do not republish raw CCOD/OCOD rows to customers",
      paidTitlePurchases: "None — Business Gateway official copies not authorized",
    },
  });

  const allHits = [];
  const scanStats = {};
  if (ccod.ok) {
    const r = await streamMatch(ccod.csvPath, hotels, "CCOD");
    scanStats.ccod = { rowsScanned: r.rowsScanned, hotelLexiconRows: r.hotelLexiconRows, hits: r.hits.length };
    allHits.push(...r.hits);
  }
  if (ocod.ok) {
    const r = await streamMatch(ocod.csvPath, hotels, "OCOD");
    scanStats.ocod = { rowsScanned: r.rowsScanned, hotelLexiconRows: r.hotelLexiconRows, hits: r.hits.length };
    allHits.push(...r.hits);
  }

  const consolidated = consolidateByHotel(hotels, allHits);
  const pilot = consolidated.filter((c) => c.hotel.pilot);
  const expansion = consolidated.filter((c) => !c.hotel.pilot);

  const verifiedHits = allHits.filter((h) => h.matchClass === "VERIFIED" && h.primaryHotelTitle);
  const companyNumbers = [
    ...new Set(
      verifiedHits.flatMap((h) => (h.proprietors || []).map((p) => p.companyNumber).filter(Boolean)),
    ),
  ];
  const verifiedHotels = consolidated.filter((c) => c.matchClass === "VERIFIED");

  const metrics = {
    track: "TRACK_B",
    wave: "CCOD_OCOD_LICENSED_ACTIVATION",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    auth,
    downloads: {
      ccodOk: Boolean(ccod.ok),
      ocodOk: Boolean(ocod.ok),
      ccodFile: ccod.file || null,
      ocodFile: ocod.file || null,
      ccodBytes: ccod.bytes || null,
      ocodBytes: ocod.bytes || null,
      ccodRowsScanned: scanStats.ccod?.rowsScanned || 0,
      ocodRowsScanned: scanStats.ocod?.rowsScanned || 0,
      reusedCache: Boolean(ccod.reusedCache || ocod.reusedCache),
    },
    hotelsEvaluated: hotels.length,
    pilotHotels: pilot.length,
    expansionHotels: expansion.length,
    verifiedPrimaryTitleHits: verifiedHits.length,
    hotelsWithVerifiedMatch: verifiedHotels.length,
    hotelsReviewRequired: consolidated.filter((c) => c.matchClass === "REVIEW_REQUIRED").length,
    hotelsUnresolved: consolidated.filter((c) => c.matchClass === "UNRESOLVED").length,
    multipleTitleHotels: consolidated.filter((c) => (c.titles || []).length > 1).length,
    corporateIdentifiersRecovered: companyNumbers.length,
    verifiedHotelProprietorExamples: verifiedHotels.slice(0, 12).map((c) => ({
      hotel: c.hotel.name,
      verificationClass: c.verificationClass,
      primaryTitles: (c.primaryTitles || []).map((t) => ({
        titleNumber: t.titleNumber,
        tenure: t.tenure,
        propertyAddress: t.propertyAddress,
        proprietors: t.proprietors,
        dataset: t.dataset,
      })),
    })),
    companiesHouseApiKeyPresent: Boolean(chKey),
    companiesHouseEnrichment:
      chKey
        ? "KEY_PRESENT_LOOKUP_PREPARED_NOT_BLOCKING"
        : "KEY_ABSENT_LAND_REGISTRY_MATCHING_NOT_BLOCKED",
    notCountedAsOpportunities: true,
    notCountedAsFranchiseOrManagementExpirations: true,
    scanStats,
  };

  writeJson(path.join(OUT, "ACTIVATION_METRICS.json"), metrics);
  writeJson(path.join(OUT, "PILOT_MATCHES.json"), {
    count: pilot.length,
    hotels: pilot,
  });
  writeJson(path.join(OUT, "EXPANSION_MATCHES.json"), {
    count: expansion.length,
    hotels: expansion,
  });
  writeJson(path.join(OUT, "ALL_TITLE_HITS.json"), {
    count: allHits.length,
    hits: allHits,
  });
  writeJson(path.join(OUT, "CORPORATE_IDENTIFIERS.json"), {
    count: companyNumbers.length,
    companyNumbers,
    enrichmentLinks: companyNumbers.map((n) => ({
      companyNumber: n,
      companiesHousePublicUrl: `https://find-and-update.company-information.service.gov.uk/company/${n}`,
    })),
  });

  // Weekly scorecard snapshot (UK ownership — not opportunity counts)
  const scorecardDir = path.join(ROOT, "reports/sec-edgar-hif-v6-weekly-scorecard/snapshots");
  fs.mkdirSync(scorecardDir, { recursive: true });
  writeJson(path.join(scorecardDir, "2026-W47-track-b-ccod-ocod-activation.json"), {
    week: "2026-W47",
    track: "TRACK_B",
    sourceFamily: "HMLR_CCOD_OCOD",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    ukHotelsEvaluated: metrics.hotelsEvaluated,
    strongTitleAddressMatches: metrics.hotelsWithVerifiedMatch,
    verifiedLegalRegisteredProprietorHits: metrics.verifiedTitleHits,
    multipleTitleOrAmbiguous: metrics.hotelsReviewRequired + metrics.multipleTitleHotels,
    corporateIdentifiersRecovered: metrics.corporateIdentifiersRecovered,
    datasetRowsAreNotOpportunities: true,
    leaseholdFreeholdNotFranchiseOrHma: true,
    metrics,
  });

  const md = `# HIF V6 Track B — Licensed CCOD + OCOD Activation

**As of:** ${asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**PR #513:** MERGED (\`58b9382ad\`) — this run activates licensed downloads after founder licence acceptance.

## 1–2) Authenticated downloads

| Dataset | Result | File | Bytes | Rows scanned |
|---------|--------|------|------:|-------------:|
| CCOD FULL | **${ccod.ok ? "OK" : ccod.errClass}** | ${ccod.file || "—"} | ${ccod.bytes ?? "—"} | ${scanStats.ccod?.rowsScanned ?? "—"} |
| OCOD FULL | **${ocod.ok ? "OK" : ocod.errClass}** | ${ocod.file || "—"} | ${ocod.bytes ?? "—"} | ${scanStats.ocod?.rowsScanned ?? "—"} |

- Auth: **${auth.outcome}** (key present, length ${auth.keyLength}; value not logged)
- Raw files: local \`data/hmlr-ulpd/\` only (gitignored)
- Signed download URLs: **never persisted**
- NPS / Business Gateway / paid titles: **not used**

## 3) Baseline ingestion
Reporting period **2026-11**. Change-only (COU) files are for subsequent monthly deltas — FULL used for baseline.

## 4) Eight-hotel pilot

| Hotel | Match class | Titles |
|-------|-------------|-------:|
${pilot
  .map(
    (p) =>
      `| ${p.hotel.name} | **${p.matchClass}** | ${(p.titles || []).length} |`,
  )
  .join("\n")}

## 5–7) Verified proprietors / expansion / unresolved

- Hotels with VERIFIED match: **${metrics.hotelsWithVerifiedMatch}**
- REVIEW_REQUIRED: **${metrics.hotelsReviewRequired}**
- UNRESOLVED: **${metrics.hotelsUnresolved}**
- Title-level VERIFIED hits: **${metrics.verifiedTitleHits}**
- Corporate registration numbers recovered: **${metrics.corporateIdentifiersRecovered}**
- Expansion hotels evaluated: **${metrics.expansionHotels}**

Registered proprietor ≠ operator / franchisor / franchisee / UBO unless separately evidenced.

## 8–10) Cost, licence, monthly process

- Cost: **$0**
- Licence: free ULPD CCOD/OCOD — internal research OK; **CUSTOMER_BLOCKED** / no raw redistribution
- Monthly: download \`*_COU_YYYY_MM.zip\` after baseline; do **not** treat COU as complete inventory

## 11) Integration

Use Track B registered workspace. Open a new PR for this activation (IC owns merge). Preserve dirty product trees.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_CCOD_OCOD_ACTIVATION.md"), md);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
