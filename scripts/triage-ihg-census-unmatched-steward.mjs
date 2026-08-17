#!/usr/bin/env node
/**
 * Steward-triage remaining unmatched CALA IHG Hotel Census rows.
 * Official sources only — no inventing URLs/Property IDs; no auto-apply here.
 *
 *   node scripts/triage-ihg-census-unmatched-steward.mjs
 *   node scripts/triage-ihg-census-unmatched-steward.mjs --probe-sitemap
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  IHG_FETCH_HEADERS,
  crawlIhgHoteldetailSitemaps,
  ihgCitySlugFromUrl,
  extractSitemapLocs,
} from "../lib/ihg-brand-directory-extract.js";
import {
  scoreIhgDirectoryAgainstCensus,
  normalizeIhgHotelNameForMatch,
  ihgBrandFamiliesAlign,
  IHG_PARENT_FORMULA,
  MAP_IHG_CENSUS_BACKFILL,
} from "../lib/hotel-census/plan-ihg-census-directory-match.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { nameSimilarity } from "../lib/independent-census/match-current-census.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

/** Markets with no IHG destinations/{slug}-hotels page (confirmed 404). */
const NO_DESTINATION_PAGE_COUNTRIES = new Set([
  "Belize",
  "Saint Kitts and Nevis",
  "Saint Lucia",
  "Curaçao",
  "Curacao",
  "Turks and Caicos",
  "British Virgin Islands",
  "US Virgin Islands",
  "Antigua and Barbuda",
  "Cuba",
  "Haiti",
  "Martinique",
  "Guadeloupe",
  "Bonaire",
]);

const ALLIANCE_NAME_RE = /\b(six senses|regent|iberostar|joia)\b/i;

const PIPELINE_HINT_RE =
  /\b(coming soon|pipeline|under construction|opening|pre[- ]opening|announced)\b/i;

/**
 * @param {string} line
 */
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

/**
 * @param {string} text
 */
function parseUnmatchedCsv(text) {
  const lines = String(text || "")
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);
  if (!lines.length) return [];
  const header = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    header.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

/**
 * @param {object[]} rows
 * @param {string[]} cols
 */
function toCsv(rows, cols) {
  const esc = (v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
}

/**
 * Load claimed Property IDs from apply logs + optional live Airtable.
 * @returns {Promise<Map<string, { censusRecordId: string, censusName: string, source: string }>>}
 */
async function loadClaimedPropertyIds() {
  /** @type {Map<string, { censusRecordId: string, censusName: string, source: string }>} */
  const claimed = new Map();
  for (const file of [
    "ihg-census-enrichment-apply-log.json",
    "ihg-census-enrichment-apply-log-prior.json",
  ]) {
    try {
      const j = JSON.parse(readFileSync(join(REPORTS, file), "utf8"));
      for (const r of j.rows || []) {
        const pid = String(r.propertyId || r.applyFields?.[CENSUS_PROPERTY_ID_FIELD] || "")
          .toUpperCase()
          .trim();
        if (!pid) continue;
        claimed.set(pid, {
          censusRecordId: r.censusRecordId || "",
          censusName: r.censusName || "",
          source: file,
        });
      }
    } catch {
      // missing log ok
    }
  }

  const base = getPlatformBase();
  if (base) {
    try {
      const records = await base(HOTEL_CENSUS_TABLE)
        .select({
          fields: [CENSUS_FIELDS.name, CENSUS_PROPERTY_ID_FIELD, CENSUS_FIELDS.affiliation],
          filterByFormula: IHG_PARENT_FORMULA,
          pageSize: 100,
        })
        .all();
      for (const rec of records) {
        const pid = rec.fields?.[CENSUS_PROPERTY_ID_FIELD];
        if (isBlankCensusValue(pid)) continue;
        const key = String(pid).toUpperCase();
        if (!claimed.has(key)) {
          claimed.set(key, {
            censusRecordId: rec.id,
            censusName: String(rec.fields?.[CENSUS_FIELDS.name] || ""),
            source: "airtable_live",
          });
        }
      }
    } catch (err) {
      console.warn("Airtable claimed-ID load failed:", err?.message || err);
    }
  }
  return claimed;
}

/**
 * Optional Affiliation / status from Airtable for unmatched record IDs.
 * @param {string[]} recordIds
 */
async function loadCensusMeta(recordIds) {
  /** @type {Map<string, { affiliation: string, status: string, city: string, name: string }>} */
  const meta = new Map();
  const base = getPlatformBase();
  if (!base || !recordIds.length) return meta;
  try {
    const orParts = recordIds.map((id) => `RECORD_ID()='${id}'`);
    // Airtable OR() max ~500 chars practical — chunk
    const chunkSize = 40;
    for (let i = 0; i < orParts.length; i += chunkSize) {
      const chunk = orParts.slice(i, i + chunkSize);
      const formula = `OR(${chunk.join(",")})`;
      const records = await base(HOTEL_CENSUS_TABLE)
        .select({
          fields: [
            CENSUS_FIELDS.name,
            CENSUS_FIELDS.affiliation,
            CENSUS_FIELDS.status,
            CENSUS_FIELDS.city,
            CENSUS_FIELDS.country,
          ],
          filterByFormula: formula,
          pageSize: 100,
        })
        .all();
      for (const rec of records) {
        meta.set(rec.id, {
          affiliation: String(rec.fields?.[CENSUS_FIELDS.affiliation] || ""),
          status: String(rec.fields?.[CENSUS_FIELDS.status] || ""),
          city: String(rec.fields?.[CENSUS_FIELDS.city] || ""),
          name: String(rec.fields?.[CENSUS_FIELDS.name] || ""),
        });
      }
    }
  } catch (err) {
    console.warn("Airtable meta load failed:", err?.message || err);
  }
  return meta;
}

/**
 * Distinctive tokens (≥5 chars) shared between census and directory names.
 * @param {string} a
 * @param {string} b
 */
const TOKEN_STOP =
  /^(hotel|inns?|suites?|express|holiday|crowne|plaza|collection|resort|mexico|ciudad|guadalajara|airport|aeropuerto|kimpton|indigo|intercontinental|staybridge|candlewood|vignette|voco|avid|atwell|regent|iberostar|waves|selection|downtown|centro|norte|north|south|sur|hotelera|financial|distrito|financiero|panama|cancun|tulum|merida|punta|cana|colombia|brazil|chile|ecuador|and|the|spa)$/i;

/** Extra triage-only synonyms (not used for auto-apply scoring). */
function normalizeIhgHotelNameForTriage(name) {
  return normalizeIhgHotelNameForMatch(name)
    .replace(/\bdistrito financiero\b/gi, "financial district")
    .replace(/\bzona centro\b/gi, "buenavista")
    .replace(/\bcd\.?\s*del\s+carmen\b/gi, "ciudad del carmen")
    .replace(/\bnavajoa\b/gi, "navojoa");
}

/**
 * Distinctive tokens (≥4 chars) shared between census and directory names.
 * Also returns multi-word place phrases (e.g. "mas olas", "zona rio").
 * @param {string} a
 * @param {string} b
 */
function sharedDistinctiveTokens(a, b) {
  const na = String(a || "").toLowerCase();
  const nb = String(b || "").toLowerCase();
  const tok = (s) =>
    new Set(
      s
        .split(/\s+/)
        .filter((w) => w.length >= 4)
        .filter((w) => !TOKEN_STOP.test(w))
    );
  const A = tok(na);
  const B = tok(nb);
  const shared = [...A].filter((t) => B.has(t));
  // Bigrams present in both (place-style); skip brand-noise pairs
  const junkBigram =
    /^(express_&|&_suites|inn_&|inn_express|hotel_indigo|avid_hotels|suites_guadalajara|inn_san|kimpton_mas)$/i;
  const bigrams = (s) => {
    const w = s.split(/\s+/).filter(Boolean);
    /** @type {string[]} */
    const out = [];
    for (let i = 0; i < w.length - 1; i++) {
      if (TOKEN_STOP.test(w[i]) && TOKEN_STOP.test(w[i + 1])) continue;
      const bg = `${w[i]}_${w[i + 1]}`;
      if (junkBigram.test(bg)) continue;
      // Require at least one non-stop token in the bigram
      if (TOKEN_STOP.test(w[i]) && TOKEN_STOP.test(w[i + 1])) continue;
      if (TOKEN_STOP.test(w[i]) || TOKEN_STOP.test(w[i + 1])) {
        // allow stop+place (e.g. mas_olas) when other token is distinctive
        const other = TOKEN_STOP.test(w[i]) ? w[i + 1] : w[i];
        if (other.length < 4) continue;
      }
      out.push(bg);
    }
    return out;
  };
  for (const bg of bigrams(na)) {
    const spaced = bg.replace(/_/g, " ");
    if (nb.includes(spaced) && !shared.includes(bg)) shared.push(bg);
  }
  return shared;
}

/**
 * Best directory near-miss for a census row.
 * Prefer higher nameSim; break ties toward unclaimed PIDs (actionable steward matches).
 * @param {object} census
 * @param {object[]} directoryRows
 * @param {Map<string, object>} claimed
 */
function bestNearMiss(census, directoryRows, claimed) {
  let best = null;
  const censusRow = {
    recordId: census.censusRecordId,
    name: census.censusName,
    country: census.censusCountry,
    city: census.city || "",
    fields: {},
  };
  for (const dir of directoryRows) {
    if (
      !dir.country ||
      String(dir.country).toLowerCase() !== String(census.censusCountry).toLowerCase()
    ) {
      continue;
    }
    const dName = dir.inferredHotelName || dir.name || "";
    const rawSim = nameSimilarity(
      normalizeIhgHotelNameForTriage(dName),
      normalizeIhgHotelNameForTriage(census.censusName)
    );
    const shared = sharedDistinctiveTokens(
      normalizeIhgHotelNameForTriage(census.censusName),
      normalizeIhgHotelNameForTriage(dName)
    );
    const brandOk = ihgBrandFamiliesAlign(census.censusName, dir.propertyUrl || "", dName);
    // Token bridge for steward reporting only (e.g. Mas Olas, Abitta) — not an apply gate.
    let sim = rawSim;
    if (brandOk && shared.length && rawSim < 0.55) {
      sim = Math.max(rawSim, shared.length >= 2 ? 0.62 : 0.52);
    }
    if (sim < 0.45 && !(brandOk && shared.length && rawSim >= 0.25)) continue;
    if (!brandOk && rawSim < 0.72) continue;
    const scored = scoreIhgDirectoryAgainstCensus(dir, censusRow);
    const pid = String(dir.propertyId || "").toUpperCase();
    const claim = claimed.get(pid);
    const cand = {
      propertyId: pid,
      directoryName: dName,
      propertyUrl: dir.propertyUrl || "",
      nameSim: sim,
      rawNameSim: rawSim,
      score: scored.score,
      confidence: scored.confidence,
      cityOk: scored.cityOk,
      brandOk,
      sharedTokens: shared.join("|"),
      claimedByRecordId: claim?.censusRecordId || "",
      claimedByName: claim?.censusName || "",
    };
    const better =
      !best ||
      cand.nameSim > best.nameSim + 0.02 ||
      (Math.abs(cand.nameSim - best.nameSim) <= 0.02 &&
        !cand.claimedByRecordId &&
        best.claimedByRecordId) ||
      (cand.nameSim === best.nameSim &&
        cand.claimedByRecordId === best.claimedByRecordId &&
        cand.score > best.score);
    if (better) best = cand;
  }
  return best;
}

/**
 * Classify one unmatched steward row.
 * @param {object} row
 * @param {object|null} near
 * @param {{ affiliation?: string, status?: string }} meta
 */
function classifyRow(row, near, meta) {
  const name = row.censusName || "";
  const country = row.censusCountry || "";
  const affiliation = meta.affiliation || "";
  const status = String(meta.status || "");
  const isPipeline =
    /pipeline/i.test(status) ||
    PIPELINE_HINT_RE.test(name) ||
    PIPELINE_HINT_RE.test(affiliation);

  // 1) Alliance / Six Senses — never invent from third parties
  if (
    ALLIANCE_NAME_RE.test(name) ||
    ALLIANCE_NAME_RE.test(affiliation) ||
    /\bsix senses\b/i.test(affiliation)
  ) {
    return {
      bucket: "alliance_not_on_ihg_directory",
      suggestedNextAction:
        "Leave stewarded — alliance/Six Senses not listed on ihg.com directories; do not scrape third parties for codes.",
    };
  }

  // 2) Wrong brand parent / obvious non-IHG census pollution
  if (/\bmarriott\b/i.test(name) || /\bhilton\b/i.test(name) || /\bhyatt\b/i.test(name)) {
    return {
      bucket: "name_variant_needs_human",
      suggestedNextAction:
        "Likely Parent Company / Affiliation mis-tag — confirm brand family and remove from IHG fill-blank queue if not IHG.",
    };
  }

  // 3) Duplicate PID already claimed (same brand family + strong name)
  if (
    near?.claimedByRecordId &&
    near.brandOk &&
    ((near.rawNameSim ?? near.nameSim) >= 0.85 ||
      ((near.rawNameSim ?? near.nameSim) >= 0.72 && near.cityOk))
  ) {
    return {
      bucket: "census_duplicate_pid_claimed",
      suggestedNextAction: `Property ID ${near.propertyId} already on ${near.claimedByRecordId} (${near.claimedByName}). Deduplicate or merge census rows; do not reassign PID.`,
    };
  }

  // 4) Markets with no destination country page
  if (NO_DESTINATION_PAGE_COUNTRIES.has(country)) {
    return {
      bucket: "no_destination_page_market",
      suggestedNextAction: `No IHG destinations page for ${country} (404). Recheck hoteldetail sitemap periodically; otherwise steward manually if property opens under IHG.`,
    };
  }

  // 5) Unclaimed official near-miss — human confirm (not auto-apply)
  const placeTokens = String(near?.sharedTokens || "")
    .split("|")
    .filter(Boolean)
    .filter((t) => !/^(center|santa|luis|carmen)$/i.test(t));
  const hasPlaceToken = placeTokens.some((t) => t.includes("_") || t.length >= 5);
  const actionableNear =
    near &&
    !near.claimedByRecordId &&
    near.brandOk &&
    ((near.rawNameSim ?? 0) >= 0.72 ||
      (hasPlaceToken && (near.rawNameSim ?? 0) >= 0.2 && near.nameSim >= 0.5) ||
      ((near.rawNameSim ?? 0) >= 0.55 && near.cityOk));
  if (actionableNear) {
    return {
      bucket: "name_variant_needs_human",
      suggestedNextAction: `Possible official match ${near.propertyId} (${near.directoryName}, sim=${Number(near.rawNameSim ?? near.nameSim).toFixed(2)}${near.sharedTokens ? `, tokens=${near.sharedTokens}` : ""}) — human confirm before apply; not safe for auto gate.`,
    };
  }

  // 6) Claimed near-miss with brand/name ambiguity (same city token or very strong name)
  if (
    near?.claimedByRecordId &&
    near.brandOk &&
    ((near.rawNameSim ?? 0) >= 0.78 ||
      (hasPlaceToken && (near.rawNameSim ?? 0) >= 0.6))
  ) {
    return {
      bucket: "name_variant_needs_human",
      suggestedNextAction: `Ambiguous near-miss ${near.propertyId} already claimed by ${near.claimedByRecordId} (${near.claimedByName}, sim=${Number(near.rawNameSim ?? near.nameSim).toFixed(2)}) — steward whether duplicate, brand variant, or distinct property.`,
    };
  }

  // 7) Pipeline / unopened
  if (isPipeline) {
    return {
      bucket: "pipeline_or_unopened",
      suggestedNextAction:
        "Treat as pipeline/unopened until an official ihg.com hoteldetail URL appears; then re-run directory sync.",
    };
  }

  return {
    bucket: "missing_from_public_listing",
    suggestedNextAction:
      "No safe official ihg.com match found. Re-extract after openings; steward Website/Property ID only from hoteldetail URLs.",
  };
}

async function probeSitemapGaps(directoryRows, unmatched) {
  const known = new Set(directoryRows.map((r) => String(r.propertyId || "").toUpperCase()));
  const cityHints = new Set();
  for (const u of unmatched) {
    const bits = String(u.censusName || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((w) => w.length >= 4);
    for (const w of bits) cityHints.add(w);
  }
  // Extra island / gap city tokens
  for (const w of [
    "belize",
    "kitts",
    "lucia",
    "baseterre",
    "basseterre",
    "isabela",
    "papagayo",
    "xala",
    "temozon",
    "sagesse",
    "kawana",
    "tulum",
    "tecate",
    "penasco",
    "fresnillo",
    "comitan",
    "funza",
    "valladolid",
    "navojoa",
    "navajoa",
    "allende",
  ]) {
    cityHints.add(w);
  }

  console.log("\n=== Probe hoteldetail sitemaps for unmatched city tokens ===\n");
  const crawl = await crawlIhgHoteldetailSitemaps({
    delayMs: 60,
    onProgress: (msg) => console.log(" ", msg),
  });
  const hotels = crawl.hotels || [];
  const novel = [];
  for (const h of hotels) {
    const pid = String(h.propertyId || "").toUpperCase();
    if (!pid || known.has(pid)) continue;
    const slug = String(h.citySlug || ihgCitySlugFromUrl(h.propertyUrl) || "").toLowerCase();
    const url = String(h.propertyUrl || "").toLowerCase();
    const hit = [...cityHints].some((tok) => slug.includes(tok) || url.includes(tok));
    if (!hit) continue;
    novel.push({
      propertyId: pid,
      citySlug: slug,
      propertyUrl: h.propertyUrl,
      brand: h.brand,
    });
  }
  console.log("Novel sitemap hotels matching unmatched tokens:", novel.length);
  console.log(JSON.stringify(novel.slice(0, 40), null, 2));
  return { novel, sitemapHotelCount: hotels.length };
}

async function main() {
  const args = process.argv.slice(2);
  const probeSitemap = args.includes("--probe-sitemap");
  mkdirSync(REPORTS, { recursive: true });

  const unmatchedPath = join(REPORTS, "ihg-census-unmatched-steward.csv");
  const unmatched = parseUnmatchedCsv(readFileSync(unmatchedPath, "utf8"));
  const dirJson = JSON.parse(readFileSync(join(REPORTS, "ihg-cala-directory-extract.json"), "utf8"));
  const directoryRows = dirJson.propertyRows || [];

  console.log("Unmatched rows:", unmatched.length);
  console.log("Directory rows:", directoryRows.length);

  const claimed = await loadClaimedPropertyIds();
  console.log("Claimed Property IDs:", claimed.size);

  const meta = await loadCensusMeta(unmatched.map((r) => r.censusRecordId));
  console.log("Airtable meta loaded:", meta.size);

  /** @type {object[]} */
  const triageRows = [];
  /** @type {Record<string, number>} */
  const countsByBucket = {};

  for (const row of unmatched) {
    const m = meta.get(row.censusRecordId) || {};
    const enriched = {
      ...row,
      city: m.city || "",
      censusName: row.censusName || m.name || "",
    };
    const near = bestNearMiss(enriched, directoryRows, claimed);
    const { bucket, suggestedNextAction } = classifyRow(enriched, near, m);
    countsByBucket[bucket] = (countsByBucket[bucket] || 0) + 1;

    triageRows.push({
      censusRecordId: row.censusRecordId,
      name: enriched.censusName,
      country: row.censusCountry,
      Affiliation: m.affiliation || "",
      status: m.status || "",
      reasonBucket: bucket,
      suggestedNextAction,
      nearMissPropertyId: near?.propertyId || "",
      nearMissDirectoryName: near?.directoryName || "",
      nearMissNameSim: near != null ? Number(Number(near.rawNameSim ?? near.nameSim).toFixed(3)) : "",
      nearMissBoostedSim: near != null ? Number(Number(near.nameSim).toFixed(3)) : "",
      nearMissScore: near?.score ?? "",
      nearMissConfidence: near?.confidence || "",
      nearMissBrandOk: near ? Boolean(near.brandOk) : "",
      nearMissSharedTokens: near?.sharedTokens || "",
      nearMissClaimedByRecordId: near?.claimedByRecordId || "",
      nearMissClaimedByName: near?.claimedByName || "",
      nearMissPropertyUrl: near?.propertyUrl || "",
    });
  }

  // Stable sort: bucket then country then name
  triageRows.sort(
    (a, b) =>
      String(a.reasonBucket).localeCompare(b.reasonBucket) ||
      String(a.country).localeCompare(b.country) ||
      String(a.name).localeCompare(b.name)
  );

  let sitemapProbe = null;
  if (probeSitemap) {
    sitemapProbe = await probeSitemapGaps(directoryRows, unmatched);
  } else {
    // Lightweight destinations.en.sitemap check for island keywords (no full hoteldetail crawl)
    try {
      const res = await fetch("https://www.ihg.com/services/sitemaps/destinations.en.sitemap.xml", {
        headers: IHG_FETCH_HEADERS,
      });
      const xml = await res.text();
      const locs = extractSitemapLocs(xml);
      const islandRe =
        /belize|kitts|lucia|curacao|turks|virgin-islands|antigua|cuba|haiti|martinique|guadeloupe|bonaire|sint-maarten|saint-martin/i;
      const islandHits = locs.filter((u) => islandRe.test(u));
      sitemapProbe = {
        destinationsSitemapHttp: res.status,
        islandDestinationHits: islandHits.length,
        islandDestinationSample: islandHits.slice(0, 20),
        note: "Full hoteldetail probe skipped (use --probe-sitemap). Island destination pages previously 404.",
      };
      console.log("Island destination sitemap hits:", islandHits.length);
    } catch (err) {
      sitemapProbe = { error: String(err?.message || err) };
    }
  }

  const cols = [
    "censusRecordId",
    "name",
    "country",
    "Affiliation",
    "status",
    "reasonBucket",
    "suggestedNextAction",
    "nearMissPropertyId",
    "nearMissDirectoryName",
    "nearMissNameSim",
    "nearMissBoostedSim",
    "nearMissScore",
    "nearMissConfidence",
    "nearMissBrandOk",
    "nearMissSharedTokens",
    "nearMissClaimedByRecordId",
    "nearMissClaimedByName",
    "nearMissPropertyUrl",
  ];

  const csvPath = join(REPORTS, "ihg-census-unmatched-steward-triage.csv");
  const jsonPath = join(REPORTS, "ihg-census-unmatched-steward-triage.json");
  writeFileSync(csvPath, toCsv(triageRows, cols) + "\n", "utf8");

  const summary = {
    generatedAt: new Date().toISOString(),
    unmatchedCount: triageRows.length,
    directoryHotels: directoryRows.length,
    claimedPropertyIds: claimed.size,
    airtableMetaLoaded: meta.size,
    newSafeApplies: 0,
    recoverySkippedReason:
      "No new official destination pages for remaining island markets (404). Near-misses either PID-claimed (duplicates) or below apply gate (name variants). Six Senses/alliance left stewarded. No live apply this pass.",
    countsByBucket,
    fieldMapping: MAP_IHG_CENSUS_BACKFILL,
    sitemapProbe,
    topStewardActions: [
      "Deduplicate strong same-brand claimed PIDs (Grand Cayman GCMSM, Santo Domingo DO SDQEX, Avid Fresnillo ZCLAV, Kimpton Virgilio MEXPL).",
      "Human-confirm unclaimed variants: Kimpton Mas Olas↔SJDTD, Abitta↔SJUCC, Royal Haciendas↔PCMRH, Panama Financial↔PCYEX, Buenavista/Zona Centro↔MEXBU.",
      "Leave Six Senses / alliance brands stewarded; no third-party code scrape.",
      "Pipeline Mexico/Colombia/DR/Costa Rica openings: wait for official hoteldetail then re-run extract+sync.",
      "Fix Marriott Buenos Aires Ezeiza Parent Company / Affiliation (not IHG).",
      "Island markets without destination pages (St Kitts): monitor hoteldetail sitemap only.",
    ],
    rows: triageRows,
  };
  writeFileSync(jsonPath, JSON.stringify(summary, null, 2) + "\n", "utf8");

  console.log("\n=== Bucket counts ===");
  console.log(JSON.stringify(countsByBucket, null, 2));
  console.log("\nWrote:", csvPath);
  console.log("Wrote:", jsonPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
