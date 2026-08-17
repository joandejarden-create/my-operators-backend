/**
 * Blind benchmark runner — Contradiction-First Status/Affiliation Checker V1.
 *
 * Sequence (default):
 *  1. Snapshot Dealality census values for Test-6 brand set (Mexico/CALA)
 *  2. Run native checker (official directories only — no Webhound)
 *  3. Freeze native results
 *
 * Comparison flag (ONLY after freeze):
 *  --compare-webhound
 *
 * Never writes Airtable. Never launches Webhound.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  checkHotelFreshness,
  findDirectoryGaps,
  loadIhgDirectoryRows,
  loadMarriottTributeDirectoryRows,
} from "../lib/research-engine-v2/check-hotel-freshness.js";
import { loadChoiceSitemapDirectoryRows } from "../lib/hotel-census/plan-choice-census-sitemap-match.js";
import {
  inferBrandExplorerPresenceFromReports,
  runCrossTableChecks,
} from "../lib/research-engine-v2/cross-table-checks.js";
import { resolveBrandFamily } from "../lib/research-engine-v2/brand-family.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_DIR = join(ROOT, "data/research-engine-v2/contradiction-first-v1");

const CALA_RE =
  /Mexico|Colombia|Panama|Peru|Barbados|Puerto Rico|Honduras|Cayman|Argentina|Brazil|Jamaica|Dominican|Costa Rica|Chile|Bahamas|Aruba|Guatemala|Ecuador|Uruguay|Nicaragua|El Salvador|Trinidad|Grenada|Dominica|Curaçao|Curacao|Bonaire|Belize|Suriname|Guyana|Paraguay|Bolivia|Venezuela|Saint Lucia|St\. Lucia|Antigua|St\. Kitts|Martinique|Guadeloupe/i;

const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 350);

function parseArgs(argv) {
  return {
    compareWebhound: argv.includes("--compare-webhound"),
    mexicoOnly: argv.includes("--mexico-only"),
    limit: Number((argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1] || 0) || 0,
  };
}

/**
 * Minimal CSV parser for census-amenities-blank-rows.csv
 * @param {string} text
 */
function parseCensusCsv(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) return [];
  const rows = [];
  for (const line of lines.slice(1)) {
    const fields = splitCsvLine(line);
    if (fields.length < 5) continue;
    rows.push({
      recordId: fields[0],
      name: fields[1],
      parentCompany: fields[2],
      status: fields[3],
      country: fields[4],
    });
  }
  return rows;
}

function splitCsvLine(line) {
  /** @type {string[]} */
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

/**
 * Map census row → benchmark brand label (or null if out of scope).
 * Includes related Marriott soft brands so reflags can be rediscovered without seeding answers.
 * @param {{ name: string, parentCompany: string }} row
 */
function classifyBenchmarkBrand(row) {
  const name = String(row.name || "");
  const parent = String(row.parentCompany || "");

  if (/NOI Indigo/i.test(name)) return null;
  if (/Hotel Indigo/i.test(name)) return "Hotel Indigo";
  if (/Kimpton/i.test(name)) return "Kimpton";
  if (/Tribute Portfolio/i.test(name)) return "Tribute Portfolio";
  if (/Autograph Collection/i.test(name)) return "Autograph Collection";
  if (/Design Hotels/i.test(name) && /Marriott/i.test(parent)) return "Design Hotels";
  if (/\bAvani\b|\bAVANI\b/i.test(name)) return "Avani";
  if (/Radisson Individuals/i.test(name)) return "Radisson Individuals Americas";
  return null;
}

function buildSnapshotHotels(args) {
  const csvPath = join(ROOT, "reports/census-amenities-blank-rows.csv");
  if (!existsSync(csvPath)) {
    throw new Error(`Missing census snapshot source: ${csvPath}`);
  }
  const all = parseCensusCsv(readFileSync(csvPath, "utf8"));
  /** @type {object[]} */
  const hotels = [];
  for (const row of all) {
    const brand = classifyBenchmarkBrand(row);
    if (!brand) continue;
    if (!CALA_RE.test(row.country || "")) continue;
    if (args.mexicoOnly && !/Mexico/i.test(row.country || "")) continue;

    // Core Test-6 brand set + Marriott related affiliations for reflag discovery
    const inCore =
      brand === "Hotel Indigo" ||
      brand === "Kimpton" ||
      brand === "Tribute Portfolio" ||
      brand === "Avani" ||
      brand === "Radisson Individuals Americas";
    const marriottRelated = brand === "Autograph Collection" || brand === "Design Hotels";
    if (!inCore && !marriottRelated) continue;

    hotels.push({
      hotelId: row.recordId,
      recordId: row.recordId,
      name: row.name,
      parentCompany: row.parentCompany === "(blank parent)" ? "" : row.parentCompany,
      currentParent: row.parentCompany === "(blank parent)" ? "" : row.parentCompany,
      status: row.status,
      currentStatus: row.status,
      country: row.country,
      city: "",
      currentBrand: brand === "Autograph Collection" || brand === "Design Hotels" ? brand : brand,
      affiliation: brand,
      brandFamily: resolveBrandFamily({
        affiliation: brand,
        parentCompany: row.parentCompany,
        name: row.name,
      }),
      website: "",
      managementCompany: "",
      snapshotSource: "reports/census-amenities-blank-rows.csv",
    });
  }

  if (args.limit > 0) return hotels.slice(0, args.limit);
  return hotels;
}

async function runBlindBenchmark(args) {
  mkdirSync(OUT_DIR, { recursive: true });
  const startedAt = new Date().toISOString();
  const t0 = Date.now();

  const hotels = buildSnapshotHotels(args);
  const inputSnapshot = {
    generatedAt: startedAt,
    blind: true,
    note: "Snapshot taken BEFORE any Webhound Test 6 comparison. Checker must not read Test 6 answers.",
    source: "reports/census-amenities-blank-rows.csv",
    filters: {
      geography: args.mexicoOnly ? "Mexico only" : "Mexico + CALA",
      brands: [
        "Hotel Indigo",
        "Kimpton",
        "Tribute Portfolio",
        "Avani",
        "Radisson Individuals Americas",
        "Autograph Collection (Marriott related)",
        "Design Hotels (Marriott related)",
      ],
    },
    hotelCount: hotels.length,
    hotels,
  };
  writeJson("01-input-snapshot.json", inputSnapshot);

  const ihgDirectoryRows = loadIhgDirectoryRows(join(ROOT, "reports/ihg-cala-directory-extract.json"));
  const marriottDirectoryRows = loadMarriottTributeDirectoryRows(
    join(ROOT, "reports/cala-tribute-property-visual-discovery.json")
  );
  let choiceDirectoryRows = [];
  try {
    choiceDirectoryRows = loadChoiceSitemapDirectoryRows(
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json"),
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv")
    );
  } catch (err) {
    console.warn("[re-v2] Choice directory load failed:", err?.message || err);
  }

  /** @type {object[]} */
  const results = [];
  let i = 0;
  for (const hotel of hotels) {
    i++;
    console.log(`[re-v2] [${i}/${hotels.length}] ${hotel.currentBrand} — ${hotel.name} (${hotel.country})`);
    try {
      const result = await checkHotelFreshness(hotel, {
        ihgDirectoryRows,
        marriottDirectoryRows,
        choiceDirectoryRows,
        fetchDelayMs: FETCH_DELAY_MS,
      });
      results.push(result);
    } catch (err) {
      console.error(`[re-v2] ERROR ${hotel.name}:`, err?.message || err);
      results.push({
        hotel,
        brandFamily: hotel.brandFamily,
        error: err?.message || String(err),
        claims: [],
        proposedCorrections: [],
        checkedAt: new Date().toISOString(),
      });
    }
  }

  const brandExplorerPresence = inferBrandExplorerPresenceFromReports([
    "Hotel Indigo",
    "Kimpton",
    "Tribute Portfolio",
    "Avani",
    "Radisson Individuals Americas",
  ]);
  const crossTable = runCrossTableChecks(results, { brandExplorerPresence });

  const mexicoFilter = /Mexico/i;
  const ihgGaps = findDirectoryGaps(hotels, ihgDirectoryRows, {
    brandFamily: "ihg",
    countryFilter: mexicoFilter,
  });
  const marriottGaps = findDirectoryGaps(hotels, marriottDirectoryRows, {
    brandFamily: "marriott",
    countryFilter: /Mexico|mexico/i,
  });

  const materialCorrections = results.flatMap((r) =>
    (r.proposedCorrections || []).filter((c) =>
      ["Proposed Status Change", "Proposed Reflag", "Proposed Parent Correction", "Proposed Update"].includes(
        c.recommended_action
      )
    )
  );

  const nativeResults = {
    generatedAt: new Date().toISOString(),
    startedAt,
    elapsedMs: Date.now() - t0,
    blind: true,
    webhoundComparison: null,
    summary: {
      hotelsChecked: results.length,
      materialProposedCorrections: materialCorrections.length,
      statusChanges: materialCorrections.filter((c) => c.recommended_action === "Proposed Status Change").length,
      reflags: materialCorrections.filter((c) => c.recommended_action === "Proposed Reflag").length,
      parentCorrections: materialCorrections.filter((c) => c.recommended_action === "Proposed Parent Correction")
        .length,
      crossTableFindings: crossTable.length,
      ihgMexicoDirectoryGaps: ihgGaps.length,
      marriottMexicoDirectoryGaps: marriottGaps.length,
      brandExplorerPresence,
    },
    materialCorrections,
    crossTableFindings: crossTable,
    directoryGaps: { ihgMexico: ihgGaps, marriottMexico: marriottGaps },
    results,
  };

  writeJson("02-native-results.json", nativeResults);
  writeMarkdownNativeSummary(nativeResults);

  console.log("\n[re-v2] Native results frozen.");
  console.log(`[re-v2] Hotels: ${results.length}`);
  console.log(`[re-v2] Material corrections: ${materialCorrections.length}`);
  console.log(`[re-v2] Elapsed: ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  console.log(`[re-v2] Output: ${OUT_DIR}`);

  if (args.compareWebhound) {
    await compareToWebhound(nativeResults);
  } else {
    console.log(
      "\n[re-v2] Blind phase complete. Re-run with --compare-webhound ONLY AFTER reviewing 02-native-results.json."
    );
  }

  return nativeResults;
}

/**
 * @param {object} nativeResults
 */
async function compareToWebhound(nativeResults) {
  const evalPath = join(ROOT, "data/webhound-test1-mexico-owner-intel/TEST6-EVALUATION.md");
  const reconPath = join(ROOT, "data/webhound-test1-mexico-owner-intel/TEST6-RECONCILIATION.md");
  const rowsPath = join(ROOT, "data/webhound-test1-mexico-owner-intel/test6-rows-compact.json");

  if (!existsSync(rowsPath) && !existsSync(evalPath)) {
    throw new Error("Test 6 artifacts not found for comparison");
  }

  const evalText = existsSync(evalPath) ? readFileSync(evalPath, "utf8") : "";
  const reconText = existsSync(reconPath) ? readFileSync(reconPath, "utf8") : "";
  const rows = existsSync(rowsPath) ? JSON.parse(readFileSync(rowsPath, "utf8")) : [];

  const materialWh = extractWebhoundMaterialFindings({ evalText, reconText, rows });
  const native = summarizeNativeFindings(nativeResults);

  /** @type {object[]} */
  const comparisons = [];
  let found = 0;
  let partial = 0;
  let missed = 0;
  let better = 0;
  let rejected = 0;

  for (const wh of materialWh) {
    const match = matchNativeToWebhound(wh, native);
    comparisons.push({ webhound: wh, nativeMatch: match });
    if (match.verdict === "Found It Independently") found++;
    else if (match.verdict === "Partially Found It") partial++;
    else if (match.verdict === "Found Better Evidence") better++;
    else if (match.verdict === "Correctly Rejected It") rejected++;
    else missed++;
  }

  const materialCount = materialWh.length || 1;
  const rediscoveryRate = ((found + better + 0.5 * partial) / materialCount) * 100;

  const nativeOnly = native.corrections.filter((c) => !materialWh.some((wh) => namesOverlap(wh.hotelName, c.hotel_name)));

  const reconciliation = {
    generatedAt: new Date().toISOString(),
    note: "Comparison performed AFTER native freeze",
    webhoundMaterialFindingCount: materialWh.length,
    rediscoveryRatePct: Number(rediscoveryRate.toFixed(1)),
    counts: {
      foundIndependently: found,
      partiallyFound: partial,
      foundBetterEvidence: better,
      correctlyRejected: rejected,
      missed: missed,
      nativeOnly: nativeOnly.length,
    },
    successThreshold: { targetPct: 70, stretchPct: 80, met: rediscoveryRate >= 70 },
    comparisons,
    nativeOnlyDiscrepancies: nativeOnly,
    webhoundFindingsUsed: materialWh,
  };

  writeJson("03-webhound-reconciliation.json", reconciliation);
  writeMissedAndFpAnalysis(reconciliation, nativeResults);
  writeFinalReport(nativeResults, reconciliation);

  console.log(`\n[re-v2] Rediscovery rate: ${reconciliation.rediscoveryRatePct}%`);
  console.log(`[re-v2] Threshold met (>=70%): ${reconciliation.successThreshold.met}`);
}

function extractWebhoundMaterialFindings({ evalText, reconText, rows }) {
  /** @type {object[]} */
  const findings = [];

  // Prefer structured compact rows when present
  const list = Array.isArray(rows) ? rows : rows?.rows || rows?.findings || [];
  for (const row of list) {
    const hotelName = row.hotelName || row.hotel || row.name || row.property || "";
    const field = row.field || row.claimType || row.topic || "";
    const classification = row.classification || row.severity || row.severityClass || "";
    const currentValue = row.currentValue || row.dealalityValue || row.current || null;
    const observedValue = row.observedValue || row.webhoundValue || row.proposed || null;
    const confidence = row.confidence || row.confidenceBand || "";
    const material =
      row.material === true ||
      /high|material|proposed/i.test(String(classification + " " + confidence + " " + (row.severityClass || "")));

    if (!hotelName && !observedValue) continue;
    // Keep rows that look like corrections / contradictions
    if (
      material ||
      /pipeline|open|reflag|parent|missing|operator|affiliation|status/i.test(
        `${field} ${classification} ${currentValue} ${observedValue}`
      )
    ) {
      findings.push({
        hotelName: String(hotelName),
        field: String(field),
        currentValue,
        observedValue,
        classification: String(classification || "material"),
        confidence: String(confidence),
        source: "test6-rows-compact.json",
        raw: row,
      });
    }
  }

  // Supplement from markdown bullet patterns if compact rows thin
  if (findings.length < 5) {
    const blob = `${evalText}\n${reconText}`;
    const bullets = blob.split(/\n/).filter((l) => /^\s*[-*]\s+/.test(l) || /^\s*\d+\.\s+/.test(l));
    for (const line of bullets) {
      if (!/pipeline|open|reflag|tribute|autograph|indigo|kimpton|avani|radisson|parent|missing|faranda|nizuc|aluna|crystal|turtle/i.test(line)) {
        continue;
      }
      findings.push({
        hotelName: guessHotelNameFromLine(line),
        field: guessFieldFromLine(line),
        currentValue: null,
        observedValue: line.replace(/^\s*[-*\d.]+\s*/, "").trim(),
        classification: "material_md",
        confidence: /high/i.test(line) ? "high" : "unknown",
        source: "test6-markdown",
        raw: { line },
      });
    }
  }

  // Dedup by hotel+field+observed
  const seen = new Set();
  return findings.filter((f) => {
    const key = `${f.hotelName}|${f.field}|${f.observedValue}`.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function guessHotelNameFromLine(line) {
  const m = String(line).match(
    /(Hotel Indigo [^–\-|,(]+|Kimpton [^–\-|,(]+|Casa Nizuc|Crystal Cove|Turtle Beach|Avani [^–\-|,(]+|Faranda [^–\-|,(]+|Tribute[^–\-|,(]*|Aluna[^–\-|,(]*|Tres R[ií]os[^–\-|,(]*)/i
  );
  return m ? m[1].trim() : "";
}

function guessFieldFromLine(line) {
  const s = String(line).toLowerCase();
  if (/reflag|autograph|tribute|affiliation|brand/.test(s)) return "Affiliation";
  if (/pipeline|open|status|operating/.test(s)) return "status";
  if (/parent|choice|ihg|marriott|minor/.test(s)) return "Parent Company";
  if (/operator|faranda|managed/.test(s)) return "operator";
  if (/missing|census|brand explorer/.test(s)) return "cross_table";
  return "unknown";
}

function summarizeNativeFindings(nativeResults) {
  return {
    corrections: nativeResults.materialCorrections || [],
    gaps: [
      ...(nativeResults.directoryGaps?.ihgMexico || []),
      ...(nativeResults.directoryGaps?.marriottMexico || []),
    ],
    crossTable: nativeResults.crossTableFindings || [],
    results: nativeResults.results || [],
  };
}

function matchNativeToWebhound(wh, native) {
  const name = wh.hotelName || "";
  const field = String(wh.field || "").toLowerCase();
  const observed = String(wh.observedValue || "").toLowerCase();

  const corrHits = native.corrections.filter(
    (c) => namesOverlap(name, c.hotel_name) || (name && observed.includes(String(c.hotel_name || "").toLowerCase().slice(0, 12)))
  );
  const gapHits = native.gaps.filter((g) => namesOverlap(name, g.directoryName) || namesOverlap(observed, g.directoryName));
  const crossHits = native.crossTable.filter(
    (x) => namesOverlap(name, x.hotelName) || namesOverlap(name, x.brand) || namesOverlap(observed, x.brand)
  );

  // Class-level matching when hotel name weak
  const classHit = classLevelNativeHit(wh, native);

  if (corrHits.length) {
    const c = corrHits[0];
    const sameField =
      !field ||
      field.includes(String(c.field || "").toLowerCase()) ||
      String(c.field || "").toLowerCase().includes(field) ||
      (/status|pipeline|open/.test(field) && /status/i.test(c.field || "")) ||
      (/brand|affiliation|reflag/.test(field) && /affiliation|brand/i.test(c.field || ""));
    return {
      verdict: sameField ? "Found It Independently" : "Partially Found It",
      evidence: c,
    };
  }
  if (gapHits.length && /missing|census|pipeline/.test(field + observed)) {
    return { verdict: "Found It Independently", evidence: gapHits[0] };
  }
  if (crossHits.length) {
    return { verdict: "Partially Found It", evidence: crossHits[0] };
  }
  if (classHit) return classHit;
  return { verdict: "Missed It", evidence: null };
}

function classLevelNativeHit(wh, native) {
  const text = `${wh.hotelName} ${wh.field} ${wh.observedValue} ${wh.classification}`.toLowerCase();
  const statusCorr = native.corrections.filter((c) => c.recommended_action === "Proposed Status Change");
  const reflagCorr = native.corrections.filter((c) => c.recommended_action === "Proposed Reflag");
  const parentCorr = native.corrections.filter((c) => c.recommended_action === "Proposed Parent Correction");
  const beMissing = native.crossTable.filter((x) => x.type === "brand_explorer_missing_vs_census_operating");

  if (/pipeline.*open|open.*pipeline|status/.test(text) && statusCorr.length) {
    // Prefer name overlap; else class-only = partial
    const named = statusCorr.find((c) => namesOverlap(wh.hotelName, c.hotel_name));
    return {
      verdict: named ? "Found It Independently" : "Partially Found It",
      evidence: named || statusCorr[0],
      note: named ? undefined : "Class-level status freshness rediscovered; hotel name link weak",
    };
  }
  if (/reflag|autograph|tribute|affiliation/.test(text) && reflagCorr.length) {
    const named = reflagCorr.find((c) => namesOverlap(wh.hotelName, c.hotel_name));
    return {
      verdict: named ? "Found It Independently" : "Partially Found It",
      evidence: named || reflagCorr[0],
    };
  }
  if (/parent|choice|regional/.test(text) && parentCorr.length) {
    return { verdict: "Partially Found It", evidence: parentCorr[0] };
  }
  if (/avani|brand explorer|missing brand/.test(text) && beMissing.length) {
    return { verdict: "Found It Independently", evidence: beMissing[0] };
  }
  if (/missing/.test(text) && native.gaps.length) {
    const named = native.gaps.find((g) => namesOverlap(wh.hotelName, g.directoryName));
    return {
      verdict: named ? "Found It Independently" : "Partially Found It",
      evidence: named || native.gaps[0],
    };
  }
  return null;
}

function namesOverlap(a, b) {
  const na = String(a || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length > 3 && !["hotel", "resort", "member", "portfolio", "collection"].includes(t));
  const nb = String(b || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length > 3 && !["hotel", "resort", "member", "portfolio", "collection"].includes(t));
  if (!na.length || !nb.length) return false;
  return na.some((t) => nb.includes(t));
}

function writeJson(name, obj) {
  writeFileSync(join(OUT_DIR, name), JSON.stringify(obj, null, 2), "utf8");
}

function writeMarkdownNativeSummary(nativeResults) {
  const lines = [
    "# Native Checker Results (BLIND — before Webhound comparison)",
    "",
    `Generated: ${nativeResults.generatedAt}`,
    `Elapsed: ${nativeResults.elapsedMs} ms`,
    `Hotels checked: ${nativeResults.summary.hotelsChecked}`,
    `Material proposed corrections: ${nativeResults.summary.materialProposedCorrections}`,
    "",
    "## Material corrections",
    "",
  ];
  for (const c of nativeResults.materialCorrections || []) {
    lines.push(
      `- **${c.hotel_name}** · ${c.field}: \`${c.current_value}\` → \`${c.observed_value}\` · ${c.recommended_action} · ${c.classification}`
    );
    lines.push(`  - Reason: ${c.reason}`);
  }
  if (!(nativeResults.materialCorrections || []).length) lines.push("_None_");
  lines.push("", "## Directory gaps (Mexico IHG / Marriott Tribute)", "");
  for (const g of nativeResults.directoryGaps?.ihgMexico || []) {
    lines.push(`- IHG gap: ${g.directoryName} (${g.country})`);
  }
  for (const g of nativeResults.directoryGaps?.marriottMexico || []) {
    lines.push(`- Marriott gap: ${g.directoryName} (${g.country})`);
  }
  lines.push("", "## Cross-table findings", "");
  for (const x of nativeResults.crossTableFindings || []) {
    lines.push(`- ${x.type}: ${x.hotelName || x.brand || ""}`);
  }
  writeFileSync(join(OUT_DIR, "02-native-results.md"), lines.join("\n"), "utf8");
}

function writeMissedAndFpAnalysis(reconciliation, nativeResults) {
  const missed = reconciliation.comparisons.filter((c) => c.nativeMatch.verdict === "Missed It");
  const partial = reconciliation.comparisons.filter((c) => c.nativeMatch.verdict === "Partially Found It");
  const lines = [
    "# Missed findings analysis",
    "",
    `Missed: ${missed.length}`,
    `Partial: ${partial.length}`,
    "",
    "## Misses",
    "",
  ];
  for (const m of missed) {
    lines.push(`- ${m.webhound.hotelName || "(unnamed)"} · ${m.webhound.field}: ${String(m.webhound.observedValue).slice(0, 200)}`);
  }
  lines.push("", "## Partials", "");
  for (const m of partial) {
    lines.push(`- ${m.webhound.hotelName || "(unnamed)"} · ${m.nativeMatch.note || "class-level"}`);
  }
  writeFileSync(join(OUT_DIR, "05-missed-findings-analysis.md"), lines.join("\n"), "utf8");

  // False positives: native material corrections with no WH overlap and weak confidence
  const fps = (nativeResults.materialCorrections || []).filter((c) => {
    const linked = reconciliation.comparisons.some(
      (cmp) =>
        cmp.nativeMatch.evidence &&
        namesOverlap(cmp.nativeMatch.evidence.hotel_name || cmp.nativeMatch.evidence.directoryName || "", c.hotel_name)
    );
    return !linked && (c.confidence == null || c.confidence < 0.5);
  });
  writeFileSync(
    join(OUT_DIR, "06-false-positive-analysis.md"),
    [
      "# False positive analysis",
      "",
      `Candidates (low-confidence native corrections without Webhound overlap): ${fps.length}`,
      "",
      ...fps.map(
        (c) =>
          `- ${c.hotel_name} · ${c.field}: ${c.current_value} → ${c.observed_value} (confidence=${c.confidence})`
      ),
      "",
      "Note: Absence from Webhound does not prove false positive; may be native-only improvement.",
    ].join("\n"),
    "utf8"
  );
}

function writeFinalReport(nativeResults, reconciliation) {
  const rate = reconciliation.rediscoveryRatePct;
  const readiness =
    rate >= 80 ? "Promising" : rate >= 70 ? "Promising" : "Experiment only";
  const lines = [
    "# Contradiction-First V1 — Final Report",
    "",
    "## Did Dealality learn enough from Webhound?",
    "",
    rate >= 70
      ? `**Yes, enough for this experiment.** Native rediscovery rate **${rate}%** of material Test 6 freshness/affiliation findings (threshold 70%).`
      : `**Not yet at threshold.** Native rediscovery rate **${rate}%** (need ≥70%). See misses analysis for missing research behaviors.`,
    "",
    `Production readiness: **${readiness}** (not production-hardened; no writers; adapters limited to IHG/Marriott/Choice + generic).`,
    "",
    "## Rediscovery",
    "",
    "```json",
    JSON.stringify(reconciliation.counts, null, 2),
    "```",
    "",
    `Rediscovery rate: ${rate}%`,
    "",
    "## Runtime",
    "",
    `- Elapsed: ${nativeResults.elapsedMs} ms`,
    `- External cost: $0 (no Webhound; public directory fetches only)`,
    "",
    "## Top 3 next improvements",
    "",
    "1. Stronger live status signals (IHG/Marriott availability APIs or puppeteer where HTML is blocked).",
    "2. Expand Marriott catalog beyond Tribute CALA visual discovery (Autograph/Design full CALA directory).",
    "3. Lightweight press/opening announcement fetch for pipeline hotels missing from live directories.",
    "",
  ];
  writeFileSync(join(OUT_DIR, "08-final-report.md"), lines.join("\n"), "utf8");
}

function writeArchitectureNote() {
  const text = `# Research Engine V2 — Contradiction-First Status/Affiliation Checker (V1)

## Goal

Prove Dealality can natively rediscover Webhound Test 6–class freshness/affiliation findings
using contradiction-first research — **without** Webhound credits or Airtable writes.

## Reused infrastructure

- Hilton census status audit pattern (\`lib/hotel-census/audit-hilton-census-status.js\`)
- IHG directory extract + hoteldetail name/brand parsers (\`lib/ihg-brand-directory-extract.js\`)
- Marriott URL/MARSHA helpers (\`lib/marriott-brand-directory-extract.js\`)
- Choice sitemap match loader (\`lib/hotel-census/plan-choice-census-sitemap-match.js\`)
- Census field constants (\`lib/hotel-census/fields.js\`)
- Local census snapshot: \`reports/census-amenities-blank-rows.csv\`
- Local directories: \`reports/ihg-cala-directory-extract.json\`, \`reports/cala-tribute-property-visual-discovery.json\`

## New modules

| Module | Role |
|--------|------|
| \`lib/research-engine-v2/claim-model.js\` | Claims + proposed corrections |
| \`lib/research-engine-v2/source-hierarchy.js\` | Claim-specific source priority + temporal resolve |
| \`lib/research-engine-v2/query-generator.js\` | Support + disproof queries (generic) |
| \`lib/research-engine-v2/brand-family.js\` | Adapter routing |
| \`lib/research-engine-v2/adapters/*\` | IHG / Marriott / Choice / generic |
| \`lib/research-engine-v2/check-hotel-freshness.js\` | \`checkHotelFreshness()\` orchestrator |
| \`lib/research-engine-v2/cross-table-checks.js\` | Light Census ↔ BE integrity |

## Blind benchmark process

1. Snapshot Dealality values → \`01-input-snapshot.json\`
2. Run native checker → freeze \`02-native-results.*\`
3. Only then \`--compare-webhound\` → \`03-webhound-reconciliation.json\`

## Non-goals (V1)

Scheduler, Airtable writer, full temporal DB, every hotel group, UI, owner/gov discovery, Webhound integration.
`;
  writeFileSync(join(OUT_DIR, "00-architecture.md"), text, "utf8");
}

function writeImplementationNotes() {
  const text = `# Implementation notes

- Experiment only; proposed corrections never applied.
- Status detection for IHG uses hoteldetail permanence + Book Now / Check Rates / New Hotel banner.
- Marriott often 403s overview HTML; adapter probes \`/photos/\` pages.
- Avani uses generic adapter (no dedicated Minor directory in V1).
- Operator claims intentionally Unverified — never inferred from brand alone.
- Probe scripts under \`scripts/_probe-ihg-*.mjs\` are disposable diagnostics.

## Proposed next step

Broaden Marriott + Choice directory coverage, add opening-announcement fetcher for pipeline-only hotels,
then re-run blind benchmark on a larger CALA slice before any apply-gate wiring.
`;
  writeFileSync(join(OUT_DIR, "07-implementation-notes.md"), text, "utf8");
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(OUT_DIR, { recursive: true });
writeArchitectureNote();
writeImplementationNotes();

if (args.compareWebhound && existsSync(join(OUT_DIR, "02-native-results.json"))) {
  const nativeResults = JSON.parse(readFileSync(join(OUT_DIR, "02-native-results.json"), "utf8"));
  await compareToWebhound(nativeResults);
} else {
  await runBlindBenchmark(args);
}
