#!/usr/bin/env node
/**
 * Steward-triage remaining blank Website+Property ID Hyatt CALA census rows.
 * Read-only vs Airtable; uses local directory extract + apply log + unmatched CSV.
 *
 *   node scripts/triage-hyatt-census-unmatched.mjs
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import {
  loadHyattDirectoryRows,
  normalizeHyattHotelNameForMatch,
  hyattBrandsAlign,
  hyattMatchIsHardExcluded,
  scoreHyattDirectoryAgainstCensus,
} from "../lib/hotel-census/plan-hyatt-census-enrichment.js";
import { nameFromHyattSlug } from "../lib/hyatt-brand-directory-extract.js";
import { nameSimilarity, normalizeKey } from "../lib/independent-census/match-current-census.js";

const UNMATCHED = "reports/hyatt-census-unmatched.csv";
const APPLY_LOG = "reports/hyatt-census-enrichment-apply-log.csv";
const DIRECTORY = "reports/hyatt-cala-directory-extract.json";
const OUT_CSV = "reports/hyatt-census-unmatched-steward-triage.csv";
const OUT_JSON = "reports/hyatt-census-unmatched-steward-triage.json";

function parseCsvLine(line) {
  const cols = [];
  let cur = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      q = !q;
      continue;
    }
    if (c === "," && !q) {
      cols.push(cur);
      cur = "";
      continue;
    }
    cur += c;
  }
  cols.push(cur);
  return cols;
}

function parseCsv(path) {
  const text = readFileSync(path, "utf8").replace(/^\uFEFF/, "").trim();
  const lines = text.split(/\r?\n/);
  const header = parseCsvLine(lines[0]);
  return lines.slice(1).filter(Boolean).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    header.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function brandCluster(name) {
  const n = String(name || "");
  const rules = [
    [/park hyatt/i, "Park Hyatt"],
    [/grand hyatt/i, "Grand Hyatt"],
    [/hyatt place/i, "Hyatt Place"],
    [/hyatt regency/i, "Hyatt Regency"],
    [/hyatt centric/i, "Hyatt Centric"],
    [/hyatt vivid/i, "Hyatt Vivid"],
    [/\bandaz\b/i, "Andaz"],
    [/\bthompson\b|\bthe cape\b/i, "Thompson"],
    [/\bsecrets\b/i, "Secrets"],
    [/\bdreams?\b/i, "Dreams"],
    [/\bbreathless\b/i, "Breathless"],
    [/\bsunscape\b/i, "Sunscape"],
    [/\bzoetry\b/i, "Zoetry"],
    [/\bimpression\b/i, "Impression"],
    [/\bnow\b/i, "NOW"],
    [/destination by hyatt/i, "Destination"],
    [/unbound|cas en bas|cas en bas|krystal|live aqua|devossion|marien|royal beach|placencia/i, "nonstandard_or_unbound"],
  ];
  for (const [re, label] of rules) {
    if (re.test(n)) return label;
  }
  return "other";
}

function isPipelineOrUnopened(name, city) {
  const n = `${name} ${city}`.toLowerCase();
  // Grand Island phase labels, residences-only, clear openings naming
  if (/\b(i|ii|iii)\s+grand island\b/i.test(name)) return true;
  if (/\bgrand island\b/i.test(name) && /\bdreams\b|\bnow\b/i.test(name)) return true;
  if (/\bresidences\b/i.test(name) && !/\bhotel\b/i.test(name)) return true;
  if (/pipeline|coming soon|under construction|proposed/i.test(n)) return true;
  return false;
}

function isNonstandard(name, cluster) {
  if (cluster === "nonstandard_or_unbound") return true;
  const n = String(name || "").toLowerCase();
  if (/cas en bas|dream hotel group|krystal|live aqua|devossion|marien|royal beach hotel|the placencia resort(?!.*hyatt)/i.test(n)) {
    return true;
  }
  // Hotel La Compañia is Unbound Collection
  if (/la compa[nñ]ia|compan[ií]a/i.test(n)) return true;
  return false;
}

function findBestDirectoryCandidates(census, dirRows, usedPids) {
  const censusName = normalizeHyattHotelNameForMatch(census.censusName);
  /** @type {object[]} */
  const scored = [];
  for (const d of dirRows) {
    const dirName = normalizeHyattHotelNameForMatch(d.name || nameFromHyattSlug(d.slug));
    const nameSim = Math.max(
      nameSimilarity(dirName, censusName),
      nameSimilarity(normalizeHyattHotelNameForMatch(nameFromHyattSlug(d.slug)), censusName)
    );
    if (nameSim < 0.35) continue;
    const brandOk = hyattBrandsAlign(censusName, d.propertyUrl || "", dirName);
    const hard = hyattMatchIsHardExcluded(
      censusName,
      `${dirName} ${d.slug || ""} ${d.propertyUrl || ""}`
    );
    const pid = String(d.propertyId || "").toUpperCase();
    scored.push({
      propertyId: pid,
      name: dirName,
      url: d.propertyUrl,
      nameSim: Math.round(nameSim * 1000) / 1000,
      brandOk,
      hardExcluded: hard,
      alreadyApplied: usedPids.has(pid),
      country: d.censusCountry || d.country || "",
    });
  }
  scored.sort((a, b) => b.nameSim - a.nameSim);
  return scored.slice(0, 3);
}

function distinctiveTokens(name) {
  const stop = new Set([
    "hyatt",
    "place",
    "house",
    "regency",
    "centric",
    "grand",
    "park",
    "resort",
    "spa",
    "hotel",
    "and",
    "the",
    "at",
    "by",
    "a",
    "airport",
    "secrets",
    "dreams",
    "breathless",
    "sunscape",
    "zoetry",
    "thompson",
    "andaz",
    "impression",
    "collection",
    "residences",
  ]);
  return normalizeKey(name)
    .split(/\s+/)
    .filter((t) => t.length > 3 && !stop.has(t));
}

function hasDistinctiveOverlap(censusName, dirName) {
  const a = new Set(distinctiveTokens(censusName));
  const b = distinctiveTokens(dirName);
  if (!a.size || !b.length) return false;
  return b.some((t) => a.has(t));
}

function classifyRow(row, candidates, appliedByName) {
  const name = row.censusName;
  const cluster = brandCluster(name);
  const top = candidates[0];
  const hardHit = candidates.find((c) => c.hardExcluded);
  // Duplicate only when an already-applied PID is a strong/near-exact name match
  // (shared distinctive tokens), not a weak same-brand near-miss (Flora≠Jade, Aruba≠Cancun).
  const dupHit = candidates.find(
    (c) =>
      c.brandOk &&
      !c.hardExcluded &&
      c.alreadyApplied &&
      (c.nameSim >= 0.8 || (c.nameSim >= 0.55 && hasDistinctiveOverlap(name, c.name)))
  );
  const freeHit = candidates.find(
    (c) =>
      c.brandOk &&
      !c.hardExcluded &&
      !c.alreadyApplied &&
      (c.nameSim >= 0.7 || (c.nameSim >= 0.55 && hasDistinctiveOverlap(name, c.name)))
  );

  // Hard exclusions
  if (/\bcariari\b/i.test(name)) {
    return {
      reasonBucket: "hard_exclusion_name_conflict",
      suggestedNextAction:
        "Keep blocked vs Pinares (SJOZP). Wait for official Hyatt Place Cariari URL/Property ID on hyatt.com before any fill.",
      notes: hardHit
        ? `Would collide with ${hardHit.propertyId} ${hardHit.name}`
        : "No Cariari directory row; Pinares must not be used.",
    };
  }
  if (/\binsurgentes\b/i.test(name)) {
    if (dupHit && dupHit.propertyId === "MEXRM") {
      return {
        reasonBucket: "census_duplicate_after_1to1",
        suggestedNextAction:
          "Duplicate Insurgentes census row — MEXRM already applied to another census record. Dedupe/merge steward decision; do not re-bind generic MEXHR.",
        notes: `MEXRM already applied; keep Insurgentes≠generic MEXHR block.`,
      };
    }
    if (!top || (top.propertyId !== "MEXRM" && !/\binsurgentes\b/i.test(top.name))) {
      return {
        reasonBucket: "hard_exclusion_name_conflict",
        suggestedNextAction:
          "Do not bind to generic Hyatt Regency Mexico City (MEXHR). Only MEXRM Insurgentes URL is valid.",
        notes: top ? `Top candidate ${top.propertyId} nameSim=${top.nameSim}` : "No Insurgentes directory candidate",
      };
    }
  }

  // Prefer a free official candidate over a weaker already-applied near-miss.
  if (freeHit && freeHit.nameSim >= 0.55 && (!dupHit || freeHit.nameSim >= dupHit.nameSim)) {
    if (freeHit.nameSim >= 0.7) {
      return {
        reasonBucket: "recovery_candidate_review",
        suggestedNextAction: `SAFE candidate may exist: ${freeHit.propertyId} — re-run plan dry-run; apply only if score/confidence gates pass.`,
        notes: `${freeHit.propertyId} ${freeHit.name} url=${freeHit.url} sim=${freeHit.nameSim}`,
      };
    }
  }

  if (isPipelineOrUnopened(name, row.censusCity)) {
    return {
      reasonBucket: "pipeline_or_unopened",
      suggestedNextAction:
        "Treat as pipeline / multi-phase / residences — re-check hyatt.com after opening; do not invent Property ID.",
      notes: freeHit
        ? `Nearby free dir hit ${freeHit.propertyId} (sim ${freeHit.nameSim}) — verify opening status before any bind`
        : dupHit
          ? `Related PID ${dupHit.propertyId} already taken`
          : "No safe directory row",
    };
  }

  if (isNonstandard(name, cluster)) {
    return {
      reasonBucket: "nonstandard_or_unbound_name",
      suggestedNextAction:
        "Confirm Hyatt Parent/Affiliation vs Unbound/third-party naming; only fill if official hyatt.com hotel URL exists with matching Property ID.",
      notes: freeHit
        ? `Possible free candidate ${freeHit.propertyId} ${freeHit.name} (sim ${freeHit.nameSim}) — steward confirm brand`
        : "No official hyatt.com directory hit at safe similarity",
    };
  }

  if (dupHit && (!freeHit || dupHit.nameSim > freeHit.nameSim)) {
    return {
      reasonBucket: "census_duplicate_after_1to1",
      suggestedNextAction: `Dedupe against census row that already holds ${dupHit.propertyId}; do not double-assign Property ID.`,
      notes: `${dupHit.propertyId} ${dupHit.name} (sim ${dupHit.nameSim}) already applied`,
    };
  }

  if (freeHit && freeHit.nameSim >= 0.55) {
    return {
      reasonBucket: "recovery_candidate_review",
      suggestedNextAction: `Candidate ${freeHit.propertyId} — confirm name/city; apply only via plan gates (no invented codes).`,
      notes: `${freeHit.propertyId} ${freeHit.name} url=${freeHit.url} sim=${freeHit.nameSim}`,
    };
  }

  // Inclusive vs classic missing
  const inclusive = /secrets|dreams|breathless|sunscape|zoetry|impression|\bnow\b|vivid|ziva|zilara|thompson|andaz/i.test(
    name
  );
  if (inclusive) {
    return {
      reasonBucket: "inclusive_missing_from_wayback",
      suggestedNextAction:
        "Absent from Wayback Inclusive CDX/sitemap harvest — retry CDX later or steward-paste from live hyatt.com when Kasada allows; never invent codes.",
      notes: top
        ? `Best weak hit ${top.propertyId} sim=${top.nameSim} brandOk=${top.brandOk} applied=${top.alreadyApplied}`
        : "No directory near-match",
    };
  }

  return {
    reasonBucket: "classic_hyatt_missing_from_archive",
    suggestedNextAction:
      "Classic/lifestyle property missing from Wayback CDX — monitor hyatt.com openings; optional targeted CDX for region slug when property launches.",
    notes: top
      ? `Best weak hit ${top.propertyId} sim=${top.nameSim} brandOk=${top.brandOk} applied=${top.alreadyApplied}`
      : "No directory near-match",
  };
}

function main() {
  const unmatched = parseCsv(UNMATCHED);
  const applyLog = existsSync(APPLY_LOG) ? parseCsv(APPLY_LOG) : [];
  const usedPids = new Set(
    applyLog.map((r) => String(r.propertyId || "").toUpperCase()).filter(Boolean)
  );
  const appliedByName = new Map();
  for (const r of applyLog) {
    const k = normalizeKey(r.censusName || "");
    if (!k) continue;
    if (!appliedByName.has(k)) appliedByName.set(k, []);
    appliedByName.get(k).push(r);
  }

  const dirRows = loadHyattDirectoryRows(DIRECTORY);
  /** @type {object[]} */
  const triage = [];

  for (const row of unmatched) {
    const candidates = findBestDirectoryCandidates(row, dirRows, usedPids);
    const { reasonBucket, suggestedNextAction, notes } = classifyRow(
      row,
      candidates,
      appliedByName
    );
    const cluster = brandCluster(row.censusName);
    triage.push({
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
      censusCity: row.censusCity,
      censusCountry: row.censusCountry,
      brandCluster: cluster,
      reasonBucket,
      suggestedNextAction,
      notes,
      topCandidatePropertyId: candidates[0]?.propertyId || "",
      topCandidateNameSim: candidates[0]?.nameSim ?? "",
      topCandidateAlreadyApplied: candidates[0]?.alreadyApplied ?? "",
    });
  }

  const counts = {};
  for (const t of triage) {
    counts[t.reasonBucket] = (counts[t.reasonBucket] || 0) + 1;
  }

  const csvHeader = [
    "censusRecordId",
    "censusName",
    "censusCity",
    "censusCountry",
    "brandCluster",
    "reasonBucket",
    "suggestedNextAction",
    "notes",
    "topCandidatePropertyId",
    "topCandidateNameSim",
    "topCandidateAlreadyApplied",
  ];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const csv = [
    csvHeader.join(","),
    ...triage.map((t) => csvHeader.map((h) => esc(t[h])).join(",")),
  ].join("\n");

  writeFileSync(OUT_CSV, csv + "\n", "utf8");
  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        unmatchedCount: triage.length,
        directoryUnique: dirRows.length,
        appliedPropertyIdsInLog: usedPids.size,
        countsByBucket: counts,
        recoveryCandidates: triage.filter((t) => t.reasonBucket === "recovery_candidate_review"),
        hardExclusions: triage.filter((t) => t.reasonBucket === "hard_exclusion_name_conflict"),
        rows: triage,
        methodology: {
          sources: [
            UNMATCHED,
            DIRECTORY,
            APPLY_LOG,
            "lib/hotel-census/plan-hyatt-census-enrichment.js hard exclusions + brand align",
          ],
          buckets: [
            "inclusive_missing_from_wayback",
            "classic_hyatt_missing_from_archive",
            "census_duplicate_after_1to1",
            "hard_exclusion_name_conflict",
            "nonstandard_or_unbound_name",
            "pipeline_or_unopened",
            "recovery_candidate_review",
          ],
          hardRules:
            "Cariari≠Pinares; Insurgentes≠generic MEXHR; fill-blank only; no invented URLs/Property IDs",
        },
      },
      null,
      2
    ),
    "utf8"
  );

  console.log(JSON.stringify({ unmatchedCount: triage.length, countsByBucket: counts, outCsv: OUT_CSV, outJson: OUT_JSON }, null, 2));
}

main();
