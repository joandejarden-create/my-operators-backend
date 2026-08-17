#!/usr/bin/env node
/**
 * Resolve DR coverage steward gaps against existing Hotel Property Census
 * (name / URL / identity-key fuzzy match). Read-only by default.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const COUNTRY = "Dominican Republic";

const STEWARD_GAPS = [
  {
    property_name: "The Ocean Club, a Luxury Collection Resort, Costa Norte",
    brand: "The Luxury Collection",
    official_property_url:
      "https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview",
    marsha_hint: "poplc",
    tokens: ["ocean club", "costa norte", "poplc"],
  },
  {
    property_name:
      "Donoma Las Terrenas Resort & Villas, Autograph Collection",
    brand: "Autograph Collection",
    official_property_url:
      "https://www.marriott.com/en-us/hotels/azsak-donoma-las-terrenas-resort-and-villas-autograph-collection/overview",
    marsha_hint: "azsak",
    // also older catalog code popak — do NOT use weak brand-only tokens like "autograph"
    tokens: ["donoma", "azsak", "popak"],
  },
];

/** Tokens that must never alone create a census match. */
const WEAK_TOKENS = new Set([
  "autograph",
  "collection",
  "luxury",
  "marriott",
  "resort",
  "villas",
]);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function urlKey(u) {
  try {
    const x = new URL(String(u || "").trim());
    return `${x.hostname.replace(/^www\./, "")}${x.pathname}`
      .toLowerCase()
      .replace(/\/+$/, "");
  } catch {
    return norm(u);
  }
}

async function listDrCensus(baseId, token) {
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "City",
    "Official Property URL",
    "Property Identity Key",
    "VIC Freeze Hash",
  ];
  const formula = `AND({Country}='${COUNTRY}')`;
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of fields) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${CENSUS_TABLE_ID}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function matchGap(gap, records) {
  const gapUrl = urlKey(gap.official_property_url);
  const gapName = norm(gap.property_name);
  const hits = [];

  for (const r of records) {
    const f = r.fields || {};
    const name = norm(f["Property Name"]);
    const canon = norm(f["Canonical Property Name"]);
    const url = urlKey(f["Official Property URL"]);
    const key = norm(f["Property Identity Key"]);
    const reasons = [];

    if (gapUrl && url && (url === gapUrl || url.includes(gap.marsha_hint) || gapUrl.includes(url.split("/hotels/")[1] || "___"))) {
      if (url === gapUrl || (gap.marsha_hint && url.includes(gap.marsha_hint))) {
        reasons.push("official_url_marsha_or_exact");
      }
    }
    // Marriott hotel code in URL path
    for (const t of gap.tokens) {
      if (WEAK_TOKENS.has(t)) continue;
      if (t.length >= 4 && url.includes(t)) reasons.push(`url_token:${t}`);
      if (t.length >= 4 && (name.includes(t) || canon.includes(t) || key.includes(t))) {
        reasons.push(`name_token:${t}`);
      }
    }
    if (gapName && (name === gapName || canon === gapName)) {
      reasons.push("exact_normalized_name");
    }
    // strong partial: donoma / ocean club + brand family signal
    if (name.includes("donoma") && gapName.includes("donoma")) {
      reasons.push("strong_name:donoma");
    }
    if (name.includes("ocean club") && gapName.includes("ocean club")) {
      reasons.push("strong_name:ocean_club");
    }

    const uniq = [...new Set(reasons)];
    if (!uniq.length) continue;
    const score =
      (uniq.includes("official_url_marsha_or_exact") ? 100 : 0) +
      (uniq.includes("exact_normalized_name") ? 80 : 0) +
      (uniq.some((x) => x.startsWith("strong_name")) ? 60 : 0) +
      uniq.filter((x) => x.startsWith("url_token")).length * 25 +
      uniq.filter((x) => x.startsWith("name_token")).length * 10;
    hits.push({
      score,
      reasons: uniq,
      id: r.id,
      property_name: f["Property Name"],
      current_brand: f["Current Brand"],
      city: f["City"],
      official_property_url: f["Official Property URL"] || null,
      property_identity_key: f["Property Identity Key"] || null,
      osm_lineage: String(f["VIC Freeze Hash"] || "").includes(
        "independent_census_dr_osm"
      ),
    });
  }

  hits.sort((a, b) => b.score - a.score);
  const best = hits[0] || null;
  let decision = "true_missing_candidate";
  if (best && best.score >= 60) decision = "already_in_census";
  else if (best && best.score >= 30) decision = "probable_census_match_steward";
  return { gap, decision, best, alt_hits: hits.slice(0, 5) };
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const records = await listDrCensus(baseId, token);
  const results = STEWARD_GAPS.map((g) => matchGap(g, records));

  const report = {
    status: "dry_run",
    airtable_writes: false,
    country: COUNTRY,
    generated_at: new Date().toISOString(),
    rule: "Never propose insert until census name/URL/identity checked",
    dr_census_count: records.length,
    results,
    summary: {
      already_in_census: results.filter((r) => r.decision === "already_in_census")
        .length,
      probable_match: results.filter(
        (r) => r.decision === "probable_census_match_steward"
      ).length,
      true_missing_candidate: results.filter(
        (r) => r.decision === "true_missing_candidate"
      ).length,
    },
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const out = "reports/census-dr-marriott-steward-gap-census-check.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        output: out,
        summary: report.summary,
        results: results.map((r) => ({
          gap: r.gap.property_name,
          decision: r.decision,
          best: r.best
            ? {
                score: r.best.score,
                n: r.best.property_name,
                city: r.best.city,
                url: r.best.official_property_url,
                reasons: r.best.reasons,
              }
            : null,
        })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
