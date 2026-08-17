#!/usr/bin/env node
/**
 * DR resort official inventory vs Hotel Property Census (read-only).
 *
 * HARD RULE: never mark missing / propose insert until census is checked
 * with distinctive name/URL tokens (brand-only matches are insufficient).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DR_OFFICIAL_RESORT_INVENTORIES,
  WEAK_MATCH_TOKENS,
} from "../lib/independent-census/dr-official-resort-inventory-control.js";
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

function distinctiveHits(token, hay) {
  const t = norm(token);
  if (!t || t.length < 4) return false;
  if (WEAK_MATCH_TOKENS.has(t)) return false;
  return hay.includes(t);
}

/**
 * Match one official inventory row against full DR census.
 * Brand-only overlap is never enough for already_in_census.
 */
function matchAgainstCensus(row, records) {
  const gapUrl = urlKey(row.url);
  const gapName = norm(row.name);
  const hits = [];

  for (const r of records) {
    const f = r.fields || {};
    const name = norm(f["Property Name"]);
    const canon = norm(f["Canonical Property Name"]);
    const url = urlKey(f["Official Property URL"]);
    const key = norm(f["Property Identity Key"]);
    const hay = `${name} ${canon} ${url} ${key}`;
    const reasons = [];
    let score = 0;

    if (gapUrl && url && gapUrl === url) {
      reasons.push("exact_official_url");
      score += 100;
    }

    // Marriott / brand path code (e.g. azsak-, poplc-, hotel-riu-naiboa)
    for (const tok of row.distinctive_tokens || []) {
      const t = norm(tok);
      if (!t || WEAK_MATCH_TOKENS.has(t)) continue;
      if (url.includes(t.replace(/\s+/g, "-")) || url.includes(t.replace(/\s+/g, ""))) {
        reasons.push(`url_distinctive:${t}`);
        score += 50;
      }
      if (distinctiveHits(tok, hay)) {
        reasons.push(`name_distinctive:${t}`);
        score += 40;
      }
    }

    for (const tok of row.alias_tokens || []) {
      const t = norm(tok);
      if (!t || t.length < 5) continue;
      if (hay.includes(t)) {
        reasons.push(`alias:${t}`);
        score += 55;
      }
    }

    if (gapName && (name === gapName || canon === gapName)) {
      reasons.push("exact_normalized_name");
      score += 90;
    }

    // Require at least one distinctive reason — drop brand-only noise
    const distinctive = reasons.filter(
      (x) =>
        x.startsWith("name_distinctive:") ||
        x.startsWith("url_distinctive:") ||
        x.startsWith("alias:") ||
        x === "exact_official_url" ||
        x === "exact_normalized_name"
    );
    if (!distinctive.length) continue;

    hits.push({
      score,
      reasons: [...new Set(reasons)],
      id: r.id,
      property_name: f["Property Name"],
      current_brand: f["Current Brand"],
      city: f["City"],
      official_property_url: f["Official Property URL"] || null,
      property_identity_key: f["Property Identity Key"] || null,
    });
  }

  hits.sort((a, b) => b.score - a.score);
  const best = hits[0] || null;
  let decision = "true_missing_after_census_check";
  if (best && best.score >= 40) decision = "already_in_census";
  else if (best && best.score >= 20) decision = "probable_match_steward_review";

  return {
    official: row,
    decision,
    best,
    alt_hits: hits.slice(0, 3),
  };
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  console.log(`[dr-resort-gap] listing DR census for dedupe…`);
  const records = await listDrCensus(baseId, token);

  const byGroup = {};
  const all = [];
  for (const [group, rows] of Object.entries(DR_OFFICIAL_RESORT_INVENTORIES)) {
    byGroup[group] = [];
    for (const row of rows) {
      const m = matchAgainstCensus(row, records);
      byGroup[group].push(m);
      all.push({ group, ...m });
    }
  }

  const summary = {
    already_in_census: all.filter((x) => x.decision === "already_in_census").length,
    probable_match_steward_review: all.filter(
      (x) => x.decision === "probable_match_steward_review"
    ).length,
    true_missing_after_census_check: all.filter(
      (x) => x.decision === "true_missing_after_census_check"
    ).length,
  };

  const trueMissing = all
    .filter((x) => x.decision === "true_missing_after_census_check")
    .map((x) => ({
      group: x.group,
      name: x.official.name,
      brand: x.official.brand,
      city: x.official.city || null,
      url: x.official.url || null,
      source: x.official.source,
      insert_blocked_until:
        "steward confirm + city/identity key; census re-check immediately before any write",
    }));

  const report = {
    status: "dry_run",
    airtable_writes: false,
    country: COUNTRY,
    generated_at: new Date().toISOString(),
    hard_rule:
      "Always check Hotel Property Census before treating a hotel as missing; brand-only matches insufficient",
    dr_census_count: records.length,
    official_inventory_count: all.length,
    summary,
    true_missing_after_census_check: trueMissing,
    by_group: Object.fromEntries(
      Object.entries(byGroup).map(([g, rows]) => [
        g,
        {
          official_count: rows.length,
          already_in_census: rows.filter((r) => r.decision === "already_in_census")
            .length,
          true_missing: rows.filter(
            (r) => r.decision === "true_missing_after_census_check"
          ).length,
          rows: rows.map((r) => ({
            official_name: r.official.name,
            decision: r.decision,
            matched_census_name: r.best?.property_name || null,
            matched_census_id: r.best?.id || null,
            score: r.best?.score || 0,
            reasons: r.best?.reasons || [],
          })),
        },
      ])
    ),
    next_actions: [
      summary.true_missing_after_census_check
        ? `Steward-review ${summary.true_missing_after_census_check} true-missing candidates (re-check census immediately before any insert)`
        : "No true-missing candidates in curated RIU/Iberostar/Marriott-steward lists",
      "Do not insert from Google Travel; expand curated official lists only from brand destination pages",
    ],
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const jsonPath = "reports/census-dr-resort-official-gap-census-deduped.json";
  const mdPath = "reports/census-dr-resort-official-gap-census-deduped.md";
  writeFileSync(join(root, jsonPath), JSON.stringify(report, null, 2));

  const md = [
    `# DR Resort Official Gap Audit (Census-Deduped)`,
    ``,
    `**Airtable writes:** no`,
    `**Hard rule:** always check census before missing; brand-only match ≠ missing`,
    `**DR census rows checked:** ${records.length}`,
    `**Official curated rows:** ${all.length}`,
    ``,
    `## Summary`,
    ``,
    `- Already in census: **${summary.already_in_census}**`,
    `- Probable steward review: **${summary.probable_match_steward_review}**`,
    `- True missing after census check: **${summary.true_missing_after_census_check}**`,
    ``,
    `## True missing (only after census check)`,
    ``,
    ...(trueMissing.length
      ? trueMissing.map(
          (m) =>
            `- **${m.name}** (${m.group} / ${m.brand}, ${m.city || "?"}) — ${m.url || "no URL"}`
        )
      : ["- (none)"]),
    ``,
    `## By group`,
    ``,
    ...Object.entries(report.by_group).flatMap(([g, block]) => [
      `### ${g}`,
      ``,
      `- Official: ${block.official_count} · In census: ${block.already_in_census} · True missing: ${block.true_missing}`,
      ``,
      ...block.rows.map(
        (r) =>
          `- ${r.decision === "already_in_census" ? "✅" : r.decision === "true_missing_after_census_check" ? "❌" : "⚠️"} **${r.official_name}** → ${r.matched_census_name || "(no census match)"} (${r.decision})`
      ),
      ``,
    ]),
    `## Next`,
    ``,
    ...report.next_actions.map((a) => `- ${a}`),
    ``,
  ].join("\n");
  writeFileSync(join(root, mdPath), md);

  console.log(
    JSON.stringify(
      {
        ok: true,
        output_json: jsonPath,
        output_md: mdPath,
        dr_census_count: records.length,
        summary,
        true_missing: trueMissing.map((m) => m.name),
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
