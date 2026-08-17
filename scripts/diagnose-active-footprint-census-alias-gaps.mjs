#!/usr/bin/env node
/**
 * Diagnose census alias / inventory gaps for Active brands with hidden Footprint Metrics
 * (from reports/brand-explorer-active-footprint-display-gate.json).
 *
 * Read-only. Quiet sequential brand fetches + one Hotel Census inventory pass.
 *
 * Usage:
 *   node scripts/diagnose-active-footprint-census-alias-gaps.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { buildBrandCensusSummary } from "../lib/hotel-census/build-brand-census-summary.js";
import {
  getGovernanceFieldAvailability,
  resetGovernanceFieldCache,
} from "../lib/hotel-census/census-governance.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE, ALIAS_FIELDS, BRAND_ALIAS_TABLE } from "../lib/hotel-census/fields.js";
import {
  buildAffiliationInventory,
  buildAffiliationIndex,
  buildAliasKeyIndex,
  attachExistingAliasStatus,
  proposeAliasesForBrand,
} from "../lib/census-backed-alias-proposals.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const GATE_REPORT = path.join(ROOT, "reports", "brand-explorer-active-footprint-display-gate.json");
const OUT_JSON = path.join(ROOT, "reports", "brand-explorer-active-footprint-census-alias-gaps.json");
const OUT_MD = path.join(ROOT, "reports", "brand-explorer-active-footprint-census-alias-gaps.md");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function mockRes() {
  const out = { statusCode: 200, payload: null };
  return {
    setHeader() {},
    status(c) {
      out.statusCode = c;
      return this;
    },
    json(p) {
      out.payload = p;
      return this;
    },
    getOut() {
      return out;
    },
  };
}

async function fetchBrand(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById(
    { method: "GET", query: { brandId: String(brandId), refresh: "1" }, headers: {} },
    res
  );
  const out = res.getOut();
  if (out.statusCode !== 200 || !out.payload?.success) {
    throw new Error(out.payload?.error || `HTTP ${out.statusCode}`);
  }
  return out.payload.brand;
}

function classifyGap(summary) {
  const warnings = summary.warnings || [];
  const open = summary.metrics?.totalOpenHotels ?? 0;
  const pipe = summary.metrics?.totalPipelineHotels ?? 0;
  const matched = summary.census?.recordsMatched ?? 0;
  const usedAlias = summary.alias?.usedAliasTable === true;

  if (
    warnings.some((w) =>
      /NO_ACTIVE_ALIAS_ROWS|NO_ALIAS_FOR_REQUESTED_BRAND|ALIAS_TABLE_UNAVAILABLE|NO_ALIAS_ROWS_FOR_CANONICAL/.test(
        w
      )
    )
  ) {
    return "alias_gap";
  }
  if (!usedAlias) return "alias_not_used_direct_name_only";
  if (usedAlias && matched === 0 && open === 0 && pipe === 0) return "alias_present_zero_census_rows";
  if (usedAlias && matched > 0 && open === 0 && pipe === 0) {
    return "alias_matched_but_all_non_open_or_filtered";
  }
  if (open === 0 && pipe === 0) return "empty_inventory";
  return "other";
}

function slimProposal(p) {
  return {
    affiliation: p["Alias / Source Brand Name"],
    canonical: p["Canonical Brand Name"],
    parentCompany: p["Parent Company"],
    confidence: p["Match Confidence"],
    reason: p["Proposal Reason"],
    existingAliasStatus: p.existingAliasStatus,
    openHotels: p.censusEvidence?.openHotels ?? 0,
    openKeys: p.censusEvidence?.openKeys ?? 0,
    pipelineHotels: p.censusEvidence?.pipelineHotels ?? 0,
    requiresHumanReview: p["Requires Human Review"] === true,
  };
}

function recommendedNextStep(gapClass, proposals) {
  if (gapClass === "alias_gap" || gapClass === "alias_not_used_direct_name_only") {
    return proposals.length
      ? "Add/activate Brand Alias Mapping for top proposal Affiliation(s); dry-run apply first."
      : "No heuristic Affiliation hit — steward research census Affiliation strings, then add alias (do not invent options).";
  }
  if (gapClass === "alias_present_zero_census_rows") {
    return "Alias Active but matchers hit 0 census rows — expand Alias/Source Brand Name to real Hotel Census Affiliation values.";
  }
  if (gapClass === "alias_matched_but_all_non_open_or_filtered") {
    return "Matchers hit records but open+pipeline=0 — check Status / Include-in-Explorer / Parent filters.";
  }
  if (gapClass === "empty_inventory") {
    return "True empty inventory for matchers — keep census fallback; unlock via Verified/Estimated MVP footprint if figures are real.";
  }
  return "Review warnings + matchers manually.";
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer — Census Alias Gaps (Hidden Footprint 18)");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push("Read-only diagnosis for Active/Live brands with Footprint Metrics hidden.");
  lines.push("");
  lines.push("## Summary by gap class");
  lines.push("");
  lines.push("| Gap class | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(report.summaryByClass)) {
    lines.push(`| \`${k}\` | ${v} |`);
  }
  lines.push("");
  lines.push("## Per-brand diagnosis");
  lines.push("");
  for (const row of report.brands) {
    lines.push(`### ${row.name} (\`${row.slug}\`)`);
    lines.push("");
    lines.push(`- **Gap class:** \`${row.gapClass}\``);
    lines.push(`- **Parent:** ${row.parentCompany || "—"}`);
    lines.push(`- **Canonical (alias):** ${row.alias?.canonicalBrandName || "—"}`);
    lines.push(`- **Used alias table:** ${row.alias?.usedAliasTable ? "yes" : "no"}`);
    lines.push(
      `- **Matchers:** ${(row.alias?.affiliationMatchers || []).map((m) => `\`${m}\``).join(", ") || "—"}`
    );
    lines.push(
      `- **Census:** open=${row.metrics?.totalOpenHotels ?? "—"} · pipeline=${row.metrics?.totalPipelineHotels ?? "—"} · recordsMatched=${row.census?.recordsMatched ?? "—"}`
    );
    if (row.warnings?.length) {
      lines.push(`- **Warnings:**`);
      for (const w of row.warnings) lines.push(`  - ${w}`);
    }
    if (row.topProposals?.length) {
      lines.push(`- **Top affiliation proposals:**`);
      for (const p of row.topProposals.slice(0, 5)) {
        lines.push(
          `  - \`${p.affiliation}\` · open=${p.openHotels} · keys=${p.openKeys} · ${p.existingAliasStatus} · ${p.confidence} · ${p.reason}`
        );
      }
    } else {
      lines.push(`- **Proposals:** ${row.proposalNote || "none"}`);
    }
    lines.push(`- **Recommended next step:** ${row.recommendedNextStep}`);
    lines.push("");
  }
  return lines.join("\n");
}

async function main() {
  resetGovernanceFieldCache();
  if (!fs.existsSync(GATE_REPORT)) {
    throw new Error(`Missing ${GATE_REPORT} — run audit-brand-explorer-active-footprint-display-gate.mjs first`);
  }
  const gate = JSON.parse(fs.readFileSync(GATE_REPORT, "utf8"));
  const targets = gate.hiddenBrands || [];
  if (!targets.length) throw new Error("No hiddenBrands in gate report");

  const platformBase = getPlatformBase();
  if (!platformBase) throw new Error("AIRTABLE_BASE_ID_ALT not configured");

  console.log(`[alias-gaps] loading Brand Alias Mapping…`);
  const aliasRecords = await platformBase(BRAND_ALIAS_TABLE)
    .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
    .all();
  const aliasKeyIndex = buildAliasKeyIndex(aliasRecords);

  const governance = await getGovernanceFieldAvailability(platformBase);
  const selectFields = [
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.parentCompany,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.name,
  ];
  if (governance.includeInBrandExplorer) {
    selectFields.push(CENSUS_FIELDS.includeInBrandExplorer);
  }

  console.log(`[alias-gaps] loading Hotel Census inventory (read-only)…`);
  const censusRecords = await platformBase(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();
  const inventory = buildAffiliationInventory(censusRecords, governance);
  const affiliationIndex = buildAffiliationIndex(inventory);
  console.log(
    `[alias-gaps] targets=${targets.length}; censusRecords=${censusRecords.length}; affiliationGroups=${inventory.length}; aliasRecords=${aliasRecords.length}`
  );

  const brands = [];
  for (let i = 0; i < targets.length; i++) {
    const t = targets[i];
    process.stdout.write(`[${i + 1}/${targets.length}] ${t.name || t.slug}… `);
    try {
      const brand = await fetchBrand(t.recordId);
      const name = brand.name || t.name;
      const parent = brand.parentCompany || null;
      const summary = await buildBrandCensusSummary(name, parent);
      const gapClass = classifyGap(summary);

      let topProposals = [];
      let proposalNote = "";
      try {
        const raw = proposeAliasesForBrand(name, parent || "", inventory, affiliationIndex);
        topProposals = attachExistingAliasStatus(raw, aliasKeyIndex).map(slimProposal).slice(0, 8);
        if (!topProposals.length) {
          proposalNote = "No high-confidence Affiliation proposals from census inventory heuristics.";
        }
      } catch (e) {
        proposalNote = `Proposal helper error: ${e.message}`;
      }

      const row = {
        slug: brand.slug || t.slug,
        recordId: t.recordId,
        name,
        parentCompany: parent,
        gapClass,
        available: summary.available,
        fallbackRecommended: summary.fallbackRecommended,
        warnings: summary.warnings || [],
        alias: summary.alias || null,
        match: summary.match || null,
        metrics: summary.metrics || null,
        census: summary.census || null,
        topProposals,
        proposalNote,
        recommendedNextStep: recommendedNextStep(gapClass, topProposals),
      };
      brands.push(row);
      console.log(
        `${gapClass} open=${summary.metrics?.totalOpenHotels ?? "n/a"} matched=${summary.census?.recordsMatched ?? "n/a"} proposals=${topProposals.length}`
      );
    } catch (err) {
      console.log(`ERROR ${err.message}`);
      brands.push({
        slug: t.slug,
        recordId: t.recordId,
        name: t.name,
        gapClass: "fetch_error",
        error: err.message,
        recommendedNextStep: "Re-run after fixing fetch/429.",
        topProposals: [],
      });
      if (/429|rate/i.test(err.message)) await sleep(8000);
    }
    if (i < targets.length - 1) await sleep(700);
  }

  const summaryByClass = {};
  for (const b of brands) {
    summaryByClass[b.gapClass] = (summaryByClass[b.gapClass] || 0) + 1;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    targetCount: targets.length,
    censusRecords: censusRecords.length,
    affiliationGroups: inventory.length,
    summaryByClass,
    brands,
  };
  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(OUT_MD, toMarkdown(report));
  console.log(`[alias-gaps] wrote ${OUT_JSON}`);
  console.log(`[alias-gaps] wrote ${OUT_MD}`);
  console.log(`[alias-gaps] summary`, summaryByClass);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
