#!/usr/bin/env node
/**
 * Build hotel segment research plan + evidence-backed coverage.
 *
 *   node scripts/gdi-hotel-segment-research-plan-v1.mjs --hotel rec0eTEnCqlsGJJt9
 *   node scripts/gdi-hotel-segment-research-plan-v1.mjs --hotels=mayflower,san_diego
 *   node scripts/gdi-hotel-segment-research-plan-v1.mjs --hotels=mayflower,san_diego --from-airtable
 *
 * Does not initiate paid research. Scores coverage from existing opportunities.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildHotelSegmentResearchPlan,
  loadHotelConfig,
  segmentResearchPlanToCsv,
  segmentResearchPlanToMarkdown,
  RESEARCH_SEGMENT,
} from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_ROOT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/segment-research-plans"
);

const HOTEL_ALIASES = {
  san_diego: "recnPB3Q02NO8Ma1o",
  sd: "recnPB3Q02NO8Ma1o",
  mayflower: "rec0eTEnCqlsGJJt9",
  may: "rec0eTEnCqlsGJJt9",
  bethesda: "recLuxvwwxID7U2B8",
  beth: "recLuxvwwxID7U2B8",
};

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

function parseHotels() {
  const multi = process.argv.find((a) => a.startsWith("--hotels="));
  if (multi) {
    return multi
      .slice("--hotels=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => HOTEL_ALIASES[s.toLowerCase()] || s);
  }
  const one = arg("--hotel");
  if (one) return [HOTEL_ALIASES[one.toLowerCase()] || one];
  return ["rec0eTEnCqlsGJJt9", "recnPB3Q02NO8Ma1o"];
}

function peArtifactForHotel(hotelId) {
  const pkgPath = path.join(
    ROOT,
    "reports/gdi-operational-baseline/pe-canonical-persist/venue-partnership-packages.json"
  );
  if (!fs.existsSync(pkgPath)) {
    return { peCanonicalPersisted: false, peVenueCount: 0 };
  }
  const doc = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  const pkgs = (doc.packages || []).filter(
    (p) => (p.hotel?.hotelId || p.hotel?.id) === hotelId && p.publish !== false
  );
  const ledgerPaths = [
    path.join(
      ROOT,
      "reports/gdi-operational-baseline/pe-canonical-persist/coordinator-sd/stage-completion-ledger.json"
    ),
    path.join(
      ROOT,
      "reports/gdi-operational-baseline/pe-canonical-persist/coordinator-may/stage-completion-ledger.json"
    ),
  ];
  let peCanonicalPersisted = pkgs.length > 0;
  for (const lp of ledgerPaths) {
    if (!fs.existsSync(lp)) continue;
    const led = JSON.parse(fs.readFileSync(lp, "utf8"));
    if (
      led.hotelId === hotelId &&
      led.stages?.apply_pe_canonical?.status === "completed" &&
      led.stages?.apply_pe_canonical?.dryRun === false
    ) {
      peCanonicalPersisted = true;
    }
  }
  return { peCanonicalPersisted, peVenueCount: pkgs.length };
}

async function loadOpps(hotelId, fromAirtable) {
  if (fromAirtable) {
    invalidateGdiHotelReadCache(hotelId);
    return listOpportunitiesForHotel(hotelId);
  }
  try {
    const doc = await loadOpportunitiesCanonical(hotelId);
    return Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  } catch {
    invalidateGdiHotelReadCache(hotelId);
    return listOpportunitiesForHotel(hotelId);
  }
}

async function main() {
  const hotels = parseHotels();
  const fromAirtable = has("--from-airtable") || !has("--canonical-only");
  fs.mkdirSync(OUT_ROOT, { recursive: true });

  const rollup = {
    version: "gdi-hotel-segment-research-plan-rollup-v1",
    generatedAt: new Date().toISOString(),
    fromAirtable,
    paidBudgetApproved: false,
    note: "Coverage scored from existing opportunities + PE persist artifacts. No paid calls initiated.",
    hotels: [],
  };

  for (const hotelId of hotels) {
    const cfg = loadHotelConfig(hotelId);
    const opportunities = await loadOpps(hotelId, fromAirtable);
    const pe = peArtifactForHotel(hotelId);
    const plan = buildHotelSegmentResearchPlan({
      hotelId,
      hotelConfig: cfg,
      opportunities,
      artifacts: {
        ...pe,
        paidBudgetApproved: false,
      },
    });

    const outDir = path.join(OUT_ROOT, hotelId);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "segment-research-plan.json"),
      JSON.stringify(plan, null, 2) + "\n"
    );
    fs.writeFileSync(
      path.join(outDir, "segment-research-plan.csv"),
      segmentResearchPlanToCsv(plan)
    );
    fs.writeFileSync(
      path.join(outDir, "segment-research-plan.md"),
      segmentResearchPlanToMarkdown(plan)
    );

    rollup.hotels.push({
      hotelId,
      displayName: plan.hotelDisplayName,
      summary: plan.summary,
      segments: plan.segments.map((s) => ({
        segmentId: s.segmentId,
        label: s.label,
        applicability: s.applicability,
        researchCoverage: s.researchCoverage,
        credible: s.counts.credible,
        partial: s.counts.partial,
      })),
      outDir,
    });

    console.log(
      JSON.stringify(
        {
          hotelId,
          summary: plan.summary,
          segments: plan.segments.map((s) => ({
            id: s.segmentId,
            applicability: s.applicability,
            coverage: s.researchCoverage,
            credible: s.counts.credible,
          })),
        },
        null,
        2
      )
    );
  }

  // Cross-hotel comparison table
  const md = [
    "# Multi-hotel segment research coverage",
    "",
    `- Generated: ${rollup.generatedAt}`,
    `- Paid budget approved: **false** (no additional paid calls)`,
    `- Source: ${fromAirtable ? "Airtable opportunity store + PE artifacts" : "canonical store"}`,
    "",
    "## Coverage matrix",
    "",
  ];

  const segIds = Object.values(RESEARCH_SEGMENT);
  const header = ["Segment", ...rollup.hotels.map((h) => h.displayName || h.hotelId)];
  md.push(`| ${header.join(" | ")} |`);
  md.push(`| ${header.map(() => "---").join(" | ")} |`);
  for (const sid of segIds) {
    const row = [
      rollup.hotels[0]?.segments.find((s) => s.segmentId === sid)?.label || sid,
    ];
    for (const h of rollup.hotels) {
      const s = h.segments.find((x) => x.segmentId === sid);
      if (!s) {
        row.push("—");
        continue;
      }
      if (s.applicability === "not_applicable") {
        row.push("not applicable");
      } else {
        row.push(`${s.researchCoverage} (${s.credible} credible)`);
      }
    }
    md.push(`| ${row.join(" | ")} |`);
  }
  md.push(
    "",
    "## Interpretation",
    "",
    "- **researched** = credible hotel-specific prospects (or PE canonical persist), not merely listed in config.",
    "- **partially_researched** = weak/calendar rows without full who→fit→contact chain.",
    "- **not_yet_researched** = applicable but no credible evidence yet.",
    "- Association volume alone is never full hotel coverage.",
    ""
  );

  fs.writeFileSync(
    path.join(OUT_ROOT, "ROLLUP.json"),
    JSON.stringify(rollup, null, 2) + "\n"
  );
  fs.writeFileSync(path.join(OUT_ROOT, "ROLLUP.md"), md.join("\n") + "\n");
  console.log(
    JSON.stringify(
      { ok: true, outRoot: OUT_ROOT, hotelCount: hotels.length },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
