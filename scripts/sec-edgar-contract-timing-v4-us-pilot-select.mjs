#!/usr/bin/env node
/**
 * Dry-run US pilot selection from V1–V3 contract-timing findings.
 *
 *   node scripts/sec-edgar-contract-timing-v4-us-pilot-select.mjs
 *   node scripts/sec-edgar-contract-timing-v4-us-pilot-select.mjs --hpc-check
 *
 * Never creates HPC hotels. Never pads CREATE_READY to hit 25–50.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  aggregateFindingsToHotels,
  selectUsPilotCandidates,
  classifyAfterCollision,
  PILOT_DISPOSITION,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [
  "C:/Dev/dealality/my-operators-backend/.env",
  path.join(ROOT, ".env"),
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const HPC_CHECK = process.argv.includes("--hpc-check");
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-contract-timing-v4");
fs.mkdirSync(OUT_DIR, { recursive: true });

function loadFindings() {
  const rows = [];
  const v3p = path.join(ROOT, "reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json");
  if (fs.existsSync(v3p)) {
    const j = JSON.parse(fs.readFileSync(v3p, "utf8"));
    for (const f of j.findings || []) rows.push({ ...f, wave: "V3" });
  }
  const v2p = path.join(ROOT, "reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json");
  if (fs.existsSync(v2p)) {
    const j = JSON.parse(fs.readFileSync(v2p, "utf8"));
    for (const f of j.combined || []) rows.push({ ...f, wave: "V2" });
  }
  const v1p = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (fs.existsSync(v1p)) {
    const j = JSON.parse(fs.readFileSync(v1p, "utf8"));
    for (const f of [...(j.verified || []), ...(j.probable || [])]) {
      rows.push({ ...f, wave: "V1" });
    }
  }
  return rows;
}

async function maybeCollisionCheck(queue) {
  if (!HPC_CHECK || !queue.length) return { checked: 0, results: [] };
  const { performLiveHpcIdentityCollisionCheck, HPC_LIVE_OUTCOME } = await import(
    "../lib/hotel-census/hpc-live-identity-collision-v1.js"
  );
  const results = [];
  for (const c of queue) {
    try {
      const collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: c.hotelName,
        aliases: c.aliasHints || [],
        city: c.city,
        country: "United States",
        brand: c.brandLabel || c.brandFamily || undefined,
      });
      results.push({
        propertyKey: c.propertyKey,
        outcome: collision?.outcome || HPC_LIVE_OUTCOME.HPC_LIVE_LOOKUP_UNAVAILABLE,
        hpcRecordId: collision?.hpcRecordId || null,
        candidates: (collision?.candidates || []).slice(0, 3).map((x) => ({
          id: x.id || x.recordId,
          name: x.name || x.canonicalName,
        })),
      });
    } catch (err) {
      results.push({
        propertyKey: c.propertyKey,
        outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE",
        error: String(err.message || err).slice(0, 200),
      });
    }
  }
  return { checked: results.length, results };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const findings = loadFindings();
  const hotels = aggregateFindingsToHotels(findings);
  const selection = selectUsPilotCandidates(hotels, {
    targetMin: 25,
    targetMax: 50,
    collisionCheckCap: 50,
  });

  let collisionMeta = { checked: 0, results: [] };
  if (HPC_CHECK) {
    collisionMeta = await maybeCollisionCheck(selection.collisionCheckQueue);
    const byKey = new Map(collisionMeta.results.map((r) => [r.propertyKey, r]));
    for (const row of selection.ranked) {
      const c = byKey.get(row.propertyKey);
      if (!c) continue;
      row.collision = c;
      row.disposition = classifyAfterCollision(row.preDisposition, c);
      await sleep(80);
    }
  } else {
    // Without HPC check, never claim CREATE_READY or LINK_EXISTING
    for (const row of selection.ranked) {
      if (row.preDisposition === "CREATE_READY_PENDING_COLLISION") {
        row.disposition = PILOT_DISPOSITION.RESEARCH_REQUIRED;
        row.dispositionNote = "Pending live HPC collision (--hpc-check)";
      }
    }
  }

  const summary = {
    LINK_EXISTING: selection.ranked.filter((r) => r.disposition === PILOT_DISPOSITION.LINK_EXISTING)
      .length,
    CREATE_READY: selection.ranked.filter((r) => r.disposition === PILOT_DISPOSITION.CREATE_READY)
      .length,
    RESEARCH_REQUIRED: selection.ranked.filter(
      (r) => r.disposition === PILOT_DISPOSITION.RESEARCH_REQUIRED,
    ).length,
    REJECT_AMBIGUOUS: selection.ranked.filter(
      (r) => r.disposition === PILOT_DISPOSITION.REJECT_AMBIGUOUS,
    ).length,
    CREATE_READY_PENDING_COLLISION: selection.ranked.filter(
      (r) => r.preDisposition === "CREATE_READY_PENDING_COLLISION",
    ).length,
  };

  // Pilot shortlist: top RESEARCH_REQUIRED / CREATE_READY / LINK_EXISTING by score, max 50, no padding
  const shortlist = selection.ranked
    .filter((r) => r.disposition !== PILOT_DISPOSITION.REJECT_AMBIGUOUS)
    .slice(0, 50);

  const report = {
    module: "V4_US_HPC_PILOT_SELECTION",
    status: "DRY_RUN_CRITERIA_ONLY",
    hpcCheck: HPC_CHECK,
    gate: {
      pr495MustMergeBeforeV4: true,
      noHpcCreates: true,
      noSchemaApply: true,
      doNotPadCreateReady: true,
    },
    input: {
      findingsLoaded: findings.length,
      usHotelsAggregated: hotels.length,
    },
    targetBand: { min: 25, max: 50 },
    summary,
    shortlistCount: shortlist.length,
    meetsTargetBandWithoutPadding: shortlist.length >= 25 && shortlist.length <= 50,
    note:
      shortlist.length < 25
        ? "Fewer than 25 non-rejected candidates — do not force CREATE_READY; expand research instead"
        : "Shortlist capped at 50; CREATE_READY only after collision + founder manifest approval",
    collisionMeta: { checked: collisionMeta.checked },
    shortlist: shortlist.map((r) => ({
      hotelName: r.hotelName,
      city: r.city,
      stateRegion: r.stateRegion,
      score: r.score,
      disposition: r.disposition,
      preDisposition: r.preDisposition,
      addressHint: r.addressHint,
      milestoneCount: r.milestoneCount,
      futureMilestoneCount: r.futureMilestoneCount,
      brandFamily: r.brandFamily,
      brandLabel: r.brandLabel,
      waves: r.waves,
      scoreReasons: r.scoreReasons,
      scorePenalties: r.scorePenalties,
      collision: r.collision || null,
      findingIds: r.findingIds.slice(0, 5),
    })),
    criteriaDoc: "docs/ai-build-system/HOTEL_CONTRACT_TIMING_V4_HPC_CREATION_WORKFLOW.md",
    selectionModule: "lib/hotel-intelligence/sec-edgar/contract-timing-v4-us-pilot-selection-v1.js",
  };

  const outJson = path.join(OUT_DIR, "US_PILOT_SELECTION_DRY_RUN.json");
  const outMd = path.join(OUT_DIR, "US_PILOT_SELECTION_CRITERIA.md");
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  fs.writeFileSync(
    outMd,
    `# V4 US HPC Pilot Selection — Criteria & Dry-Run

**Status:** Criteria locked · Dry-run only · No HPC creates · No schema apply  
**Gate:** PR #495 must merge before V4 implementation  

## Priorities (deterministic)

1. Verified physical-property identity (name + city + US state; brand/hotel lexeme)
2. Full address or distinctive geo evidence (from CMBS schedule snippet when present)
3. At least one verified hotel contract milestone (not loan maturity)
4. Future 2027–2040 milestones preferred; historical retained with status caution
5. Brand/alias evidence when available
6. Ownership/management counterparty mentions when available
7. Hotel Explorer enrichment potential (branded + address + milestone)

## Dispositions

| Class | Meaning |
|---|---|
| LINK_EXISTING | Live HPC collision verified match |
| CREATE_READY | Strong evidence + HPC verified absent — still needs founder manifest approval |
| RESEARCH_REQUIRED | Promising but incomplete geo/status/collision |
| REJECT_AMBIGUOUS | Non-hotel, generic, portfolio-wide, or identity collision |

**Do not force CREATE_READY to reach 25–50.**

## This dry-run

| Metric | Value |
|---|---:|
| Findings loaded | ${report.input.findingsLoaded} |
| US hotels aggregated | ${report.input.usHotelsAggregated} |
| LINK_EXISTING | ${summary.LINK_EXISTING} |
| CREATE_READY | ${summary.CREATE_READY} |
| RESEARCH_REQUIRED | ${summary.RESEARCH_REQUIRED} |
| REJECT_AMBIGUOUS | ${summary.REJECT_AMBIGUOUS} |
| Pending collision (pre) | ${summary.CREATE_READY_PENDING_COLLISION} |
| Shortlist (non-reject, ≤50) | ${report.shortlistCount} |
| HPC check run | ${HPC_CHECK} |

${report.note}

## Top 15 shortlist

${shortlist
  .slice(0, 15)
  .map(
    (r, i) =>
      `${i + 1}. **${r.hotelName}** (${r.city}, ${r.stateRegion}) · score ${r.score} · ${r.disposition}`,
  )
  .join("\n")}

## Next after #495 merge

1. Re-run with \`--hpc-check\`
2. Build founder creation manifest from CREATE_READY + LINK_EXISTING
3. Explicit approval per CREATE row before apply
`,
  );

  console.log(
    JSON.stringify(
      {
        outJson,
        outMd,
        summary,
        shortlistCount: shortlist.length,
        hpcCheck: HPC_CHECK,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
