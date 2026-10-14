#!/usr/bin/env node
/**
 * Audit + repair reviewed-prospect drawer completeness for Bethesda + Mayflower.
 *
 * 1) Parse sales-review overlay (hotel-specific thesis / actions)
 * 2) Re-import packages with overlay (filesystem or Airtable dry-run/apply)
 * 3) Score hotel-facing cards against drawer-content-contract-v2
 * 4) Write reports/gdi-drawer-completeness/*
 *
 * Default: local filesystem apply under an isolated data-root, plus Airtable
 * production dry-run (no production writes unless --apply-airtable).
 *
 * Usage:
 *   node scripts/gdi-drawer-completeness-repair-v1.mjs
 *   node scripts/gdi-drawer-completeness-repair-v1.mjs --apply-local
 *   node scripts/gdi-drawer-completeness-repair-v1.mjs --dry-run-production
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  importReviewedProspectsForHotel,
  loadReviewedProspectPackage,
} from "../lib/group-demand-intelligence/reviewed-prospect-import-v1.js";
import {
  reportDrawerCompleteness,
  evaluateDrawerContentCompleteness,
  DRAWER_CONTENT_CONTRACT_VERSION,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";
import { REVIEWED_PROSPECT_HOTEL_IDS } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import {
  clearGdiRepositoryReadCache,
  getGdiDataRoot,
} from "../lib/group-demand-intelligence/repository.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-drawer-completeness");
const PACKAGE_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-process-replication/hotel-review"
);
const OVERLAY_PATH = path.join(OUT, "sales-review-overlay.json");
const LOCAL_ROOT = path.join(OUT, "_local-repaired-store");

const HOTELS = {
  recLuxvwwxID7U2B8: { key: "bethesda", label: "Bethesda Marriott", facingExpected: 13 },
  rec0eTEnCqlsGJJt9: { key: "mayflower", label: "Mayflower", facingExpected: 11 },
};

function has(flag) {
  return process.argv.includes(flag);
}

function ensureOverlay() {
  const parse = spawnSync(
    process.execPath,
    [path.join(ROOT, "scripts/gdi-parse-sales-review-overlay.mjs")],
    { cwd: ROOT, encoding: "utf8" }
  );
  if (parse.status !== 0) {
    throw new Error(`overlay_parse_failed: ${parse.stderr || parse.stdout}`);
  }
  // Re-apply NADO strengtheners (same as repair pass)
  const doc = JSON.parse(fs.readFileSync(OVERLAY_PATH, "utf8"));
  const nadoMay = {
    opportunityType: "OVERFLOW_HOUSING",
    eventCity: "Arlington",
    venueCity: "Arlington",
    destinationStatus: "Arlington, Virginia",
    whyFits:
      "WashCon 2028 is announced for Arlington (hotel not yet named). Mayflower (downtown DC Autograph on Conn Ave) is a stretch versus the Crystal City/Arlington host pattern used in 2026–2027 — only pursue if planners want downtown DC lodging options for attendees who prefer not to stay in Arlington. Do not pitch as a drop-in replacement for Crystal Gateway without evidence the 2028 hotel selection is open to downtown properties.",
    nextAction:
      "Ask Jamie whether 2028 WashCon hotel selection is still open, whether downtown DC properties (including Mayflower) are under consideration versus an Arlington/Crystal City default, and who owns lodging contracting if Brittany Salazar is the meetings lead.",
    specificQuestion:
      "Is 2028 WashCon hotel selection open to downtown DC Autograph properties, or is the plan still an Arlington/Crystal City primary?",
    contactRelevance:
      "Jamie McCormick is listed on NADO staff with Events and Member Services seniority and a published direct line — a credible meetings-adjacent entry point. Title and staff listing do not prove hotel-block booking authority; Brittany Salazar (Associate Director, Meetings and Events) remains a stronger lodging-owner follow-up if confirmed.",
  };
  const nadoBeth = {
    opportunityType: "OVERFLOW_HOUSING",
    eventCity: "Arlington",
    venueCity: "Arlington",
    destinationStatus: "Arlington, Virginia",
    whyFits:
      "If planners reopen hotel selection for 2028 WashCon (Arlington dates published; hotel not named), Bethesda Marriott can compete as Metro-accessible full-service inventory versus a Crystal City default — not because Bethesda is closer than Arlington hotels.",
    nextAction:
      "Ask Jamie/NADO whether 2028 WashCon hotel selection is open and whether Bethesda can bid or be listed as overflow for attendees preferring the MoCo/metro corridor.",
    specificQuestion:
      "Is 2028 WashCon hotel selection open, and would Bethesda Marriott be considered for primary or Metro overflow versus Crystal City?",
    contactRelevance: nadoMay.contactRelevance,
  };
  for (const c of doc.cards || []) {
    if (c.cardId === "may-nado-2028") Object.assign(c, nadoMay);
    if (c.cardId === "beth-nado-2028") Object.assign(c, nadoBeth);
    if (
      c.cardId === "may-nado-2027" ||
      c.cardId === "beth-nado-2027-overflow"
    ) {
      c.opportunityType = "OVERFLOW_HOUSING";
      c.eventCity = "Arlington";
      c.destinationStatus = "Arlington, Virginia";
    }
  }
  fs.writeFileSync(OVERLAY_PATH, JSON.stringify(doc, null, 2) + "\n");
  return doc;
}

function seedLocalStoreFromCanonicalFs() {
  fs.mkdirSync(LOCAL_ROOT, { recursive: true });
  for (const hotelId of REVIEWED_PROSPECT_HOTEL_IDS) {
    const src = path.join(
      ROOT,
      "data/group-demand-intelligence/hotels",
      hotelId
    );
    const dst = path.join(LOCAL_ROOT, "hotels", hotelId);
    fs.mkdirSync(dst, { recursive: true });
    for (const f of ["opportunities.json", "profile.json"]) {
      const from = path.join(src, f);
      if (fs.existsSync(from)) {
        fs.copyFileSync(from, path.join(dst, f));
      }
    }
  }
}

async function runLocalRepair() {
  ensureOverlay();
  seedLocalStoreFromCanonicalFs();
  process.env.GDI_DATA_ROOT = LOCAL_ROOT;
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
  process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";
  clearGdiRepositoryReadCache();
  for (const id of REVIEWED_PROSPECT_HOTEL_IDS) {
    invalidateGdiHotelReadCache(id);
  }

  const applyLocal = has("--apply-local") || !has("--audit-only");
  const results = [];
  for (const hotelId of REVIEWED_PROSPECT_HOTEL_IDS) {
    const r = await importReviewedProspectsForHotel({
      hotelId,
      packageDir: PACKAGE_DIR,
      dryRun: !applyLocal,
      backupDir: path.join(OUT, "backups"),
      contentOverlayPath: OVERLAY_PATH,
      allowOrgHeuristic: hotelId === "recLuxvwwxID7U2B8",
    });
    results.push(r);
  }

  // Score persisted facing cards
  const perHotel = {};
  const allFacing = [];
  for (const hotelId of REVIEWED_PROSPECT_HOTEL_IDS) {
    const doc = await loadOpportunitiesCanonical(hotelId);
    const ops = (doc.opportunities || []).filter(
      (o) =>
        o.prospectHotelFacing === true ||
        o.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"
    );
    const facing = ops.filter((o) => o.prospectHotelFacing !== false);
    const report = reportDrawerCompleteness(facing, {
      hotelId,
      facingOnly: true,
    });
    perHotel[hotelId] = {
      ...HOTELS[hotelId],
      import: results.find((r) => r.hotelId === hotelId)?.counts || null,
      drawer: report,
    };
    for (const row of report.results) {
      allFacing.push({ hotelId, ...row });
    }
  }

  return { results, perHotel, allFacing, dataRoot: getGdiDataRoot() };
}

async function runProductionDryRun() {
  delete process.env.GDI_DATA_ROOT;
  process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
  process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";
  clearGdiRepositoryReadCache();
  for (const id of REVIEWED_PROSPECT_HOTEL_IDS) {
    invalidateGdiHotelReadCache(id);
  }
  const results = [];
  for (const hotelId of REVIEWED_PROSPECT_HOTEL_IDS) {
    results.push(
      await importReviewedProspectsForHotel({
        hotelId,
        packageDir: PACKAGE_DIR,
        dryRun: true,
        contentOverlayPath: OVERLAY_PATH,
        allowOrgHeuristic: hotelId === "recLuxvwwxID7U2B8",
      })
    );
  }
  return results;
}

function writeFieldLossDiagnosis() {
  const md = `# Field-loss diagnosis — Mayflower NADO/DDAA 2028

## Trace path
original research → review register → contact package → importer → Airtable/FS payload → list/detail DTO → drawer

## Classification (NADO 2028)

| Field | Status | Notes |
|-------|--------|-------|
| Account / occasion identity | Researched + persisted | Register account + timing |
| Opportunity type | Researched but lost during import | Never mapped; now set from overlay / section default |
| Timing (Mar 19–22 2028) | Researched; partially persisted | Dates parsed; "Verified" label had staff-only sources on drawer |
| Location (Arlington) | Researched but lost during import | Lived only inside timing string; now \`eventCity\` / \`destinationStatus\` |
| Commercial sourcing status | Persisted | UNKNOWN / hotel sourcing unconfirmed — correct |
| Hotel-specific thesis | Researched but lost during import | Sales-review \`whyFits\` discarded; importer wrote \`commercialStatus\` as thesis |
| Fit score 55 | Invented at import | Default with no canonical scoring basis — now cleared |
| Event sources (nado.org conference / 2027washcon) | Researched but lost during import | Register sources unused; only staff URL from contact package survived |
| Contact route (Jamie) | Persisted | Email + direct phone Group 1 |
| Contact relevance / no booking authority claim | Researched; thin on Mayflower package | Staff page + annual report evidence; overlay restores explicit non-authority language |
| Competitive / venue history / room demand | Never researched (or N/A) | Explicit not-established notes required |
| Concrete next action | Researched but lost / genericized | Overlay \`nextAction\` + specific question |

## Root cause
Importer treated \`commercialStatus\` browse labels as hotelOpportunityThesis / fitExplanation, defaulted \`hotelFitScore\` to 55, and merged only contact-package URLs into \`sources\` — dropping register event evidence.

## Contract rule
Staff pages support contact facts only. Event dates/location require occasion sources. TBA / hotel-not-named remain sourcing unconfirmed.
`;
  fs.writeFileSync(path.join(OUT, "00-field-loss-diagnosis.md"), md);
}

function writeContractDoc() {
  const md = `# Shared drawer-content contract

**Version:** \`${DRAWER_CONTENT_CONTRACT_VERSION}\`
**Module:** \`lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js\`

## Separately inspectable layers
1. **Commercial / publication eligibility** — hotelFacing vs enrichment queue
2. **Contact completeness** — groups 1–4
3. **Drawer content** — sales-card substance

Unknown room volume or unconfirmed sourcing alone must not disqualify a credible prospect.

## Required when claiming completeness
- Account + occasion/cohort identity
- Opportunity type (or explicit not-established)
- Timing + location with evidence status
- Commercial sourcing status (separate from completeness)
- Specific lodging/meeting thesis (hotel-specific; not browse labels)
- Hotel-specific fit rationale including material geographic/capacity caveats
- Why consider now
- Usable contact route + responsibility class + relevance (for named contacts)
- Sources supporting material claims (event sources ≠ staff-only)
- Concrete next sales action (not generic “contact the published route”)

## Optional / explicit empty
Room demand, venue history, reactivation, competitive context — populate when supported; otherwise “Not established / Not researched / Not applicable”.

## Prohibitions
- No fabricated estimates, venues, competitors, authority, or scores
- Modeled room ranges must state assumptions
- Numeric fit scores only with canonical scoring + explainable basis
- Generic repeated thesis text fails the contract
`;
  fs.writeFileSync(path.join(OUT, "01-shared-content-contract.md"), md);
}

function writePerCardResults(allFacing, perHotel) {
  const pass = allFacing.filter((r) => r.evaluation.ok);
  const fail = allFacing.filter((r) => !r.evaluation.ok);
  const md = [
    `# Per-card drawer completeness`,
    ``,
    `Contract: \`${DRAWER_CONTENT_CONTRACT_VERSION}\``,
    ``,
    `## Summary`,
    `- Facing cards scored: **${allFacing.length}**`,
    `- Meet full drawer contract: **${pass.length}**`,
    `- Incomplete: **${fail.length}**`,
    ``,
    `### By hotel`,
    ...Object.entries(perHotel).map(
      ([id, h]) =>
        `- ${h.label} (\`${id}\`): ${h.drawer.pass}/${h.drawer.total} pass (expected facing ~${h.facingExpected})`
    ),
    ``,
    `## Incomplete cards`,
    ...fail.map(
      (r) =>
        `- \`${r.id}\` — ${r.organizationName || "?"} / ${r.title || "?"} — failed: ${(r.evaluation.failed || []).join(", ") || "(none)"}; warnings: ${(r.evaluation.warnings || []).join(", ") || "—"}`
    ),
    ``,
    `## Passing cards`,
    ...pass.map(
      (r) =>
        `- \`${r.id}\` — ${r.organizationName || "?"} (${r.prospectReviewSection || "?"})`
    ),
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "02-per-card-completeness.md"), md);
  fs.writeFileSync(
    path.join(OUT, "02-per-card-completeness.json"),
    JSON.stringify({ asOf: new Date().toISOString(), perHotel, allFacing }, null, 2) +
      "\n"
  );
}

function writeProductionManifest(prodDry) {
  const rows = (prodDry || []).flatMap((r) =>
    (r.manifest || []).map((m) => ({
      hotelId: r.hotelId,
      ...m,
      drawerOk: (r.drawerCompleteness?.results || []).find(
        (d) => d.cardId === m.cardId
      )?.drawerOk,
      drawerFailed: (
        (r.drawerCompleteness?.results || []).find((d) => d.cardId === m.cardId)
          ?.failed || []
      ).join("|"),
    }))
  );
  const md = `# Production update manifest (dry-run only)

**Do not apply in this task.** Local repair completed under \`reports/gdi-drawer-completeness/_local-repaired-store\`.

## Intended production action
Re-import reviewed packages with sales-review content overlay for:
- Bethesda \`recLuxvwwxID7U2B8\` (13 hotel-facing)
- Mayflower \`rec0eTEnCqlsGJJt9\` (11 hotel-facing)

Command (when authorized later):
\`\`\`bash
node scripts/gdi-import-reviewed-prospects.mjs --both --adapter airtable --apply \\
  --content-overlay reports/gdi-drawer-completeness/sales-review-overlay.json \\
  --package-dir reports/gdi-bethesda-process-replication/hotel-review \\
  --out-dir reports/gdi-drawer-completeness/production-apply
\`\`\`

## Rollback
1. Restore from importer backups under \`reports/gdi-drawer-completeness/backups/\` (or Airtable snapshot taken before apply).
2. Re-import prior package hashes without overlay if needed.
3. Stable opportunity IDs, \`validationFeedback\`, actions, and outcomes are protected user fields and must not be wiped.

## Dry-run summary
${(prodDry || [])
  .map(
    (r) =>
      `- ${r.hotelId}: update=${r.counts?.update || 0} create=${r.counts?.create || 0} facing=${r.counts?.hotelFacing || 0} drawerPass=${r.counts?.drawerCompletenessPass ?? "?"} drawerFail=${r.counts?.drawerCompletenessFail ?? "?"}`
  )
  .join("\n")}

## Field-level update intent
Prefer richer thesis / actions / event sources / location; clear unexplained fit scores; never replace richer existing content with commercialStatus generics.
`;
  fs.writeFileSync(path.join(OUT, "04-production-update-manifest.md"), md);
  fs.writeFileSync(
    path.join(OUT, "04-production-dryrun.json"),
    JSON.stringify({ asOf: new Date().toISOString(), results: prodDry, rows }, null, 2) +
      "\n"
  );
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  writeFieldLossDiagnosis();
  writeContractDoc();

  const local = await runLocalRepair();
  writePerCardResults(local.allFacing, local.perHotel);

  let prodDry = [];
  if (has("--dry-run-production") || !has("--skip-production-dryrun")) {
    try {
      prodDry = await runProductionDryRun();
      writeProductionManifest(prodDry);
    } catch (err) {
      fs.writeFileSync(
        path.join(OUT, "04-production-dryrun-error.md"),
        `# Production dry-run error\n\n${err.stack || err.message}\n`
      );
    }
  }

  const pass = local.allFacing.filter((r) => r.evaluation.ok).length;
  const fail = local.allFacing.length - pass;
  const final = `# Final report (A–E)

## A. What was missing research versus lost mapping?
Primarily **lost at import mapping**: hotel-specific thesis, register event sources, structured location, opportunity type, and concrete next actions were researched in sales-review / register but overwritten by \`commercialStatus\` labels and staff-only contact URLs. Fit score **55** was **invented**, not researched. Room demand / venue history / competitive context were **never researched** for most cards (explicit not-established notes now required). Contact routes were generally **persisted**.

## B. How many cards now meet the full drawer contract?
**${pass} / ${local.allFacing.length}** hotel-facing reviewed cards pass \`${DRAWER_CONTENT_CONTRACT_VERSION}\` after local repair.

## C. Which remain incomplete and why?
See \`02-per-card-completeness.md\`. Cards that fail retain precise \`failed\` reasons (e.g. missing event evidence, thin thesis, contact relevance). Unknown sourcing / unknown room volume alone do **not** fail the contract.

## D. Is this enforced for the next hotel automatically?
**Yes, for process gating:** \`scripts/gdi-research-to-page-workflow-v1.mjs\` scores drawer completeness at \`generate_cards\` / publication prep, and the canonical importer loads the sales-review overlay + reports drawer layers separately from eligibility and contact completeness. Imports prefer richer existing content over generic defaults.

## E. What exact production update remains?
Airtable re-import with overlay — **dry-run prepared** in \`04-production-update-manifest.md\` / \`04-production-dryrun.json\`. **No production apply in this task.**

---
Local data root: \`${local.dataRoot}\`
`;
  fs.writeFileSync(path.join(OUT, "05-final-report-A-E.md"), final);
  fs.writeFileSync(
    path.join(OUT, "repair-run-summary.json"),
    JSON.stringify(
      {
        asOf: new Date().toISOString(),
        local,
        productionDryRunCounts: (prodDry || []).map((r) => ({
          hotelId: r.hotelId,
          counts: r.counts,
          drawerCompleteness: r.drawerCompleteness,
        })),
      },
      null,
      2
    ) + "\n"
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        pass,
        fail,
        total: local.allFacing.length,
        out: OUT,
        localDataRoot: local.dataRoot,
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
