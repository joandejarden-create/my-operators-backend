/**
 * Production Census first-pass enrichment — dry-run / apply.
 *
 *   npm run research-engine-v2:production-census-first-pass-enrichment -- --dry-run
 *
 *   ALLOW_PRODUCTION_CENSUS_FIRST_PASS=1 \
 *   CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
 *   CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
 *   CONFIRM_NO_ROOM_DATE_WRITES=1 \
 *   npm run research-engine-v2:production-census-first-pass-enrichment -- --apply \
 *     --confirm-first-pass-census-enrichment \
 *     --confirm-source-supported-coordinates-only \
 *     --confirm-no-city-centroid-coordinates \
 *     --confirm-no-zero-zero-coordinates \
 *     --confirm-official-public-sources-only \
 *     --confirm-no-brand-explorer-writes \
 *     --confirm-no-owner-operator-writes \
 *     --confirm-no-room-date-writes \
 *     --confirm-no-recent-momentum \
 *     --confirm-held-records-blocked
 */

import { execSync } from "node:child_process";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseFirstPassArgs,
  checkFirstPassEnvFlags,
  runFirstPassDryRun,
  runFirstPassApply,
  renderFirstPassDryRunMarkdown,
  renderFirstPassApplyMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-first-pass-enrichment.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const DRY_JSON = join(REPORTS, "production-census-first-pass-enrichment-dry-run.json");
const DRY_MD = join(REPORTS, "production-census-first-pass-enrichment-dry-run.md");
const APPLY_JSON = join(REPORTS, "production-census-first-pass-enrichment-apply.json");
const APPLY_MD = join(REPORTS, "production-census-first-pass-enrichment-apply.md");
const DOC_MD = join(DOCS, "production-census-first-pass-enrichment.md");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function stripProposalsForDisk(report) {
  const { proposals, ...rest } = report;
  return {
    ...rest,
    proposals_count: Array.isArray(proposals) ? proposals.length : 0,
    proposals_omitted: true,
    proposals_omitted_reason: "Full proposal payloads kept for apply in-process; disk report uses summary + sample",
  };
}

function runBeGate(label, command) {
  try {
    execSync(command, {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      maxBuffer: 20 * 1024 * 1024,
    });
    return { label, command, ok: true, exit_code: 0 };
  } catch (err) {
    return {
      label,
      command,
      ok: false,
      exit_code: err.status ?? 1,
      stderr_tail: (err.stderr?.toString?.() || "").slice(-1000),
    };
  }
}

function loadBeSummary(gates) {
  const safety = {
    gates: gates.map((g) => ({
      label: g.label,
      ok: g.ok,
      exit_code: g.exit_code,
      command: g.command,
    })),
    all_pass: gates.every((g) => g.ok),
  };
  try {
    const u = JSON.parse(
      readFileSync(join(ROOT, "reports/brand-explorer-active-universe-source-of-truth.json"), "utf8")
    );
    safety.active_universe = u.activeSourceOfTruth?.totalCount ?? 62;
  } catch {
    safety.active_universe = null;
  }
  return safety;
}

function renderDurableDoc({ dry, apply, beSafety, webhound }) {
  const s = dry.summary || {};
  return `# Production Census First Pass Enrichment

**Status:** \`${apply?.status || dry.status}\`  
**Contract:** \`production-census-first-pass-enrichment-v1\`  
**Generated:** ${new Date().toISOString()}

## Executive summary

First-pass production enrichment for Hotel Property Census (666). Fills safe fields from VIC official directory/property claims, classifies Radar readiness, and queues blocked owner/operator/rooms/date values without writing them.

| Metric | Value |
| --- | ---: |
| Total scanned | ${s.total_records_scanned ?? "—"} |
| Active-brand mapped | ${s.active_brand_mapped_records ?? "—"} |
| Eligible | ${s.eligible_records ?? "—"} |
| Blocked | ${s.blocked_records ?? "—"} |
| Coordinate updates | ${s.coordinate_updates_proposed ?? "—"} |
| Radar updates | ${s.radar_status_updates_proposed ?? "—"} |
| Amenity updates | ${s.amenity_updates_proposed ?? "—"} |
| Description updates | ${s.description_updates_proposed ?? "—"} |
| Blocked research queue | ${s.blocked_field_research_queue_count ?? "—"} |
| Airtable updates | ${apply?.updates_written ?? s.exact_airtable_update_count ?? "—"} |

## Active-brand scope

Mapped via Brand Explorer Active/Live 62 baseline (slug, name, aliases). Held / Brand-Unconfirmed / uncertain mappings are excluded from content enrichment and classified Radar **Hold**.

## Coordinate rules

- Source: VIC freeze \`field_claims\` for Latitude/Longitude (Hilton directory localization; Choice hotel-card geoLocation; IHG when present).
- Reject: missing URL, Low confidence, 0,0, city-centroid / airport sources.
- Marriott freeze currently has no property-level coords → Public List Eligible until next sourcing lane.
- Shared campus pins: still written when official, Public Display Confidence downgraded to Medium.

## Radar readiness

Populated after geography review using v1.1.2 select options only.

## Safe enrichment

Amenities / structured tags / strategic flags / property type / asset context / Dealality Market·Submarket when source-supported. Descriptions written only when VIC has grounded description source text (rare in current freeze).

## Blocked fields (research queue only)

Owner Name, Developer, Operator / Management Company, Rooms / Keys, Opening Date, Renovation Date, Affiliation Start Date — never written in this lane.

## Webhound usage

${webhound || "Optional sidecar: Marriott Mexico hotel page coordinate extraction patterns (source-discovery only; no production writes)."}

## Forbidden fields untouched

Owner / operator / rooms / dates / Company Validated / Brand Verified / Recent Momentum / Brand Explorer public fields.

## Brand Explorer safety

\`\`\`json
${JSON.stringify(beSafety || { note: "run gates after apply" }, null, 2)}
\`\`\`

## Commands

\`\`\`bash
npm run research-engine-v2:production-census-first-pass-enrichment -- --dry-run

ALLOW_PRODUCTION_CENSUS_FIRST_PASS=1 \\
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \\
CONFIRM_NO_ROOM_DATE_WRITES=1 \\
npm run research-engine-v2:production-census-first-pass-enrichment -- --apply \\
  --confirm-first-pass-census-enrichment \\
  --confirm-source-supported-coordinates-only \\
  --confirm-no-city-centroid-coordinates \\
  --confirm-no-zero-zero-coordinates \\
  --confirm-official-public-sources-only \\
  --confirm-no-brand-explorer-writes \\
  --confirm-no-owner-operator-writes \\
  --confirm-no-room-date-writes \\
  --confirm-no-recent-momentum \\
  --confirm-held-records-blocked
\`\`\`

## Reports

- \`reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.md\`
- \`reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.json\`
- \`reports/research-engine-v2/production-census-first-pass-enrichment-apply.md\`
- \`reports/research-engine-v2/production-census-first-pass-enrichment-apply.json\`

## Next recommended lane

${apply?.next_recommended_lane || dry.next_recommended_lane || "—"}
`;
}

async function main() {
  const args = parseFirstPassArgs();
  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(`[first-pass] mode=${args.apply ? "apply" : "dry-run"}`);

  const dry = await runFirstPassDryRun();
  const dryDisk = stripProposalsForDisk(dry);
  // Persist a compact proposal index for audit (no huge field_claims)
  dryDisk.proposal_index = (dry.proposals || []).map((p) => ({
    record_id: p.record_id,
    identity_key: p.identity_key,
    property_name: p.property_name,
    eligible: p.eligible,
    block_reason: p.block_reason,
    brand_mapping: p.brand_mapping,
    patch_fields: Object.keys(p.patch || {}),
    coordinate: p.coordinate,
    source_urls: (p.sources || []).map((s) => s.source_url).filter(Boolean),
  }));
  dryDisk.blocked_field_research_queue = dry.blocked_field_research_queue;
  writeJson(DRY_JSON, dryDisk);
  writeMd(DRY_MD, renderFirstPassDryRunMarkdown(dry));
  console.log(`[first-pass] dry-run status=${dry.status} updates=${dry.summary?.exact_airtable_update_count}`);

  if (!args.apply) {
    writeMd(
      DOC_MD,
      renderDurableDoc({
        dry,
        webhound:
          "Sidecar session started for Marriott Mexico coordinate extraction patterns (webhound session; no Airtable writes).",
      })
    );
    console.log(`[first-pass] wrote ${DRY_MD}`);
    process.exit(dry.dry_run_pass ? 0 : 2);
  }

  const env = checkFirstPassEnvFlags();
  if (!args.allConfirms || !env.allOk) {
    const blocked = {
      version: dry.version,
      status: STATUS.CONFIRMATION_MISSING,
      apply_executed: false,
      missing_cli_confirms: Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k),
      env_flags: env.flags,
    };
    writeJson(APPLY_JSON, blocked);
    writeMd(APPLY_MD, renderFirstPassApplyMarkdown(blocked));
    console.error("[first-pass] apply blocked: missing confirms or env flags");
    process.exit(2);
  }

  const apply = await runFirstPassApply(dry);
  writeJson(APPLY_JSON, apply);
  writeMd(APPLY_MD, renderFirstPassApplyMarkdown(apply));
  console.log(`[first-pass] apply status=${apply.status} written=${apply.updates_written}`);

  console.log("[first-pass] running Brand Explorer safety gates…");
  const gates = [
    runBeGate("active_universe_sot", "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"),
    runBeGate(
      "global_active_semantic",
      "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh"
    ),
    runBeGate("pvql_quiet", "node scripts/brand-explorer-quiet-sequential-pvql.mjs"),
    runBeGate(
      "momentum_evidence",
      "npm run test:brand-explorer-recent-momentum-evidence-quality"
    ),
    runBeGate("mandatory_release_gates", "npm run test:brand-explorer-mandatory-release-gates"),
  ];
  const beSafety = loadBeSummary(gates);
  apply.brand_explorer_safety = beSafety;
  writeJson(APPLY_JSON, apply);

  writeMd(
    DOC_MD,
    renderDurableDoc({
      dry,
      apply,
      beSafety,
      webhound:
        "Sidecar: Marriott Mexico hotel page coordinate extraction patterns — improves next-lane crawler logic only; never direct production writes.",
    })
  );

  const ok = apply.status === STATUS.APPLIED && beSafety.all_pass;
  process.exit(ok ? 0 : 2);
}

main().catch((err) => {
  console.error("[first-pass] fatal:", err);
  process.exit(1);
});
