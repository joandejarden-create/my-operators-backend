/**
 * Production Census coordinate resolver — dry-run only (no Airtable apply).
 *
 *   npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run
 *   npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run --fetch-limit=40 --families=Marriott,IHG
 *
 * Does not write Airtable. Does not invoke Webhound.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  parseResolverArgs,
  runCoordinateResolverDryRun,
  renderResolverDryRunMarkdown,
  STATUS,
} from "../lib/research-engine-v2/production-census-coordinate-resolver.js";
import { COORDINATE_CRAWLER_RULES } from "../lib/research-engine-v2/production-census-coordinate-extractor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

function loadWebhoundClosure(sidecarPath) {
  if (existsSync(sidecarPath)) {
    try {
      return JSON.parse(readFileSync(sidecarPath, "utf8"));
    } catch {
      return null;
    }
  }
  return null;
}

function renderDurableDoc(report, webhound) {
  const s = report.summary || {};
  const v = report.first_pass_coordinate_validation || {};
  return `# Production Census Coordinate Resolver

**Status:** \`${report.status}\`  
**Contract:** \`${report.version}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** false (dry-run only)

## Executive summary

Code-based coordinate resolver replaces Webhound as the production path for Census pins. Webhound was capped/closed as a learning sidecar only.

| Metric | Value |
| --- | ---: |
| Census scanned | ${s.total_records_scanned ?? "—"} |
| Already valid coordinates | ${s.records_with_valid_coordinates ?? "—"} |
| Active missing | ${s.records_missing_coordinates_active ?? "—"} |
| Proposed (dry-run) | ${s.proposed_coordinate_updates ?? "—"} |
| Steward review | ${s.steward_review_records ?? "—"} |
| Pages fetched | ${s.pages_fetched ?? "—"} |
| Webhound production writes | 0 |

## Webhound sidecar

| Item | Value |
| --- | --- |
| Lifecycle | ${webhound?.summary?.webhound_lifecycle || "see closed report"} |
| Spend | $${webhound?.summary?.final_spend_usd ?? "—"} / $${webhound?.summary?.budget_usd ?? "—"} |
| Page visits | ${webhound?.summary?.page_visits ?? "—"} |
| Production writes from Webhound | **0** |
| Session | ${webhound?.summary?.session_url || "https://webhound.ai/session/bbaa85f9-3d19-4b05-a53c-4bc4e44fde02"} |

Full learning table: \`reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.md\`.

## Resolver method

1. Census Official Property URL / Source URL  
2. Fetch official property or brand directory page  
3. Extract JSON-LD geo, family payloads (Marriott/Hilton/Choice/IHG), map embeds, official address  
4. Optional: geocode **official property name + street address only** (\`--allow-official-address-geocode\`)  
5. Validate ranges / reject 0,0 / city centroids / airports  
6. High/Medium → propose; Low/uncertain → steward  

### Crawler rules (code-reproducible)

\`\`\`json
${JSON.stringify(COORDINATE_CRAWLER_RULES, null, 2)}
\`\`\`

## First-pass validation

- Coordinates present: **${v.coordinates_present}**
- Safe: **${v.safe_count}**
- Needs review: **${v.needs_review_count}**
- Shared-campus downgrade-later: **${v.downgrade_later_count}**
- Public Map missing coords: **${v.public_map_missing_coords}**
- Zero-zero: **${v.zero_zero}**

No first-pass coordinates were modified in this task.

## Commands

\`\`\`bash
npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run
npm run research-engine-v2:production-census-coordinate-resolver -- --dry-run --fetch-limit=40 --families=Marriott,IHG
\`\`\`

## Next step

${report.next_step}
`;
}

async function main() {
  const args = parseResolverArgs();
  if (args.apply) {
    console.error("[coord-resolver] --apply is not enabled in this lane. Dry-run only.");
    process.exit(2);
  }

  mkdirSync(REPORTS, { recursive: true });
  mkdirSync(DOCS, { recursive: true });

  console.log(`[coord-resolver] dry-run fetch-limit=${args.fetchLimit} families=${args.families.join(",")}`);
  const report = await runCoordinateResolverDryRun(args);

  const dryJson = join(REPORTS, "production-census-coordinate-resolver-dry-run.json");
  const dryMd = join(REPORTS, "production-census-coordinate-resolver-dry-run.md");
  writeJson(dryJson, report);
  writeMd(dryMd, renderResolverDryRunMarkdown(report));

  const sidecarJson = join(REPORTS, "webhound-coordinate-learning-sidecar-closed.json");
  const webhound = loadWebhoundClosure(sidecarJson);
  writeMd(join(DOCS, "production-census-coordinate-resolver.md"), renderDurableDoc(report, webhound));

  console.log(`[coord-resolver] status=${report.status}`);
  console.log(`[coord-resolver] proposed=${report.summary.proposed_coordinate_updates} steward=${report.summary.steward_review_records} fetched=${report.summary.pages_fetched}`);
  process.exit(report.status === STATUS.DRY_RUN_READY ? 0 : 2);
}

main().catch((err) => {
  console.error("[coord-resolver] fatal:", err);
  process.exit(1);
});
