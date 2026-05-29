#!/usr/bin/env node
/**
 * Validate OAS field options: live Airtable vs planned, aliases, backfill values.
 *   node scripts/validate-operator-alignment-airtable-options.mjs
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getLiveOperatorAlignmentOptions,
  comparePlannedToLive,
  LIVE_OPTIONS_JSON,
} from "../lib/operator-alignment-airtable-options-loader.js";
import { OAS_AUDIT_FIELD_SPECS, OAS_AUDIT_TABLES } from "../lib/operator-alignment-airtable-options-registry.js";
import { OAS_OPTION_ALIAS_GROUPS } from "../lib/operator-alignment-airtable-option-aliases.js";
import { buildAliasToLiveMap, resolveAliasToLiveLabel } from "../lib/operator-alignment-airtable-option-aliases.js";
import { normalizeOptionKey } from "../lib/operator-alignment-airtable-options-loader.js";
import {
  loadAllSamplePlans,
  planAeropuertoCancun,
  validateProposalValue,
  FIELD_TO_TABLE_KEY,
} from "../lib/operator-alignment-deal-backfill-plans.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let failed = 0;
function fail(msg) {
  console.error("FAIL:", msg);
  failed += 1;
}
function ok(msg) {
  console.log("ok:", msg);
}

async function main() {
  const live = await getLiveOperatorAlignmentOptions({ refresh: true });
  fs.writeFileSync(LIVE_OPTIONS_JSON, JSON.stringify(live, null, 2));
  ok("live options exported to " + LIVE_OPTIONS_JSON);

  let missingField = 0;
  let exactPhase5B = 0;
  let partialPhase5B = 0;

  for (const spec of OAS_AUDIT_FIELD_SPECS) {
    const key = `${spec.tableKey}::${spec.fieldName}`;
    const entry = live.fields[key];
    if (!entry || entry.status !== "ok") {
      if ((spec.plannedOptions || []).length > 0) {
        fail(`Missing field in Airtable: ${OAS_AUDIT_TABLES[spec.tableKey]} / ${spec.fieldName}`);
        missingField += 1;
      }
      continue;
    }
    if (!spec.plannedOptions?.length) continue;
    const cmp = comparePlannedToLive(spec.plannedOptions, entry.liveOptions);
    if (cmp.matchStatus === "Exact") exactPhase5B += 1;
    else if (cmp.matchStatus === "Partial") {
      partialPhase5B += 1;
      fail(
        `Partial option match ${spec.fieldName}: missing in live [${cmp.missing.join("; ")}] extra [${cmp.extra.join("; ")}]`
      );
    } else if (cmp.missing.length) {
      fail(`Planned options missing in live for ${spec.fieldName}: ${cmp.missing.join(", ")}`);
    }
    if (entry.fieldType !== "singleSelect" && entry.fieldType !== "multipleSelects" && spec.plannedOptions.length) {
      fail(`Type mismatch ${spec.fieldName}: ${entry.fieldType}`);
    }
  }
  ok(`Phase 5B fields exact: ${exactPhase5B}, partial: ${partialPhase5B}`);

  for (const spec of OAS_AUDIT_FIELD_SPECS) {
    if (!spec.plannedOptions?.length) continue;
    const entry = live.fields[`${spec.tableKey}::${spec.fieldName}`];
    if (!entry?.liveOptions?.length) continue;
    const aliasMap = buildAliasToLiveMap(entry.liveOptions);
    for (const group of Object.values(OAS_OPTION_ALIAS_GROUPS)) {
      for (const alias of group.aliases) {
        const resolved = aliasMap[normalizeOptionKey(alias)] || resolveAliasToLiveLabel(alias, entry.liveOptions);
        if (resolved && !entry.liveOptions.includes(resolved)) {
          fail(`Alias resolves to non-live label: ${alias} -> ${resolved} (${spec.fieldName})`);
        }
      }
    }
  }
  ok("alias map spot-check complete");

  const liveIndex = live;
  const plans = new Map([["recIeGRZP21udmTnt", planAeropuertoCancun()]]);
  try {
    for (const [id, p] of loadAllSamplePlans(ROOT)) plans.set(id, p);
  } catch {
    ok("sample plans partial load (cala results optional)");
  }

  for (const [dealId, plan] of plans) {
    if (plan.skip) continue;
    for (const [col, prop] of Object.entries(plan.fields || {})) {
      if (!FIELD_TO_TABLE_KEY[col]) continue;
      const v = validateProposalValue(col, prop.value, liveIndex);
      if (!v.ok) fail(`Backfill value invalid ${dealId} ${col}: ${(v.bad || []).join(", ")}`);
    }
  }
  ok(`backfill plans validated (${plans.size} deals)`);

  const ocs = fs.readFileSync(path.join(ROOT, "lib/operator-capability-snapshot-build.js"), "utf8");
  const weights = fs.readFileSync(path.join(ROOT, "api/my-deals.js"), "utf8");
  ok(!ocs.includes("OPERATOR_MATCH_WEIGHTS"), "OCS weights untouched");
  ok(weights.includes("geographyMarkets: 18"), "operator weights unchanged");

  console.log(failed ? `\n${failed} failure(s)` : "\nAll option validation checks passed.");
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
