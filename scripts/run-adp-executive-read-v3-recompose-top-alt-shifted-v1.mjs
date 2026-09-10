#!/usr/bin/env node
/**
 * Recompose Executive Read V3 for properties whose Top Alternative shifted
 * after competitor extract reprocess (additive immutable editions).
 *
 *   node scripts/run-adp-executive-read-v3-recompose-top-alt-shifted-v1.mjs
 *   node scripts/run-adp-executive-read-v3-recompose-top-alt-shifted-v1.mjs --apply
 *   node scripts/run-adp-executive-read-v3-recompose-top-alt-shifted-v1.mjs --apply --all-published
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { generateImmutableV3EditionForProperty } from "../lib/ai-demand-positioning/executive-read-v3/immutable-edition-activation-v3.js";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { readShareRegistry } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";

const APPLY = process.argv.includes("--apply");
const ALL = process.argv.includes("--all-published");

const TOP_ALT_SHIFTED = [
  "adp_bethesda_marriott",
  "adp_hotel_caribe_faranda_grand",
  "adp_jw_marriott_monterrey_valle",
  "adp_jw_marriott_santo_domingo",
  "adp_renaissance_times_square",
];

const propertyIds = ALL ? listPublishedPropertyIds() : TOP_ALT_SHIFTED;
const shareRegistry = readShareRegistry();
const rows = [];

for (const propertyId of propertyIds) {
  const result = generateImmutableV3EditionForProperty(propertyId, {
    apply: APPLY,
    shareRegistry,
  });
  rows.push({
    propertyId,
    ok: result?.ok === true,
    dryRun: result?.dryRun === true,
    reason: result?.reason || null,
    newEditionId: result?.edition?.newEditionId || result?.edition?.editionId || null,
    compositionHash: result?.edition?.compositionHash || null,
    appliedToPublished: APPLY && result?.ok === true && !result?.dryRun,
    detail: result?.detail || null,
  });
  console.log(
    JSON.stringify(
      {
        propertyId,
        ok: rows[rows.length - 1].ok,
        newEditionId: rows[rows.length - 1].newEditionId,
        apply: APPLY,
      },
      null,
      0
    )
  );
}

const summary = {
  apply: APPLY,
  allPublished: ALL,
  total: rows.length,
  ok: rows.filter((r) => r.ok).length,
  fail: rows.filter((r) => !r.ok).length,
  rows,
};
console.log(JSON.stringify(summary, null, 2));
if (rows.some((r) => !r.ok)) process.exit(1);
