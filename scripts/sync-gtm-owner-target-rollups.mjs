/**
 * Roll up CoStar Properties → Owner Targets in the GTM base.
 *
 * Reads existing Properties rows (True Owner, RBA/GLA, etc.), creates/updates
 * Owner Targets rollup rows, and links properties to their owner target.
 *
 * Default: dry-run. Use --apply to write.
 *
 * Usage:
 *   AIRTABLE_GTM_BASE_ID=appKZuK006BWIVjNW node scripts/sync-gtm-owner-target-rollups.mjs
 *   AIRTABLE_GTM_BASE_ID=appKZuK006BWIVjNW node scripts/sync-gtm-owner-target-rollups.mjs --apply
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  GTM_OWNER_TARGET_TABLES,
  MAP_GTM_OWNER_TARGET,
  MAP_GTM_PROPERTIES,
} from "../lib/gtm-owner-target/field-map.js";
import {
  getGtmAirtableBase,
  assertGtmBaseConfigured,
  assertNotProductBase,
} from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { computeOwnerRollups } from "../lib/gtm-owner-target/rollup.js";
import {
  validateOwnerTargetWrite,
  buildOwnerTargetFieldsFromRollup,
  buildOwnerTargetUpdateFieldsFromRollup,
} from "../lib/gtm-owner-target/validate.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PREVIEW = join(__dirname, "..", "reports", "gtm-owner-target-rollup-preview.json");
const APPLY = process.argv.includes("--apply");

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function loadExistingOwnerTargets(base) {
  const records = await base(GTM_OWNER_TARGET_TABLES.ownerTargets)
    .select({
      fields: [
        MAP_GTM_OWNER_TARGET.ownerName,
        MAP_GTM_OWNER_TARGET.ownerNameNormalized,
        MAP_GTM_OWNER_TARGET.outreachStatus,
        MAP_GTM_OWNER_TARGET.pitchStatus,
      ],
    })
    .all();

  const byNormalized = new Map();
  for (const rec of records) {
    const key = String(
      rec.fields[MAP_GTM_OWNER_TARGET.ownerNameNormalized] ||
        rec.fields[MAP_GTM_OWNER_TARGET.ownerName] ||
        ""
    )
      .trim()
      .toLowerCase();
    if (key) byNormalized.set(key, rec);
  }
  return { records, byNormalized };
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);

  const { records: propertyRecords } = await fetchAllGtmProperties();
  const groups = groupAirtablePropertiesByOwner(propertyRecords);
  const rollups = computeOwnerRollups(groups);

  const preview = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    baseId,
    propertyCount: propertyRecords.length,
    ownerCount: rollups.length,
    owners: rollups.map((r) => ({
      ownerName: r.ownerName,
      priorityTier: r.priorityTier,
      propertyCount: r.propertyCount,
      totalRbaSf: r.totalRbaSf,
      countriesSummary: r.countriesSummary,
    })),
  };

  mkdirSync(dirname(PREVIEW), { recursive: true });
  writeFileSync(PREVIEW, JSON.stringify(preview, null, 2));
  console.log(`Properties: ${propertyRecords.length} → ${rollups.length} owner rollups`);
  console.log("Wrote", PREVIEW);

  if (!APPLY) {
    console.log("\nDry-run. Re-run with --apply to upsert Owner Targets and link Properties.");
    return;
  }

  const base = getGtmAirtableBase();
  const existingOwners = await loadExistingOwnerTargets(base);
  const ownerIdByKey = new Map();
  let creates = 0;
  let updates = 0;

  for (const rollup of rollups) {
    const existing = existingOwners.byNormalized.get(rollup.ownerNameNormalized);
    if (existing) {
      const fields = buildOwnerTargetUpdateFieldsFromRollup(rollup);
      const validation = validateOwnerTargetWrite({
        ...fields,
        [MAP_GTM_OWNER_TARGET.ownerName]: rollup.ownerName,
        [MAP_GTM_OWNER_TARGET.dataSource]: "costar_internal",
        [MAP_GTM_OWNER_TARGET.dataLicense]: "costar_licensed_internal",
        [MAP_GTM_OWNER_TARGET.visibility]: "internal_only",
      });
      if (!validation.ok) {
        throw new Error(`Owner update failed for ${rollup.ownerName}: ${validation.failures.join("; ")}`);
      }
      await base(GTM_OWNER_TARGET_TABLES.ownerTargets).update([{ id: existing.id, fields }]);
      ownerIdByKey.set(rollup.ownerNameNormalized, existing.id);
      updates++;
    } else {
      const fields = buildOwnerTargetFieldsFromRollup(rollup);
      const validation = validateOwnerTargetWrite(fields);
      if (!validation.ok) {
        throw new Error(`Owner create failed for ${rollup.ownerName}: ${validation.failures.join("; ")}`);
      }
      const created = await base(GTM_OWNER_TARGET_TABLES.ownerTargets).create([{ fields }]);
      ownerIdByKey.set(rollup.ownerNameNormalized, created[0].id);
      creates++;
    }
  }

  let propertyLinks = 0;
  const linkUpdates = [];
  for (const group of groups) {
    const ownerId = ownerIdByKey.get(group.ownerKey);
    if (!ownerId) continue;
    for (const recordId of group.propertyRecordIds) {
      linkUpdates.push({
        id: recordId,
        fields: { [MAP_GTM_PROPERTIES.ownerTargetLink]: [ownerId] },
      });
    }
  }

  for (const batch of chunk(linkUpdates, 10)) {
    await base(GTM_OWNER_TARGET_TABLES.properties).update(batch);
    propertyLinks += batch.length;
  }

  console.log("\nApply complete:");
  console.log(`  Owner Targets created: ${creates}`);
  console.log(`  Owner Targets updated: ${updates}`);
  console.log(`  Properties linked: ${propertyLinks}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
