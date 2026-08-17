#!/usr/bin/env node
/**
 * Apply Arbor Lodging (CALA) Operating Platform child rows + platform cap_* mirror.
 *
 *   node scripts/apply-arbor-cala-operating.mjs
 *   node scripts/apply-arbor-cala-operating.mjs --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_PLATFORM_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorOperatingPlatformRows } from "../api/lib/operator-setup-new-base-writer.js";
import { applyOperatingPlatformToLegacyPrefill } from "../api/lib/operator-operating-platform-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-operating-explorer-arbor-cala.json");

function parseArgs(argv) {
  const args = argv.slice(2);
  return {
    dryRun: args.includes("--dry-run"),
    masterId: args.find((a) => !a.startsWith("--")) || DEFAULT_MASTER,
    fixturePath: args.filter((a) => !a.startsWith("--"))[1] || DEFAULT_FIXTURE,
  };
}

function enc(s) {
  return encodeURIComponent(s);
}

const PLATFORM_MIRROR_KEY = /^(cap_|op_)/;

function buildPlatformPatch(platformRaw, operatingPlatform) {
  const patch = {};
  const prefill = {};
  applyOperatingPlatformToLegacyPrefill(prefill, operatingPlatform);
  Object.entries(prefill).forEach(([key, value]) => {
    if (value == null || value === "") return;
    if (!PLATFORM_MIRROR_KEY.test(key)) return;
    patch[key] = value;
  });
  Object.entries(platformRaw || {}).forEach(([key, value]) => {
    if (value == null || value === "") return;
    if (!PLATFORM_MIRROR_KEY.test(key)) return;
    patch[key] = value;
  });
  return patch;
}

async function patchPlatformRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_PLATFORM_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Platform PATCH failed (${status}): ${msg}`);
  }
  return json;
}

async function main() {
  const { masterId, fixturePath, dryRun } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const operatingPlatform = fixture.operatingPlatform;
  if (!operatingPlatform) throw new Error("Fixture missing operatingPlatform");

  const platRows = await fetchRecordsLinkedToMaster(NEW_BASE_PLATFORM_TABLE, masterId);
  if (!platRows.length) throw new Error(`No Platform row for ${masterId}`);

  const platPatch = buildPlatformPatch(fixture.platformFields || {}, operatingPlatform);
  const summary = {
    masterId,
    fixturePath,
    dryRun,
    platformRecordId: platRows[0].id,
    platformFieldCount: Object.keys(platPatch).length,
  };

  if (dryRun) {
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  if (Object.keys(platPatch).length) {
    await patchPlatformRecord(platRows[0].id, platPatch);
  }
  const opRes = await replaceOperatorOperatingPlatformRows(
    masterId,
    { operatingPlatform },
    randomUUID()
  );

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-operating-explorer-arbor-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(JSON.stringify({ ...summary, publicFixture, operatingPlatform: opRes }, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
