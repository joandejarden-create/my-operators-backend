#!/usr/bin/env node
/**
 * Apply Arbor Lodging (CALA) profile scrub (history, snapshot signals, transition copy).
 *
 *   node scripts/apply-arbor-cala-profile.mjs
 *   node scripts/apply-arbor-cala-profile.mjs --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  NEW_BASE_PROFILE_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-profile-explorer-arbor-cala.json");

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

async function patchRecord(table, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`${table} PATCH failed (${status}): ${msg}`);
  }
  return json;
}

async function main() {
  const { masterId, fixturePath, dryRun } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const profileFields = fixture.profileFields || {};

  const profRows = await fetchRecordsLinkedToMaster(NEW_BASE_PROFILE_TABLE, masterId);
  if (!profRows.length) throw new Error(`No Profile row for ${masterId}`);

  const summary = {
    masterId,
    fixturePath,
    dryRun,
    profileRecordId: profRows[0].id,
    profileFieldCount: Object.keys(profileFields).length,
  };

  if (dryRun) {
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  if (Object.keys(profileFields).length) {
    await patchRecord(NEW_BASE_PROFILE_TABLE, profRows[0].id, profileFields);
  }

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-profile-explorer-arbor-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  console.log(JSON.stringify({ ...summary, publicFixture }, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
