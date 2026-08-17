/**
 * Apply HE CALA Recognition Overview fields (Governance certifications, Commercial industryRecognition, Profile overview signals).
 *
 *   node scripts/restore-he-cala-recognition.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_PROFILE_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const FIXTURE = path.join(ROOT, "fixtures", "operator-recognition-explorer-he-cala.json");

function enc(s) {
  return encodeURIComponent(s);
}

async function patchRecord(tableName, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    throw new Error(`${tableName} PATCH failed (${status}): ${json?.error?.message || JSON.stringify(json)}`);
  }
  return json;
}

async function main() {
  const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const masterId = fixture._meta?.operatorMasterId || DEFAULT_MASTER;

  const [govRows, commRows, profRows] = await Promise.all([
    fetchRecordsLinkedToMaster(NEW_BASE_GOVERNANCE_TABLE, masterId),
    fetchRecordsLinkedToMaster(NEW_BASE_COMMERCIAL_TABLE, masterId),
    fetchRecordsLinkedToMaster(NEW_BASE_PROFILE_TABLE, masterId),
  ]);

  if (!govRows.length) throw new Error(`No Governance row for ${masterId}`);
  if (!commRows.length) throw new Error(`No Commercial row for ${masterId}`);
  if (!profRows.length) throw new Error(`No Profile row for ${masterId}`);

  const results = {};

  if (fixture.governanceFields || fixture.leadershipFields?.lead_narrative_functional) {
    const govPatch = { ...govRows[0].fields };
    if (fixture.governanceFields?.certifications) {
      govPatch.certifications = fixture.governanceFields.certifications;
    }
    if (fixture.leadershipFields?.lead_narrative_functional) {
      govPatch.lead_narrative_functional = fixture.leadershipFields.lead_narrative_functional;
    }
    results.governance = await patchRecord(NEW_BASE_GOVERNANCE_TABLE, govRows[0].id, govPatch);
  }

  if (fixture.commercialFields?.industryRecognition) {
    results.commercial = await patchRecord(
      NEW_BASE_COMMERCIAL_TABLE,
      commRows[0].id,
      {
        ...commRows[0].fields,
        industryRecognition: fixture.commercialFields.industryRecognition,
      }
    );
  }

  if (fixture.profileFields) {
    results.profile = await patchRecord(NEW_BASE_PROFILE_TABLE, profRows[0].id, {
      ...profRows[0].fields,
      ...fixture.profileFields,
    });
  }

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-recognition-explorer-he-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(FIXTURE, publicFixture);

  console.log(JSON.stringify({ masterId, patched: Object.keys(results) }, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
