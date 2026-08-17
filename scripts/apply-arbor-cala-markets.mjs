#!/usr/bin/env node
/**
 * Apply Arbor Lodging (CALA) Markets & Footprint fields from Experiencia Regional deck.
 *
 *   node scripts/apply-arbor-cala-markets.mjs
 *   node scripts/apply-arbor-cala-markets.mjs --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_GOVERNANCE_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorLeadershipPlatformRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recF5Z87OAqFgndoq";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-markets-explorer-arbor-cala.json");
const DEFAULT_LEADERSHIP_FIXTURE = path.join(
  ROOT,
  "fixtures",
  "operator-leadership-explorer-arbor-cala.json"
);

function parseArgs(argv) {
  const args = argv.slice(2);
  return {
    dryRun: args.includes("--dry-run"),
    masterId: args.find((a) => !a.startsWith("--")) || DEFAULT_MASTER,
    fixturePath:
      args.filter((a) => !a.startsWith("--"))[1] || DEFAULT_FIXTURE,
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
  const platformFields = {
    ...(fixture.platformFields || {}),
    ...(fixture.footprintGeoFields || {}),
  };
  const governanceFields = fixture.governanceFields || {};
  const leadershipFixturePath = DEFAULT_LEADERSHIP_FIXTURE;
  const leadershipFixture = fs.existsSync(leadershipFixturePath)
    ? JSON.parse(fs.readFileSync(leadershipFixturePath, "utf8"))
    : null;

  const platRows = await fetchRecordsLinkedToMaster(NEW_BASE_PLATFORM_TABLE, masterId);
  const govRows = await fetchRecordsLinkedToMaster(NEW_BASE_GOVERNANCE_TABLE, masterId);
  if (!platRows.length) throw new Error(`No Platform row for ${masterId}`);
  if (!govRows.length) throw new Error(`No Governance row for ${masterId}`);

  const summary = {
    masterId,
    fixturePath,
    leadershipFixturePath,
    dryRun,
    platformRecordId: platRows[0].id,
    governanceRecordId: govRows[0].id,
    platformFieldCount: Object.keys(platformFields).length,
    governanceFieldCount: Object.keys(governanceFields).length,
    leadershipTeamMarkets: leadershipFixture?.lead_team_markets_json?.length || 0,
  };

  if (dryRun) {
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  await patchRecord(NEW_BASE_PLATFORM_TABLE, platRows[0].id, platformFields);
  await patchRecord(NEW_BASE_GOVERNANCE_TABLE, govRows[0].id, governanceFields);

  if (leadershipFixture?.lead_team_markets_json?.length) {
    const leadershipBody = {
      leadershipPlatform: {
        orgStructure: leadershipFixture.lead_org_structure_json || [],
        teamDepth: leadershipFixture.lead_team_depth_json || [],
        languages: leadershipFixture.lead_language_capability_json || [],
        governanceCadence: leadershipFixture.lead_governance_cadence_json || [],
        teamMarkets: leadershipFixture.lead_team_markets_json || [],
        ownerRelationship: leadershipFixture.lead_owner_relationship_json || [],
      },
    };
    const lpRes = await replaceOperatorLeadershipPlatformRows(
      masterId,
      leadershipBody,
      randomUUID()
    );
    summary.leadershipPlatform = lpRes;
  }

  const publicFixture = path.join(ROOT, "public", "fixtures", "operator-markets-explorer-arbor-cala.json");
  fs.mkdirSync(path.dirname(publicFixture), { recursive: true });
  fs.copyFileSync(fixturePath, publicFixture);

  const bundle = await loadNewBaseOperatorBundle(masterId);
  const prefill = buildPrefillObjectFromNewBaseRows(
    bundle.master,
    bundle.profile,
    bundle.platform,
    bundle.commercial,
    bundle.governance
  );

  console.log(
    JSON.stringify(
      {
        ...summary,
        publicFixture,
        teamExperienceMarkets: prefill.teamExperienceMarkets || null,
        activeCountries: prefill.activeCountries || null,
        activeMarkets: prefill.activeMarkets || null,
        targetGrowthMarkets: prefill.specificMarkets || prefill.targetGrowthMarkets || null,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
