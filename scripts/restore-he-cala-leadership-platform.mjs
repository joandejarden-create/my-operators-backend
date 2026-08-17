/**
 * Load HE CALA Leadership Platform child rows (org structure, team depth, languages, etc.)
 * into Operator Setup - Leadership Platform for a Master record.
 *
 *   node scripts/restore-he-cala-leadership-platform.mjs [masterRecordId] [path-to-fixture.json]
 *
 * Default master: recWPKu5laVZxsvpn
 * Default fixture: fixtures/operator-leadership-explorer-he-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import { replaceOperatorLeadershipPlatformRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-leadership-explorer-he-cala.json");

function parseArgs(argv) {
  const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
  return {
    masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
    fixturePath: (pos[1] || "").trim() || DEFAULT_FIXTURE,
  };
}

function fixtureToLeadershipPlatformBody(fixture) {
  return {
    leadershipPlatform: {
      orgStructure: fixture.lead_org_structure_json || [],
      teamDepth: fixture.lead_team_depth_json || [],
      languages: fixture.lead_language_capability_json || [],
      governanceCadence: fixture.lead_governance_cadence_json || [],
      teamMarkets: fixture.lead_team_markets_json || [],
      ownerRelationship: fixture.lead_owner_relationship_json || [],
    },
  };
}

function countSections(lp) {
  return {
    orgStructure: (lp.orgStructure || []).length,
    teamDepth: (lp.teamDepth || []).length,
    languages: (lp.languages || []).length,
    governanceCadence: (lp.governanceCadence || []).length,
    teamMarkets: (lp.teamMarkets || []).length,
    ownerRelationship: (lp.ownerRelationship || []).length,
    total:
      (lp.orgStructure || []).length +
      (lp.teamDepth || []).length +
      (lp.languages || []).length +
      (lp.governanceCadence || []).length +
      (lp.teamMarkets || []).length +
      (lp.ownerRelationship || []).length,
  };
}

async function main() {
  const { masterId, fixturePath } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const body = fixtureToLeadershipPlatformBody(fixture);
  const counts = countSections(body.leadershipPlatform);

  if (!counts.total) {
    throw new Error("Fixture has no leadership platform rows");
  }

  const res = await replaceOperatorLeadershipPlatformRows(masterId, body, randomUUID());
  console.log(
    JSON.stringify(
      {
        masterId,
        fixturePath,
        operatorName: fixture._meta?.operatorName || null,
        rowCounts: counts,
        airtable: res,
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
