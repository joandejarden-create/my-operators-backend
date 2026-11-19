/**
 * Gates for Mullen HPC population pilot (no live Airtable required).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  IDENTITY_OUTCOME,
  MULLEN_PILOT_BASE_ID_CANONICAL,
  MULLEN_PILOT_HOTELS,
  MULLEN_PILOT_HPC_TABLE_ID,
  founderPreviewUrl,
  mapHpcCollisionToIdentityOutcome,
} from "../lib/hotel-intelligence/hpc-population/mullen-pilot-cohort-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

function mustInclude(hay, needle) {
  assert.ok(hay.includes(needle), `missing ${needle}`);
}

assert.equal(MULLEN_PILOT_HOTELS.length, 8);
assert.equal(MULLEN_PILOT_BASE_ID_CANONICAL, "appCCUsuGsE1ifoLk");
assert.equal(MULLEN_PILOT_HPC_TABLE_ID, "tbl9aY5ijiuIzzWam");
assert.equal(
  MULLEN_PILOT_HOTELS.find((h) => h.pilotKey === "bethesda_marriott")?.knownHpcId,
  "recLuxvwwxID7U2B8"
);
assert.ok(
  MULLEN_PILOT_HOTELS.every((h) => h.hotelName && Array.isArray(h.aliases))
);

assert.equal(
  mapHpcCollisionToIdentityOutcome({
    outcome: "HPC_EXISTING_RECORD_VERIFIED",
    hpcRecordId: "recLuxvwwxID7U2B8",
  }),
  IDENTITY_OUTCOME.MATCHED_HPC
);
assert.equal(
  mapHpcCollisionToIdentityOutcome({ outcome: "HPC_IDENTITY_COLLISION_REQUIRES_REVIEW" }),
  IDENTITY_OUTCOME.AMBIGUOUS_MATCH
);
assert.equal(
  mapHpcCollisionToIdentityOutcome({ outcome: "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT" }, []),
  IDENTITY_OUTCOME.NEW_HPC_CANDIDATE
);
assert.equal(
  mapHpcCollisionToIdentityOutcome(
    { outcome: "HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT" },
    [{ id: "recCensus1", score: 0.9 }]
  ),
  IDENTITY_OUTCOME.MATCHED_HOTEL_CENSUS
);

assert.equal(
  founderPreviewUrl("recLuxvwwxID7U2B8"),
  "http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recLuxvwwxID7U2B8&previewMode=proposed"
);
assert.equal(founderPreviewUrl("not-a-rec"), null);

const workflow = fs.readFileSync(
  path.join(root, "docs/ai-build-system/HE_HPC_POPULATION_WORKFLOW_V1.md"),
  "utf8"
);
mustInclude(workflow, "DISCOVER → MATCH → RESEARCH → VERIFY → DRY-RUN → APPROVED APPLY");
mustInclude(workflow, "applyAuthorized");
mustInclude(workflow, "hpc-live-identity-collision-v1.js");
mustInclude(workflow, "STOP");

const runner = fs.readFileSync(
  path.join(root, "scripts/he-mullen-pilot-hpc-identity-resolution-v1.mjs"),
  "utf8"
);
mustInclude(runner, "applyAuthorized: false");
mustInclude(runner, "resolveMullenPilotIdentities");
assert.ok(!/Airtable\.update|\.create\(|--apply/.test(runner));

const resolver = fs.readFileSync(
  path.join(root, "lib/hotel-intelligence/hpc-population/mullen-pilot-identity-resolution-v1.js"),
  "utf8"
);
mustInclude(resolver, "performLiveHpcIdentityCollisionCheck");
mustInclude(resolver, "writeBlocked: true");
mustInclude(resolver, "airtableWrites: 0");

console.log("PASS test-he-mullen-pilot-hpc-population-v1");
