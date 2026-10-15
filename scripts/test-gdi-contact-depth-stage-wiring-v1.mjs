/**
 * Lock: initial coordinator + weekly refresh share contact-depth execution
 * (buyer_contact_enrichment_v1), not queue-only by default.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const coord = fs.readFileSync(
  path.join(root, "scripts/gdi-research-to-page-workflow-v1.mjs"),
  "utf8"
);
const weekly = fs.readFileSync(
  path.join(
    root,
    "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js"
  ),
  "utf8"
);
const contract = fs.readFileSync(
  path.join(
    root,
    "lib/group-demand-intelligence/workflow/buyer-contact-enrichment-contract-v1.js"
  ),
  "utf8"
);

assert.match(coord, /Contact-depth is a required stage/);
assert.match(coord, /skip-buyer-enrichment-execute/);
assert.match(coord, /runBuyerContactEnrichmentPass/);
// Must not leave default path as authorizeSurfe-only gate for execution
assert.doesNotMatch(
  coord,
  /if \(!authorizeSurfe && !has\("--run-buyer-enrichment"\)\)/
);

assert.match(weekly, /runBuyerContactEnrichmentPass/);
assert.match(weekly, /execute_public_contact_depth/);

assert.match(contract, /NAMED_RESEARCH_NOT_ATTEMPTED/);
assert.match(contract, /NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK/);
assert.match(contract, /functional_route_only_named_enrichment_pending/);
assert.match(contract, /org_inbox_present_person_specific_channel_still_required/);

console.log("test:gdi-contact-depth-stage-wiring-v1 OK");
