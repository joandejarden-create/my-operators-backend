#!/usr/bin/env node
/**
 * Certify Full E2E → research inventory persistence bridge (dry-run / fail-closed).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  evaluateResearchPersistenceAuthorization,
  persistFullE2eResearchInventory,
  RESEARCH_PERSISTENCE_ACTIVATION,
  GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { preflightCanonicalFullE2e } from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-research-consolidation-and-full-e2e-persistence-v1",
);

assert.equal(RESEARCH_PERSISTENCE_ACTIVATION.disabledByDefault, true);
assert.equal(
  RESEARCH_PERSISTENCE_ACTIVATION.separateFromPaidResearchAuth,
  true,
);

const locked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "test-token",
  authorizePaidResearch: true,
});
assert.equal(locked.productionUnlocked, false);
assert.ok(
  locked.blockers.some((b) => b.code === "ENV_PRODUCTION_GATE_OFF"),
);

const refusedPublish = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "test-token",
  authorizePublish: true,
});
assert.equal(refusedPublish.productionUnlocked, false);
assert.ok(
  refusedPublish.blockers.some(
    (b) => b.code === "PUBLICATION_FLAG_NOT_ACCEPTED_ON_RESEARCH_BRIDGE",
  ),
);

const prevEnv = process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES;
delete process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES;

// Empty candidate set — exercises bridge controls without Airtable I/O.
const dry = await persistFullE2eResearchInventory({
  hotelId: "recLuxvwwxID7U2B8",
  runId: "test-run-persistence-bridge",
  repoRoot: ROOT,
  researchCompleted: true,
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "should-still-dry-run",
  opportunities: [],
});

assert.equal(dry.version, GDI_FULL_E2E_RESEARCH_PERSISTENCE_BRIDGE_V1);
assert.equal(dry.dryRun, true);
assert.equal(dry.blockedApply, true);
assert.equal(dry.gdoWrites, 0);
assert.equal(dry.publicationWrites, 0);
assert.equal(dry.destinationTableId, "tblbkDj0vhQLfrssV");
assert.equal(dry.counts.candidates, 0);

// Optional live dry-run only when credentials are present.
const hasAirtable =
  Boolean(process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT) &&
  process.env.GDI_BRIDGE_LIVE_DRY_RUN === "1";
if (hasAirtable) {
  const liveDry = await persistFullE2eResearchInventory({
    hotelId: "recLuxvwwxID7U2B8",
    runId: "test-run-persistence-bridge-live-dry",
    repoRoot: ROOT,
    researchCompleted: true,
    apply: false,
    authorizeResearchPersistence: false,
    opportunities: [
      {
        opportunityId: "gdi_test_persistence_bridge_synthetic_001",
        organizationName: "Synthetic Persistence Bridge Org",
        title: "Synthetic persistence bridge opportunity",
        segment: "Association / Government",
        priority: "MEDIUM_PRIORITY",
      },
    ],
  });
  assert.equal(liveDry.dryRun, true);
  assert.equal(liveDry.gdoWrites, 0);
  assert.ok(liveDry.rows.length === 1);
}

const preflight = preflightCanonicalFullE2e({
  hotelId: "recLuxvwwxID7U2B8",
  mode: "fixture",
});
assert.equal(preflight.authorizePublish, false);
assert.equal(preflight.publicationAuthorized, false);

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "BRIDGE_TEST_SNAPSHOT.json"),
  `${JSON.stringify(
    {
      ok: true,
      dryRun: dry.dryRun,
      blockedApply: dry.blockedApply,
      counts: dry.counts,
      productionUnlocked: locked.productionUnlocked,
    },
    null,
    2,
  )}\n`,
);

if (prevEnv === undefined) {
  delete process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES;
} else {
  process.env.GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = prevEnv;
}

console.log("PASS test:gdi-full-e2e-research-persistence-bridge-v1");
