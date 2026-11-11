#!/usr/bin/env node
/**
 * Permanent local GDI published-inventory mirror regression (CI-safe).
 *
 * Always uses a deterministic temp fixture built from the deploy-bundled
 * Spice approved pack — does NOT require the founder Windows mirror path.
 *
 * When the optional shared Windows/dev mirror is present, also verifies it
 * still serves the certified Spice seven (local acceptance only).
 *
 *   node scripts/test-gdi-local-published-inventory-mirror-v1.mjs
 *   npm run test:gdi-local-published-inventory-mirror-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  diagnoseGdiPublishedInventoryRead,
  resolveGdiPublishedInventoryRoot,
  GDI_LOCAL_DEV_PUBLISHED_INVENTORY_MIRROR_DEFAULT,
} from "../lib/group-demand-intelligence/durable-publication/resolve-gdi-published-registry-root-v1.js";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
  COMMERCIAL_PUBLICATION_CLASS,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import {
  resolveApprovedPublicationOpportunities,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  SPICE_ISLAND_HOTEL_ID,
} from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";
import { stampApprovedPackAsCustomerFacing } from "../lib/group-demand-intelligence/durable-publication/gdi-approved-pack-customer-inventory-fallback-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WELL_KNOWN = path.resolve(GDI_LOCAL_DEV_PUBLISHED_INVENTORY_MIRROR_DEFAULT);
const EXPECTED = [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS].sort();

function withEnv(extra, fn) {
  const prev = { ...process.env };
  for (const [k, v] of Object.entries(extra)) {
    if (v === "" || v == null) delete process.env[k];
    else process.env[k] = String(v);
  }
  try {
    return fn();
  } finally {
    for (const k of Object.keys(process.env)) {
      if (!(k in prev)) delete process.env[k];
    }
    Object.assign(process.env, prev);
  }
}

function assertSpiceSeven(rows, label) {
  const ids = (rows || []).map((o) => o.id || o.opportunityId).filter(Boolean).sort();
  assert.deepEqual(ids, EXPECTED, `${label}: Spice seven IDs mismatch`);
  const sales = (rows || []).filter(
    (o) =>
      o.inventorySalesReady === true ||
      o.commercialInventoryClass === "SALES_READY" ||
      o.commercialClassification === "SALES_READY" ||
      o.commercialClassification === COMMERCIAL_PUBLICATION_CLASS.SALES_READY
  ).length;
  assert.equal(sales, 1, `${label}: expected 1 Sales Ready`);
  const hangers = (rows || []).find((o) => o.id === "gdi_s4912_b6d9b1929c05");
  assert.ok(hangers, `${label}: Hangers opportunity missing`);
  const body = JSON.stringify(hangers);
  assert.match(body, /Khaleen|Johnson|Aviann|Murray|Hangers/i, `${label}: contacts`);
  assert.match(body, /qualificationQuestions|establishedFacts/i, `${label}: drawer fields`);
}

function buildCiFixtureRegistry() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pub-inv-ci-"));
  const resolved = resolveApprovedPublicationOpportunities({
    hotelId: SPICE_ISLAND_HOTEL_ID,
    opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  });
  assert.ok(
    resolved?.ok && (resolved.opportunities || []).length >= 7,
    `approved pack must resolve Spice opportunities for CI fixture (missing=${JSON.stringify(resolved?.missing || [])})`
  );
  const stamped = stampApprovedPackAsCustomerFacing(
    resolved.opportunities,
    SPICE_ISLAND_HOTEL_ID
  );
  const registry = createGdiPublishedInventoryRegistry({
    root: tmp,
    env: {
      GDI_PUBLISHED_INVENTORY_ROOT: tmp,
      GDI_PUBLISHED_INVENTORY_REGISTRY_V1: "1",
    },
  });
  const approval = {
    authorized: true,
    authorizedBy: "ci_fixture",
    authorizationPhrase: "CI_FIXTURE_ONLY",
  };
  for (const row of stamped) {
    const classification =
      row.commercialInventoryClass ||
      row.commercialClassification ||
      COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT;
    const opportunityId = row.id || row.opportunityId;
    registry.publishOpportunity({
      hotelId: SPICE_ISLAND_HOTEL_ID,
      opportunityId,
      classification,
      approval,
      snapshot: { ...row, id: opportunityId },
      // Approved-pack rows are already customer-certified; gate may be stricter than pack shape.
      skipCommercialContentGate: true,
    });
  }
  return tmp;
}

const fixtureRoot = buildCiFixtureRegistry();

// 1) Production must NEVER auto-select the Windows/dev mirror
withEnv(
  {
    NODE_ENV: "production",
    GDI_PUBLISHED_INVENTORY_ROOT: "",
    GDI_PUBLISHED_INVENTORY_DEV_MIRROR_ROOT: "",
  },
  () => {
    const resolved = path.resolve(resolveGdiPublishedInventoryRoot());
    assert.notEqual(
      resolved,
      WELL_KNOWN,
      "production must not auto-select the local Windows dev mirror"
    );
  }
);

// 2) Explicit fixture root wins
withEnv(
  {
    NODE_ENV: "development",
    GDI_PUBLISHED_INVENTORY_ROOT: fixtureRoot,
    GDI_PUBLISHED_INVENTORY_REGISTRY_V1: "1",
    GDI_PUBLISHED_INVENTORY_HOTEL_IDS: SPICE_ISLAND_HOTEL_ID,
  },
  () => {
    assert.equal(
      path.resolve(resolveGdiPublishedInventoryRoot()),
      path.resolve(fixtureRoot)
    );
    const diag = diagnoseGdiPublishedInventoryRead(SPICE_ISLAND_HOTEL_ID);
    assert.equal(diag.ok, true, `diagnostic not ok: ${diag.code}`);
    assert.equal(diag.hotelPublishedOpportunityCount, 7);
    const rows = mergePublishedInventoryIntoOpportunities(
      SPICE_ISLAND_HOTEL_ID,
      [],
      { strictPublishedOnly: true }
    );
    assertSpiceSeven(rows, "ci-fixture-merge");
  }
);

// 3) Missing registry → explicit diagnostic (never silent empty OK)
withEnv(
  {
    NODE_ENV: "development",
    GDI_PUBLISHED_INVENTORY_ROOT: path.join(ROOT, "reports", "_missing-gdi-registry-test"),
    GDI_PUBLISHED_INVENTORY_REGISTRY_V1: "1",
    GDI_PUBLISHED_INVENTORY_HOTEL_IDS: SPICE_ISLAND_HOTEL_ID,
  },
  () => {
    const diag = diagnoseGdiPublishedInventoryRead(SPICE_ISLAND_HOTEL_ID);
    assert.equal(diag.ok, false);
    assert.equal(diag.code, "REGISTRY_UNAVAILABLE");
    const leaked = mergePublishedInventoryIntoOpportunities(
      SPICE_ISLAND_HOTEL_ID,
      [{ id: "gdi_research_only_must_not_leak", title: "research row" }],
      {
        strictPublishedOnly: true,
        registry: { listPublished: () => [] },
      }
    );
    assert.deepEqual(leaked, []);
  }
);

// 4) Optional local shared mirror acceptance (skip on CI / machines without mirror)
let localMirrorVerified = false;
const localRegistry = path.join(WELL_KNOWN, "registry", "current-registry-v1.json");
if (fs.existsSync(localRegistry)) {
  const mtimeBefore = fs.statSync(localRegistry).mtimeMs;
  withEnv(
    {
      NODE_ENV: "development",
      GDI_PUBLISHED_INVENTORY_ROOT: WELL_KNOWN,
      GDI_PUBLISHED_INVENTORY_REGISTRY_V1: "1",
      GDI_PUBLISHED_INVENTORY_HOTEL_IDS: SPICE_ISLAND_HOTEL_ID,
    },
    () => {
      const rows = mergePublishedInventoryIntoOpportunities(
        SPICE_ISLAND_HOTEL_ID,
        [],
        { strictPublishedOnly: true }
      );
      assertSpiceSeven(rows, "local-shared-mirror");
      localMirrorVerified = true;
    }
  );
  const mtimeAfter = fs.statSync(localRegistry).mtimeMs;
  assert.equal(mtimeBefore, mtimeAfter, "read path must not mutate shared mirror");
  assert.ok(
    !WELL_KNOWN.replace(/\\/g, "/").startsWith(ROOT.replace(/\\/g, "/")),
    "shared mirror must live outside the git worktree"
  );
}

// Cleanup CI fixture (best-effort)
try {
  fs.rmSync(fixtureRoot, { recursive: true, force: true });
} catch (_e) {
  /* ignore */
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "gdi-local-published-inventory-mirror-v1",
      spiceIds: EXPECTED,
      productionIsolated: true,
      ciFixtureVerified: true,
      localMirrorVerified,
      readOnlyVerified: true,
    },
    null,
    2
  )
);
