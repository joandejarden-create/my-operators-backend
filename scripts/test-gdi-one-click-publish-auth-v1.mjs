#!/usr/bin/env node
/**
 * Auth/surface gates for one-click publish:
 * - server route must require requireAdminAccess
 * - share routes must not mount publish
 * - verify-before-pointer leaves registry unchanged on failure
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  publishHotelInventoryOneClick,
} from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import {
  resolveApprovedPublicationOpportunities,
  assertApprovedEventTitlesPreserved,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  SPICE_ISLAND_HOTEL_ID,
} from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";

const ROOT = path.dirname(fileURLToPath(import.meta.url)) + "/..";
const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

check("server_route_requires_admin", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  const idx = server.indexOf("/hotels/:hotelId/publish");
  assert.ok(idx > 0, "publish route missing");
  const snippet = server.slice(idx, idx + 320);
  assert.match(snippet, /requireAdminAccess/);
  assert.match(snippet, /postGdiPublishHotelInventory/);
  assert.match(snippet, /memberstackAuth/);
});

check("share_routes_have_no_publish", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.equal(
    /group-demand-intelligence\/share\/[^"\n]*publish/.test(server),
    false,
    "GDI share paths must not expose publish",
  );
  // Explicit: only the authenticated admin hotel publish route may publish.
  const publishHits = [
    ...server.matchAll(
      /\/api\/group-demand-intelligence\/[^"\n]*publish/g,
    ),
  ].map((m) => m[0]);
  assert.deepEqual(publishHits, [
    "/api/group-demand-intelligence/hotels/:hotelId/publish",
  ]);
});

check("approved_pack_resolves_seven_without_local_doc", () => {
  const resolved = resolveApprovedPublicationOpportunities({
    hotelId: SPICE_ISLAND_HOTEL_ID,
    opportunities: [], // simulate production research doc missing IDs
    opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  });
  assert.equal(resolved.ok, true, JSON.stringify(resolved.missing));
  assert.equal(resolved.opportunities.length, 7);
  for (const id of SPICE_ISLAND_CERTIFIED_SEVEN_IDS) {
    assert.equal(resolved.sources[id], "deploy_bundled_enriched_v2_pack");
  }
  const titles = assertApprovedEventTitlesPreserved(resolved.opportunities);
  assert.equal(titles.ok, true, JSON.stringify(titles.fails));
});

check("verify_failure_does_not_advance_pointer", () => {
  const resolved = resolveApprovedPublicationOpportunities({
    hotelId: SPICE_ISLAND_HOTEL_ID,
    opportunities: [],
    opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-auth-verify-"));
  const registry = createGdiPublishedInventoryRegistry({ root: tmp });
  // Seed a prior pointer so we can prove it is preserved.
  const seeded = publishHotelInventoryOneClick({
    hotelId: SPICE_ISLAND_HOTEL_ID,
    opportunities: resolved.opportunities,
    opportunityIds: SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
    approval: {
      authorized: true,
      authorizedBy: "seed",
      authorizationPhrase: "SEED",
    },
    dryRun: false,
    registry,
    root: tmp,
  });
  assert.equal(seeded.ok, true);
  const priorId = seeded.pointer.publicationId;

  // Force verify failure by corrupting a candidate snapshot field after prepare
  // via a broken manifest hash expectation: mutate one opportunity title empty
  // after resolve so content gate or verify fails. Use registry batch directly
  // with verifyBeforeCommit returning false.
  const batchFail = (() => {
    try {
      return registry.publishHotelInventoryBatch({
        hotelId: SPICE_ISLAND_HOTEL_ID,
        approval: { authorized: true, authorizedBy: "test" },
        currentPublication: {
          publicationId: "should_not_promote",
          previousPublicationId: priorId,
        },
        items: [
          {
            opportunityId: SPICE_ISLAND_CERTIFIED_SEVEN_IDS[0],
            classification: "SALES_READY",
            snapshot: {
              ...resolved.opportunities[0],
              id: SPICE_ISLAND_CERTIFIED_SEVEN_IDS[0],
              whatIsHappening: resolved.opportunities[0].whatIsHappening,
              potentialHotelBusiness:
                resolved.opportunities[0].potentialHotelBusiness ||
                resolved.opportunities[0].commercialOpportunity ||
                "x",
              establishedFacts: resolved.opportunities[0].establishedFacts || ["a"],
              qualificationQuestions:
                resolved.opportunities[0].qualificationQuestions || ["q?"],
              primaryContact: resolved.opportunities[0].primaryContact,
            },
          },
        ],
        verifyBeforeCommit: () => ({ ok: false, reason: "forced_fail" }),
      });
    } catch (err) {
      return err;
    }
  })();
  assert.equal(batchFail.code, "PUBLICATION_VERIFY_FAILED");
  const reg = registry.loadRegistry();
  assert.equal(
    reg.hotels[SPICE_ISLAND_HOTEL_ID].currentPublication.publicationId,
    priorId,
    "pointer must remain prior after verify failure",
  );
});

const failed = results.filter((r) => !r.ok);
console.log(JSON.stringify({ ok: failed.length === 0, results }, null, 2));
process.exit(failed.length ? 1 : 0);
