#!/usr/bin/env node
/**
 * Local durability simulation — GDI_PUBLISHED_INVENTORY_REGISTRY_V1
 * Isolated temp root. No production writes. No Airtable. No tokens.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createGdiPublishedInventoryRegistry,
  mergePublishedInventoryIntoOpportunities,
  PUBLICATION_STATE,
  COMMERCIAL_PUBLICATION_CLASS,
} from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pub-reg-"));
const approval = {
  authorized: true,
  authorizedBy: "founder_simulation",
  authorizationPhrase: "SIMULATION_ONLY",
};

const HOTEL_A = "recHotelA_sim";
const HOTEL_B = "recHotelB_sim";

check("publish_approved_opportunity", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const { row } = reg.publishOpportunity({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_001",
    classification: COMMERCIAL_PUBLICATION_CLASS.SALES_READY,
    approval,
    sourceResearchReference: "sim/research/v1",
    snapshot: {
      id: "gdi_sim_001",
      title: "Sim Future Conference",
      whyThisHotel: "Selective lodging pathway",
    },
  });
  assert.equal(row.publicationState, PUBLICATION_STATE.PUBLISHED);
  assert.equal(row.publishedVersion, 1);
  assert.equal(row.customerFacing, true);
  assert.deepEqual(reg.getPublishedOpportunityIds(HOTEL_A), ["gdi_sim_001"]);
});

check("update_existing_published_opportunity_preserves_history", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  reg.updatePublishedOpportunity({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_001",
    approval,
    snapshot: {
      id: "gdi_sim_001",
      title: "Sim Future Conference — updated thesis",
      whyThisHotel: "Updated selective lodging pathway",
    },
  });
  const hist = reg.getHistory(HOTEL_A, "gdi_sim_001");
  assert.ok(hist.some((h) => h.type === "PUBLISH"));
  assert.ok(hist.some((h) => h.type === "UPDATE"));
  const row = reg.listPublished(HOTEL_A).find((r) => r.opportunityId === "gdi_sim_001");
  assert.equal(row.publishedVersion, 2);
  assert.ok(reg.loadSnapshot(HOTEL_A, "gdi_sim_001", 1));
  assert.ok(reg.loadSnapshot(HOTEL_A, "gdi_sim_001", 2));
});

check("revoke_publication_hides_from_customer_list", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  reg.publishOpportunity({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_002",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
    approval,
    snapshot: { id: "gdi_sim_002", title: "Sim DMC Account" },
  });
  reg.revokePublication({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_002",
    approval,
    reason: "simulation_revoke",
  });
  assert.equal(reg.getPublishedOpportunityIds(HOTEL_A).includes("gdi_sim_002"), false);
  const all = reg.listPublished(HOTEL_A, { includeRevoked: true });
  const revoked = all.find((r) => r.opportunityId === "gdi_sim_002");
  assert.equal(revoked.publicationState, PUBLICATION_STATE.REVOKED);
  assert.equal(revoked.customerFacing, false);
  const hist = reg.getHistory(HOTEL_A, "gdi_sim_002");
  assert.ok(hist.some((h) => h.type === "REVOKE"));
});

check("prevent_unauthorized_promotion_without_approval", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const blocked = reg.assertCannotPromoteFromResearchAlone({
    approval: { authorized: false },
  });
  assert.equal(blocked.blocked, true);
  assert.throws(
    () =>
      reg.publishOpportunity({
        hotelId: HOTEL_A,
        opportunityId: "gdi_sim_unauth",
        classification: COMMERCIAL_PUBLICATION_CLASS.SALES_READY,
        approval: { authorized: false },
        snapshot: { id: "gdi_sim_unauth" },
      }),
    /PUBLICATION_REQUIRES_EXPLICIT_APPROVAL/
  );
});

check("reject_non_customer_facing_classification", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  assert.throws(
    () =>
      reg.publishOpportunity({
        hotelId: HOTEL_A,
        opportunityId: "gdi_sim_watch",
        classification: "WATCH",
        approval,
        snapshot: { id: "gdi_sim_watch" },
      }),
    /UNAUTHORIZED_PUBLICATION_CLASS/
  );
});

check("survive_process_restart_reload_from_disk", () => {
  const reg1 = createGdiPublishedInventoryRegistry({ root: tmp });
  reg1.publishOpportunity({
    hotelId: HOTEL_B,
    opportunityId: "gdi_sim_hotel_b",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
    approval,
    snapshot: { id: "gdi_sim_hotel_b", title: "Hotel B AD" },
  });
  // New process simulation = new registry instance same root
  const reg2 = createGdiPublishedInventoryRegistry({ root: tmp });
  assert.deepEqual(reg2.getPublishedOpportunityIds(HOTEL_B), ["gdi_sim_hotel_b"]);
  assert.ok(reg2.loadRegistry().updatedAt);
});

check("survive_simulated_railway_redeploy_new_ephemeral_cwd", () => {
  // Redeploy simulation: code tree replaced, volume root unchanged
  const volumeRoot = path.join(tmp, "railway-volume-sim");
  const regA = createGdiPublishedInventoryRegistry({ root: volumeRoot });
  regA.publishOpportunity({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_volume",
    classification: COMMERCIAL_PUBLICATION_CLASS.SALES_READY,
    approval,
    snapshot: { id: "gdi_sim_volume", title: "Volume durable" },
  });
  const regB = createGdiPublishedInventoryRegistry({
    root: volumeRoot,
    env: { ...process.env, PWD: "/app-new-deploy" },
  });
  assert.deepEqual(regB.getPublishedOpportunityIds(HOTEL_A), ["gdi_sim_volume"]);
  assert.equal(regB.loadSnapshot(HOTEL_A, "gdi_sim_volume", 1)?.snapshot?.title, "Volume durable");
});

check("support_multiple_hotels", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const all = reg.listAllHotelsPublished();
  const hotels = new Set(all.map((r) => r.hotelId));
  assert.ok(hotels.has(HOTEL_A));
  assert.ok(hotels.has(HOTEL_B));
});

check("weekly_incremental_update_without_full_republish_of_others", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const before = reg.getPublishedOpportunityIds(HOTEL_A);
  reg.publishOpportunity({
    hotelId: HOTEL_A,
    opportunityId: "gdi_sim_weekly_new",
    classification: COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT,
    approval,
    sourceResearchReference: "weekly-refresh/2026-W45",
    snapshot: { id: "gdi_sim_weekly_new", title: "Weekly net-new AD" },
  });
  const after = reg.getPublishedOpportunityIds(HOTEL_A);
  assert.ok(after.includes("gdi_sim_weekly_new"));
  for (const id of before) assert.ok(after.includes(id));
});

check("prevent_leakage_of_unpublished_research_in_strict_merge", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const researchOnly = [
    { id: "gdi_sim_001", title: "published one" },
    { id: "gdi_research_secret", title: "UNPUBLISHED RESEARCH — must not leak" },
  ];
  const merged = mergePublishedInventoryIntoOpportunities(HOTEL_A, researchOnly, {
    registry: reg,
    strictPublishedOnly: true,
  });
  assert.equal(
    merged.some((o) => o.id === "gdi_research_secret"),
    false
  );
  assert.ok(merged.some((o) => o.id === "gdi_sim_001"));
  assert.ok(
    merged.every(
      (o) =>
        o.commercialInventoryClass === COMMERCIAL_PUBLICATION_CLASS.SALES_READY ||
        o.commercialInventoryClass === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT
    )
  );
});

check("account_development_is_first_class_customer_facing", () => {
  const reg = createGdiPublishedInventoryRegistry({ root: tmp });
  const row = reg
    .listPublished(HOTEL_A)
    .find((r) => r.opportunityId === "gdi_sim_weekly_new");
  assert.equal(
    row.commercialClassification,
    COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT
  );
  assert.equal(row.customerFacing, true);
});

const failed = results.filter((r) => !r.ok);
const out = {
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  tmpRoot: tmp,
  results,
};
console.log(JSON.stringify(out, null, 2));
process.exit(failed.length ? 1 : 0);
