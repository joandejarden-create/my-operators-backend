/**
 * Bethesda historical research visibility — internal admin projection.
 *   node scripts/test-gdi-bethesda-historical-research-visibility-v1.mjs
 *   npm run test:gdi-bethesda-historical-research-visibility-v1
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BETHESDA_CANONICAL_HOTEL_ID } from "../lib/group-demand-intelligence/research/bethesda-full-demand-database-reconciliation-v1.js";
import {
  buildBethesdaHistoricalResearchVisibility,
  sanitizeProvenance,
} from "../lib/group-demand-intelligence/research/bethesda-historical-research-visibility-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const checks = [];

function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
  } catch (err) {
    checks.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const oppPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  BETHESDA_CANONICAL_HOTEL_ID,
  "opportunities.json"
);
const beforeHash = crypto
  .createHash("sha256")
  .update(fs.readFileSync(oppPath))
  .digest("hex");

const payload = buildBethesdaHistoricalResearchVisibility({ root: ROOT });

check("attestation_rendering_counts", () => {
  assert.equal(payload.counts.canonicalOpportunities, 59);
  assert.equal(payload.counts.historicalAttestations, 29);
  assert.equal(payload.counts.localCateringEvents, 10);
  assert.equal(payload.counts.potentialPartnerships, 8);
  assert.equal(payload.counts.otherGroupCorporateAssociation, 11);
  assert.ok(payload.counts.researchContinuationAvailable <= 17);
  assert.equal(payload.attestations.length, 29);
});

check("attestation_labels_and_dispositions", () => {
  for (const row of payload.attestations) {
    assert.equal(row.bodyStatus, "HISTORICAL_ATTESTATION_ONLY");
    assert.equal(row.customerFacing, false);
    assert.equal(row.salesReady, false);
    assert.equal(row.publicationApproved, false);
    assert.ok(
      [
        "ATTESTED_BODY_MISSING",
        "OVERFLOW_WATCH",
        "PARTNERSHIP_WATCH",
      ].includes(row.historicalDisposition),
      row.id
    );
  }
  const overflow = payload.attestations.filter(
    (r) => r.historicalDisposition === "OVERFLOW_WATCH"
  );
  const partnership = payload.attestations.filter(
    (r) => r.historicalDisposition === "PARTNERSHIP_WATCH"
  );
  assert.equal(overflow.length, 5);
  assert.equal(partnership.length, 8);
});

check("no_absolute_paths_or_secrets_in_browser_payload", () => {
  const blob = JSON.stringify(payload);
  assert.ok(!/[A-Za-z]:\\/.test(blob), "no windows absolute paths");
  assert.ok(!/crsr_|sk-|api[_-]?key|bearer /i.test(blob), "no secrets");
  const bad = sanitizeProvenance({
    sourceArtifactPath: "C:\\Secrets\\token.env",
    evidenceThatRecordExisted: ["source:C:\\Users\\x\\secret", "run:ok"],
  });
  assert.equal(bad.artifactLabel, null);
  assert.deepEqual(bad.evidenceLabels, ["run:ok"]);
});

check("canonical_opportunity_store_untouched", () => {
  const afterHash = crypto
    .createHash("sha256")
    .update(fs.readFileSync(oppPath))
    .digest("hex");
  assert.equal(afterHash, beforeHash);
  assert.equal(
    beforeHash,
    "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad"
  );
  assert.equal(payload.opportunityStore.sha256, beforeHash);
  assert.equal(payload.safety.opportunitiesJsonWrites, false);
  assert.equal(payload.safety.shareRegistryWrites, false);
  assert.equal(payload.safety.internalAdminOnly, true);
});

check("admin_route_and_ui_wired", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.match(
    server,
    /historical-research-recovery/
  );
  assert.match(server, /getAdminGdiHistoricalResearchRecovery/);
  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/ai-demand-admin.html"),
    "utf8"
  );
  assert.match(html, /graHistoricalRecovery/);
  assert.match(html, /Historical Research Recovery/);
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/admin-gdi-research-audit.js"),
    "utf8"
  );
  assert.match(js, /loadHistoricalRecovery/);
  assert.match(js, /historical-research-recovery/);
  assert.match(js, /HISTORICAL_ATTESTATION_ONLY/);
});

check("customer_and_share_surfaces_do_not_reference_recovery", () => {
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.ok(!/historical-research-recovery/.test(app));
  assert.ok(!/Historical Research Recovery/.test(app));
  assert.ok(!/historical-research-recovery/.test(share));
  assert.ok(!/Historical Research Recovery/.test(share));
  assert.ok(!/graHistoricalRecovery/.test(app));
});

check("api_handler_rejects_non_bethesda", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/admin-gdi-historical-research-recovery.js"),
    "utf8"
  );
  assert.match(api, /isBethesdaHistoricalVisibilityHotel/);
  assert.match(api, /historical_research_recovery_not_available/);
  assert.match(api, /adpMonthlyReviewAdminAuth|Never writes/i);
});

check("published_inventory_isolation_contract_still_present", () => {
  const pub = fs.readFileSync(
    path.join(ROOT, "scripts/test-gdi-local-published-inventory-mirror-v1.mjs"),
    "utf8"
  );
  assert.ok(pub.length > 100);
  const relocation = fs.readFileSync(
    path.join(ROOT, "scripts/test-gdi-research-audit-admin-relocation-v1.mjs"),
    "utf8"
  );
  assert.match(relocation, /customer_gdi_has_no_research_audit_tab/);
});

const failed = checks.filter((c) => !c.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      contract: "gdi-bethesda-historical-research-visibility-v1",
      beforeHash,
      counts: payload.counts,
      continuationAvailable: payload.researchContinuation.availableCount,
      checks,
    },
    null,
    2
  )
);
if (failed.length) process.exit(1);
