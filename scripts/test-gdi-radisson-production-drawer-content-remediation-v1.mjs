#!/usr/bin/env node
/**
 * Permanent gates for Radisson truncation remediation + prose completeness.
 *   npm run test:gdi-radisson-production-drawer-content-remediation-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  appearsTruncatedCustomerProse,
  softClampCustomerProse,
  assertPackageEssentialProseCompleteness,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-prose-completeness-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { runPublicationQualityGate } from "../lib/group-demand-intelligence/publication-package/gdi-publication-quality-gate-v1.js";
import {
  evaluateCertifiedDrawerCommercialContent,
  summarizeCertifiedDrawerAudit,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recH9h3McgMNFQEla";
const PRIOR_CURRENT_FP =
  "533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d";
const CURRENT_FP =
  "f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d";
const TEMPO_STAGED_FP =
  "3f342937994ebaa36769e8c90af56e9cb26effeb940fa8a4de2be6cce4f995a8";
const NEW_STAGED_VERSION =
  "gdi_pub_recH9h3McgMNFQEla_20261116_drawer_truncation_remediation_v1";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("soft_clamp_prefers_sentence_boundary", () => {
  const long =
    "Officially announced activity dates: April 21–22, 2027, Enercare Centre. " +
    "These are event dates, not publication dates, and are within the requested horizon. " +
    "No official 2028 edition dates or venue were located; do not roll 2027 dates forward. " +
    "Extra trailing material that would previously be hard-sliced mid word abcdefghijklmnopqrstuvwxyz";
  const out = softClampCustomerProse(long, 260);
  assert.ok(!appearsTruncatedCustomerProse(out));
  assert.match(out, /\.\s*$/);
});

check("hard_slice_detector_flags_260_cut", () => {
  const cut =
    "Officially announced: DesignThinkers Toronto, October 21–22, 2027, at Meridian Hall and St. Lawrence Centre for the Arts, Toronto, Ontario, Canada. This is within the requested horizon. The 2026 conference ran October 20–21, with studio tours October 19, and i";
  assert.equal(appearsTruncatedCustomerProse(cut), true);
});

check("current_fingerprint_is_approved_f2bfebc9", () => {
  const store = createGdiPublicationPackageStore();
  const cur = store.loadCustomerVisiblePackage(HOTEL);
  assert.equal(cur.ok, true);
  assert.equal(cur.package.contentFingerprintSha256, CURRENT_FP);
  assert.equal(cur.package.lifecycleState, "CUSTOMER_PUBLISHED");
});

check("prior_533f2642_package_bytes_preserved", () => {
  const store = createGdiPublicationPackageStore();
  const prior = store.loadPackage(
    HOTEL,
    "gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1",
  );
  assert.equal(prior.ok, true);
  assert.equal(prior.package.contentFingerprintSha256, PRIOR_CURRENT_FP);
});

check("tempo_staged_package_still_present_immutable", () => {
  const store = createGdiPublicationPackageStore();
  const tempo = store.loadPackage(
    HOTEL,
    "gdi_pub_recH9h3McgMNFQEla_20261116_tempo_drawer_commercial_remediation_v1",
  );
  assert.equal(tempo.ok, true);
  assert.equal(tempo.package.contentFingerprintSha256, TEMPO_STAGED_FP);
});

check("new_staged_package_zero_truncations_66_certified", () => {
  const store = createGdiPublicationPackageStore();
  const staged = store.loadPackage(HOTEL, NEW_STAGED_VERSION);
  assert.equal(staged.ok, true);
  assert.equal((staged.package.opportunities || []).length, 66);
  const prose = assertPackageEssentialProseCompleteness(staged.package);
  assert.equal(prose.ok, true, JSON.stringify(prose.failing?.slice(0, 3)));
  const summary = summarizeCertifiedDrawerAudit(
    (staged.package.opportunities || []).map((o) =>
      evaluateCertifiedDrawerCommercialContent(o, { hotelId: HOTEL }),
    ),
  );
  assert.equal(summary.withTruncatedText, 0);
  assert.equal(summary.fullyPassing, 66);
  const gate = runPublicationQualityGate({
    opportunities: staged.package.opportunities,
    enforceCommercialExpirationGate: false,
    enforceCommercialActionabilityGate: false,
  });
  const truncBlocks = (gate.blocking || []).filter(
    (b) => b.code === "TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE",
  );
  assert.equal(truncBlocks.length, 0);
});

check("staged_pointer_targets_published_fingerprint", () => {
  const store = createGdiPublicationPackageStore();
  const ptr = store.readPointer(HOTEL, "STAGED");
  assert.equal(ptr.publicationVersion, NEW_STAGED_VERSION);
  assert.equal(ptr.contentFingerprintSha256, CURRENT_FP);
  assert.notEqual(ptr.contentFingerprintSha256, TEMPO_STAGED_FP);
  assert.notEqual(ptr.contentFingerprintSha256, PRIOR_CURRENT_FP);
});

check("ui_cache_bust_parity_marker", () => {
  const auth = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence-share.html"),
    "utf8",
  );
  assert.match(auth, /dealality-gdi-ui\.js\?v=gdi-svg-icon-color-restore-v1/);
  assert.match(share, /dealality-gdi-ui\.js\?v=gdi-svg-icon-color-restore-v1/);
});

check("research_scripts_no_hard_260_whyNow_slice", () => {
  for (const rel of [
    "scripts/gdi-radisson-blu-toronto-15usd-full-e2e-v1.mjs",
    "scripts/gdi-radisson-toronto-six-missing-segments-exa-30usd-v1.mjs",
    "scripts/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1.mjs",
  ]) {
    const src = fs.readFileSync(path.join(ROOT, rel), "utf8");
    assert.equal(
      /futureEdition(?:Text)?\)?\.slice\(0,\s*260\)/.test(src),
      false,
      rel,
    );
    assert.match(src, /softClampCustomerProse/);
  }
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-radisson-production-drawer-content-remediation-v1");
