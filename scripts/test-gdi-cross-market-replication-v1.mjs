/**
 * Cross-market replication V1 gates — IT locale, country-scoped portable seed,
 * market-local plan, ADP Jev shadow adapter, no Rome/Italy/W production switches.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  proposeHotelOnboardSeed,
  compareOnboardSeedProposals,
} from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import {
  selectPortableTemplatesForHotel,
  resolvePortableGeoScopes,
  textHasPilotGeoBleed,
  textHasNycReferenceBleed,
} from "../lib/group-demand-intelligence/research-coverage/portable-seed-templates.js";
import {
  buildMarketLocalDiscoveryPlan,
  proposeMarketLocalTargets,
  classifyCatchmentBand,
  CATCHMENT_BAND,
} from "../lib/group-demand-intelligence/research-coverage/market-local-expansion.js";
import { COUNTRY_LOCALE, LOCALE_PACKS } from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import {
  describeAdpJevShadowAdapter,
  ADP_JEV_FORBIDDEN,
} from "../lib/ai-demand-positioning/adp-jev-shadow.js";
import { describeAdpJevShadowIntegrationPoints } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import { resolveCanonicalHotelId } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { isHotelOnboardedForGdi } from "../lib/group-demand-intelligence/hotel-profile.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const W_ROME = "gdi_hotel_w_rome";
const RENAISSANCE = "recG66DQJKP2c0UNh";
const NOW = "2026-09-25T10:00:00.000Z";

let passed = 0;
let failed = 0;

function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`FAIL ${name}: ${e.message}`);
  }
}

check("it_locale_pack_present", () => {
  assert.ok(LOCALE_PACKS.it);
  assert.ok(LOCALE_PACKS.it.eventHints.includes("congresso"));
  assert.deepEqual(COUNTRY_LOCALE.IT.languages, ["it", "en"]);
});

check("italy_geo_scopes_exclude_us_only", () => {
  const scopes = resolvePortableGeoScopes("Italy");
  assert.ok(scopes.includes("EU"));
  assert.ok(scopes.includes("GLOBAL"));
  assert.ok(!scopes.includes("US") || scopes[0] !== "US");
  const { templates } = selectPortableTemplatesForHotel({
    capabilityProfile: { serviceLevel: "lifestyle_luxury_urban" },
    country: "Italy",
  });
  const domains = templates.map((t) => t.officialDomain);
  assert.ok(domains.includes("iccaworld.org"));
  assert.ok(domains.includes("ufi.org"));
  assert.ok(!domains.includes("asaecenter.org"));
  assert.ok(!domains.includes("hcea.org"));
});

check("us_hotel_still_gets_us_templates", () => {
  const { templates } = selectPortableTemplatesForHotel({
    capabilityProfile: { serviceLevel: "full_service_urban" },
    country: "US",
  });
  const domains = templates.map((t) => t.officialDomain);
  assert.ok(domains.includes("asaecenter.org"));
});

check("w_rome_config_onboarded_canonical", () => {
  assert.equal(isHotelOnboardedForGdi(W_ROME), true);
  assert.equal(resolveCanonicalHotelId(W_ROME), "rece0or38cxo3Fymb");
  const cfg = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json"),
      "utf8"
    )
  );
  assert.equal(cfg.redirectTo, "rece0or38cxo3Fymb");
});

check("generic_seed_w_rome_no_bleed", () => {
  const p = proposeHotelOnboardSeed(W_ROME, { now: NOW });
  assert.ok(p.fits.length >= 4, `expected fits, got ${p.fits.length}`);
  assert.ok(p.targets.length >= 4);
  const blob = JSON.stringify(p);
  assert.equal(textHasPilotGeoBleed(blob), false);
  assert.equal(textHasNycReferenceBleed(blob), false);
  assert.ok(!/asaecenter\.org/i.test(blob));
  assert.ok(/iccaworld\.org|ufi\.org|iapco\.org|mpi\.org|siteglobal/i.test(blob));
  for (const t of p.targets) {
    assert.notEqual(t.status, "NEW");
  }
});

check("w_rome_seed_deterministic", () => {
  const a = proposeHotelOnboardSeed(W_ROME, { now: NOW });
  const b = proposeHotelOnboardSeed(W_ROME, { now: NOW });
  const c = compareOnboardSeedProposals(a, b);
  assert.equal(c.deterministic, true);
  assert.equal(c.fitOverlapPct, 100);
  assert.equal(c.targetOverlapPct, 100);
});

check("renaissance_seed_unaffected_structurally", () => {
  const p = proposeHotelOnboardSeed(RENAISSANCE, { now: NOW });
  assert.ok(p.fits.length >= 6);
  assert.ok(p.geoScopes?.includes("US") || p.geoScopes?.includes("GLOBAL"));
});

check("market_local_plan_uses_it_en", () => {
  const plan = buildMarketLocalDiscoveryPlan(W_ROME, { maxTasks: 40 });
  assert.deepEqual(plan.languages, ["it", "en"]);
  assert.ok(plan.taskCount >= 10);
  assert.ok(plan.tasks.some((t) => t.language === "it"));
  assert.ok(!/if \(rome\)|if \(italy\)|w hotels only/i.test(JSON.stringify(plan)));
});

check("market_local_empty_evidence_yields_zero_targets", () => {
  const r = proposeMarketLocalTargets(W_ROME, [], { now: NOW });
  assert.equal(r.totals.targets, 0);
  assert.equal(r.quality.hotelSpecificOrgsHardcoded, false);
});

check("catchment_bands_generic", () => {
  assert.equal(classifyCatchmentBand(2, "URBAN_BUSINESS_MEETINGS").band, CATCHMENT_BAND.CORE);
  assert.equal(
    classifyCatchmentBand(100, "URBAN_BUSINESS_MEETINGS").band,
    CATCHMENT_BAND.OUT_OF_MARKET
  );
});

check("adp_jev_shadow_adapter_exists_apply_off", () => {
  const d = describeAdpJevShadowAdapter();
  assert.equal(d.shadowAdapterExists, true);
  assert.equal(d.productionAdpBehaviorChanged, false);
  assert.equal(d.apply, false);
  assert.ok(ADP_JEV_FORBIDDEN.includes("finding_truth"));
  const prep = describeAdpJevShadowIntegrationPoints();
  assert.equal(prep.shadowAdapterExists, true);
  assert.equal(prep.productionAdpBehaviorChanged, false);
});

check("no_w_rome_if_switches_in_lib", () => {
  const files = [
    "lib/group-demand-intelligence/research-coverage/portable-seed-templates.js",
    "lib/group-demand-intelligence/research-coverage/market-local-expansion.js",
    "lib/group-demand-intelligence/discovery-recall-v4.js",
    "lib/ai-demand-positioning/adp-jev-shadow.js",
  ];
  for (const f of files) {
    const src = fs.readFileSync(path.join(ROOT, f), "utf8");
    assert.ok(!/if\s*\(\s*['\"]w rome['\"]/i.test(src));
    assert.ok(!/hotelId\s*===\s*['\"]gdi_hotel_w_rome['\"]/.test(src));
    assert.ok(!/if\s*\(\s*country\s*===\s*['\"]Italy['\"]\s*&&\s*city/i.test(src));
  }
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
