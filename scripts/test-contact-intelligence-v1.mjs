#!/usr/bin/env node
/**
 * Contact Intelligence V1 — unit + slice + publication + benchmark gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  CONTACT_INTELLIGENCE_VERSION,
  CONTACT_WRITE_GUARANTEES,
  ATTRIBUTION,
  CHANNEL_KIND,
  UNRESOLVED_REASON,
  classifyEmailAttribution,
  classifyPhoneAttribution,
  isGenericMailboxEmail,
  isRoleMailboxEmail,
  applyPublicationRules,
  publishContactPackage,
  createChannel,
  createHotelContactPackage,
  createContactStore,
  createContactIntelligenceService,
  CONTACT_SLICE_TEN_HOTELS,
  CONTACT_SLICE_TEN_VERSION,
  buildSliceTenSeedPackage,
  getBenchmark100Cohort,
  runContactBenchmark100,
  createDefaultBenchmarkResolver,
  isPaidEnrichmentEnabled,
} from "../lib/hotel-intelligence/contact-intelligence/index.js";

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ci-v1-test-"));
const store = createContactStore({ root: tmp });
const svc = createContactIntelligenceService({ store, forceNew: true });

console.log("Contact Intelligence V1 tests…");

assert.equal(CONTACT_INTELLIGENCE_VERSION, "contact-intelligence-v1");
assert.equal(CONTACT_WRITE_GUARANTEES.owner_name_auto_writes, 0);
assert.equal(CONTACT_WRITE_GUARANTEES.paid_enrichment_default, 0);
assert.equal(isPaidEnrichmentEnabled({}), false);
assert.equal(isPaidEnrichmentEnabled({ CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED: "false" }), false);

assert.equal(isGenericMailboxEmail("info@hotel.com"), true);
assert.equal(isRoleMailboxEmail("ir@gsfhotels.com"), true);
assert.equal(classifyEmailAttribution("info@hotel.com"), ATTRIBUTION.ORGANIZATION);
assert.equal(classifyEmailAttribution("ir@gsfhotels.com"), ATTRIBUTION.ROLE_MAILBOX);
assert.equal(
  classifyEmailAttribution("jose.ancira@gsfhotels.com", { contactName: "Jose Ancira" }),
  ATTRIBUTION.NAMED_PERSON
);
assert.equal(
  classifyEmailAttribution("mystery@gsfhotels.com", { claimedOfficial: true }),
  ATTRIBUTION.INFERRED
);
assert.equal(
  classifyPhoneAttribution("+52 33 0000", { lineType: "SWITCHBOARD", claimedDirectPersonal: true }),
  ATTRIBUTION.ORGANIZATION
);

{
  const inferred = createChannel({
    kind: CHANNEL_KIND.PERSON_EMAIL,
    value: "maybe@example.com",
    display_label: "Official email",
    force_inferred: true,
  });
  const pub = applyPublicationRules(inferred, { audience: "public" });
  assert.equal(pub.ok, false);
  assert.ok(
    pub.violations.includes("inferred_labeled_as_official") ||
      pub.violations.includes("inferred_not_public")
  );
}

{
  const switchboard = createChannel({
    kind: CHANNEL_KIND.SWITCHBOARD,
    value: "+52 33 0000",
    display_label: "Direct personal mobile",
    line_type: "SWITCHBOARD",
  });
  const pub = applyPublicationRules(switchboard, { audience: "public" });
  assert.equal(pub.ok, false);
  assert.ok(pub.violations.includes("switchboard_labeled_as_direct_personal"));
}

{
  const okCh = createChannel({
    kind: CHANNEL_KIND.HOTEL_PHONE,
    value: "+52 322 226 0700",
    display_label: "Hotel phone",
    attribution: ATTRIBUTION.PROPERTY,
    line_type: "MAIN",
    verified: true,
  });
  const pub = applyPublicationRules(okCh, { audience: "public" });
  assert.equal(pub.ok, true);
}

assert.equal(CONTACT_SLICE_TEN_HOTELS.length, 10);
assert.equal(CONTACT_SLICE_TEN_VERSION, "contact-slice-ten-v1");

const kgpv = svc.hotelGet({ hotel_id: "recUNycnMwOVFX0hc" });
assert.equal(kgpv.ok, true);
assert.ok(kgpv.package.hotel_contact.channels.length >= 1);
assert.ok(kgpv.package.organization_contact_route.channels.length >= 1);
assert.ok(kgpv.owner_anchor || kgpv.package.owner_entity_id);

const cam = svc.hotelGet({ hotel_id: "recIwaP1etgx2g9nA" });
assert.equal(cam.ok, true);
assert.ok(cam.package.unresolved_reasons.includes(UNRESOLVED_REASON.NO_PERSON_EVIDENCE));

const slice = svc.sliceTen();
assert.equal(slice.count, 10);
assert.ok(slice.hotels.every((h) => h.hotel_id));

const allianceA = svc.hotelGet({ hotel_id: "recTYaiA4S6fR6ixx" });
const allianceB = svc.hotelGet({ hotel_id: "recGZZCek9vDQGG1L" });
assert.equal(allianceA.package.owner_entity_id, allianceB.package.owner_entity_id);
assert.ok(allianceA.package.portfolio_reuse?.reused || allianceB.package.portfolio_reuse?.reused);

const refresh = svc.refreshHotel({
  hotel_id: "recUNycnMwOVFX0hc",
  paid: true,
});
assert.equal(refresh.ok, false);
assert.equal(refresh.error, "paid_enrichment_disabled");

const native = svc.refreshHotel({ hotel_id: "recUNycnMwOVFX0hc" });
assert.equal(native.ok, true);
assert.equal(native.job.paid, false);

const dup = svc.refreshHotel({ hotel_id: "recUNycnMwOVFX0hc" });
assert.equal(dup.deduped, true);

const published = publishContactPackage(
  createHotelContactPackage({
    hotel_id: "recX",
    channels: [
      createChannel({
        kind: CHANNEL_KIND.PERSON_EMAIL,
        value: "x@y.com",
        force_inferred: true,
        display_label: "Email",
      }),
    ],
  }),
  { audience: "public" }
);
assert.equal(published.channels.length, 0);
assert.ok(published.publication.rejected_count >= 1);

const cohort = getBenchmark100Cohort();
assert.equal(cohort.total, 100);
assert.equal(cohort.development_count, 60);
assert.equal(cohort.held_out_count, 40);
const devGroups = new Set(cohort.owner_groups.development);
for (const g of cohort.owner_groups.held_out) {
  assert.equal(devGroups.has(g), false, "held-out owner group must not appear in development");
}

const bench = runContactBenchmark100({
  service: svc,
  resolveHotel: createDefaultBenchmarkResolver(svc),
  includeHeldOutDetail: false,
});
assert.equal(bench.metrics.all.n, 100);
assert.equal(bench.metrics.held_out.n, 40);
assert.equal(bench.gates.population_wide_enrichment_allowed, false);
assert.equal(bench.gates.thousand_hotel_stage_allowed, false);
assert.ok(Array.isArray(bench.gates.notes));
console.log(
  "Benchmark summary:",
  JSON.stringify(
    {
      held_out_org_route: bench.metrics.held_out.coverage.org_route,
      held_out_precision: bench.metrics.held_out.precision.hotel_contact,
      portfolio_reuse: bench.metrics.held_out.portfolio_reuse_rate,
      cost_usd: bench.metrics.held_out.cost_usd,
      gates_passed: bench.gates.passed,
      thousand_allowed: bench.gates.thousand_hotel_stage_allowed,
    },
    null,
    2
  )
);

const seed = buildSliceTenSeedPackage("recUNycnMwOVFX0hc");
assert.ok(seed.hotel_name);

console.log("OK — Contact Intelligence V1");
