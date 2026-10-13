/**
 * GDI Weekly Discovery V1.2 — fixture / unit tests.
 */
import assert from "node:assert/strict";
import {
  CONTACT_TIER,
  classifyContactTier,
  stripSurfeProviderPii,
  shouldShowGetContactDetailsCta,
  summarizeContactCoverage,
} from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { buildCandidateFromTargetEvidence } from "../lib/group-demand-intelligence/weekly-lane-harvest-v1-2.js";
import { routeTargetToPlaybook, PLAYBOOK } from "../lib/group-demand-intelligence/weekly-discovery-orchestrator.js";
import { TARGET_TYPE } from "../lib/group-demand-intelligence/research-coverage/constants.js";
import { findCanonicalDuplicate } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

check("named_event_director_tier_named_partial", () => {
  const tier = classifyContactTier({
    primaryContact: {
      name: "Jane Smith",
      role: "Conference Director",
      email: null,
      phone: null,
    },
  });
  assert.equal(tier, CONTACT_TIER.NAMED_PARTIAL);
  assert.equal(shouldShowGetContactDetailsCta({
    primaryContact: { name: "Jane Smith", role: "Conference Director" },
    contactTier: tier,
  }), true);
});

check("functional_events_email_only", () => {
  const tier = classifyContactTier({
    primaryContact: {
      name: "Events Desk",
      email: "events@example.org",
      functionalEntity: true,
    },
  });
  assert.equal(tier, CONTACT_TIER.FUNCTIONAL_CONTACT);
});

check("generic_info_is_generic_only", () => {
  const tier = classifyContactTier({
    primaryContact: { email: "info@example.org" },
  });
  assert.equal(tier, CONTACT_TIER.GENERIC_ONLY);
});

check("official_source_without_person_is_organization_path", () => {
  const tier = classifyContactTier({
    officialSource: "https://example.org/housing",
    primaryContact: null,
  });
  assert.equal(tier, CONTACT_TIER.ORGANIZATION_PATH);
});

check("no_public_contact", () => {
  assert.equal(classifyContactTier({}), CONTACT_TIER.NO_CONTACT);
});

check("surfe_pii_stripped", () => {
  const stripped = stripSurfeProviderPii(
    {
      name: "Jane Smith",
      role: "Meetings Owner",
      email: "jane@secret.com",
      phone: "555-0100",
      linkedinUrl: "https://linkedin.com/in/jane",
      surfeEnriched: true,
      provider: "surfe",
    },
    { surfeUsed: true }
  );
  assert.equal(stripped.email, undefined);
  assert.equal(stripped.phone, undefined);
  assert.equal(stripped.linkedinUrl, undefined);
  assert.equal(stripped.name, "Jane Smith");
  assert.equal(stripped.reachabilityDeferred, true);
});

check("surfe_not_called_when_person_unresolved_cta_false", () => {
  assert.equal(
    shouldShowGetContactDetailsCta({
      primaryContact: { email: "events@example.org", functionalEntity: true },
    }),
    false
  );
});

check("contact_update_does_not_create_new_duplicate", () => {
  const existing = [{ id: "gdi_opp_x", title: "X Conference", organizationName: "Org X" }];
  const dup = findCanonicalDuplicate(
    { id: "gdi_opp_x", title: "X Conference", organizationName: "Org X" },
    existing
  );
  assert.ok(dup);
  assert.equal(dup.match.id, "gdi_opp_x");
});

check("target_routes_to_full_harvest_playbooks", () => {
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.DEMAND_GENERATOR }),
    PLAYBOOK.DEMAND_GENERATOR
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.EVENT_SERIES }),
    PLAYBOOK.EVENT_FUTURE_CYCLE
  );
  assert.equal(
    routeTargetToPlaybook({ targetType: TARGET_TYPE.PRIVATE_EVENT_VENUE }),
    PLAYBOOK.VENUE_PARTNERSHIP
  );
});

check("candidate_from_target_evidence_stable_id", () => {
  const c = buildCandidateFromTargetEvidence(
    {
      targetId: "gdirt_t1",
      canonicalName: "Regulatory Information Conference",
      primarySourceUrl: "https://www.nrc.gov/hotels",
      entityId: "nrc",
    },
    {
      text: "Hotel information for September 30, 2026 lodging accommodations",
      hasLodging: true,
      hasFuture: true,
      finalUrl: "https://www.nrc.gov/hotels",
    },
    "recLuxvwwxID7U2B8"
  );
  assert.match(c.id, /^gdi_opp_/);
  assert.ok(c.lodgingEvidence);
  assert.equal(c.hotelId, "recLuxvwwxID7U2B8");
});

check("coverage_summary_counts_tiers", () => {
  const s = summarizeContactCoverage([
    { primaryContact: { name: "Ada Lovelace", email: "ada@org.org", phone: "1" } },
    { officialSource: "https://x.org" },
    {},
  ]);
  assert.equal(s.total, 3);
  assert.ok(s.counts.NO_CONTACT >= 1);
  assert.ok(s.counts.ORGANIZATION_PATH >= 1);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Weekly Discovery V1.2 checks passed.");
