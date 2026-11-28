#!/usr/bin/env node
/**
 * Findymail Contact Intelligence V1.1 — zero-write fixture certification.
 * No Findymail API. No Airtable writes. No Surfe changes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1,
  LICENSE_RETENTION_CLASS,
  USAGE_OUTCOMES,
  resolveGdiStakeholderIdentity,
  evaluateChannelReuse,
  planEmailChannelUpsert,
  projectVerifiedEmailOntoOpportunity,
  buildUsageLedgerEvent,
  channelDedupeKey,
} from "../lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT =
  process.env.FINDYMAIL_V11_REPORT_DIR ||
  path.join(ROOT, "reports/gdi-findymail-contact-intelligence-v1-1");

const results = [];
function test(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
    console.error(`FAIL ${name}: ${err?.message || err}`);
  }
}

test("contract_version", () => {
  assert.equal(
    GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1,
    "gdi-findymail-channel-persistence-contract-v1-1",
  );
});

test("gdi_only_stakeholder_identity_without_hotel_ownership", () => {
  const id = resolveGdiStakeholderIdentity({
    name: "Ada Contoso",
    organization: "Caribbean Public Health Agency",
    domain: "carpha.org",
  });
  assert.equal(id.ok, true);
  assert.equal(id.hotelOwnershipRequired, false);
  assert.equal(id.hotelOrgRelationshipRequired, false);
  assert.equal(id.hiOrgContactCompatible, true);
  assert.ok(id.personIdentityKey);
  assert.ok(id.affiliationDedupeKey);
});

test("identity_rejects_name_only_without_org_or_domain", () => {
  const id = resolveGdiStakeholderIdentity({ name: "No Org Person" });
  assert.equal(id.ok, false);
  assert.equal(id.code, "MISSING_ORG_OR_DOMAIN");
});

test("hi_person_and_channel_reuse_zero_credits", () => {
  const identity = resolveGdiStakeholderIdentity({
    name: "Jordan Lee",
    organization: "Example Assoc",
    domain: "example.org",
  });
  const email = "jordan.lee@example.org";
  const dedupe = channelDedupeKey({
    subjectId: identity.affiliationDedupeKey,
    kind: "Email",
    value: email,
  });
  const plan = planEmailChannelUpsert(
    {
      name: "Jordan Lee",
      organization: "Example Assoc",
      domain: "example.org",
      hotelId: "hotel_a",
      opportunityId: "opp_1",
    },
    {
      channels: [
        {
          dedupeKey: dedupe,
          subjectId: identity.affiliationDedupeKey,
          channelKind: "Email",
          channelValue: email,
          verificationStatus: "Verified Direct",
          provider: "Findymail",
          observedAt: new Date().toISOString(),
          licenseRetentionClass:
            LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
          publicationState: "INTERNAL_ONLY",
        },
      ],
    },
    { authorizeProviderLookup: false },
  );
  assert.equal(plan.ok, true);
  assert.equal(plan.stage, "REUSE_EXISTING_CHANNEL");
  assert.equal(plan.usageEvent.creditsCharged, 0);
  assert.equal(plan.usageEvent.outcome, USAGE_OUTCOMES.REUSE_HIT);
  assert.equal(plan.salesReadyPromotion, false);
});

test("same_person_across_multiple_hotels_reuse", () => {
  const base = {
    name: "Sam Rivera",
    organization: "Travel Trade Co",
    domain: "traveltrade.example",
  };
  const identity = resolveGdiStakeholderIdentity(base);
  const email = "sam.rivera@traveltrade.example";
  const dedupe = channelDedupeKey({
    subjectId: identity.affiliationDedupeKey,
    value: email,
  });
  const store = {
    channels: [
      {
        dedupeKey: dedupe,
        subjectId: identity.affiliationDedupeKey,
        channelKind: "Email",
        channelValue: email,
        verificationStatus: "Verified Direct",
        provider: "Findymail",
        observedAt: new Date().toISOString(),
        licenseRetentionClass:
          LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
      },
    ],
  };
  const a = planEmailChannelUpsert(
    { ...base, hotelId: "hotel_1", opportunityId: "opp_a" },
    store,
  );
  const b = planEmailChannelUpsert(
    { ...base, hotelId: "hotel_2", opportunityId: "opp_b" },
    store,
  );
  assert.equal(a.usageEvent.creditsCharged, 0);
  assert.equal(b.usageEvent.creditsCharged, 0);
  assert.equal(a.identity.personIdentityKey, b.identity.personIdentityKey);
  assert.equal(a.ops[0].hotelId, "hotel_1");
  assert.equal(b.ops[0].hotelId, "hotel_2");
});

test("duplicate_lookup_suppression_window", () => {
  const plan = planEmailChannelUpsert(
    {
      name: "Casey Ng",
      organization: "Meetings Org",
      domain: "meetings.example",
      opportunityId: "opp_x",
    },
    {
      channels: [],
      usageEvents: [
        {
          personIdentityKeyHash: resolveGdiStakeholderIdentity({
            name: "Casey Ng",
            organization: "Meetings Org",
            domain: "meetings.example",
          }).personIdentityKey,
          provider: "findymail",
          at: new Date().toISOString(),
          outcome: USAGE_OUTCOMES.LOOKUP_MISS,
        },
      ],
    },
    { authorizeProviderLookup: true, provider: "findymail" },
  );
  assert.equal(plan.ok, false);
  assert.equal(plan.code, "LOOKUP_BLOCKED_DUPLICATE_WINDOW");
  assert.equal(plan.usageEvent.creditsCharged, 0);
});

test("stale_invalid_email_handling", () => {
  const stale = evaluateChannelReuse(
    {
      channelValue: "old@example.org",
      observedAt: "2024-01-01T00:00:00.000Z",
      verificationStatus: "Verified Direct",
      licenseRetentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
    },
    { freshnessDays: 90, now: "2026-11-23T00:00:00.000Z" },
  );
  assert.equal(stale.reuse, false);
  assert.equal(stale.reason, "STALE_NEEDS_REFRESH");

  const invalid = evaluateChannelReuse({
    channelValue: "info@example.org",
    observedAt: new Date().toISOString(),
    licenseRetentionClass:
      LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
  });
  assert.equal(invalid.reuse, false);
  assert.equal(invalid.reason, "MISSING_OR_GENERIC_EMAIL");
});

test("license_expiration_and_suppression", () => {
  const expired = evaluateChannelReuse({
    channelValue: "a@b.co",
    observedAt: new Date().toISOString(),
    licenseRetentionClass: LICENSE_RETENTION_CLASS.EXPIRED,
  });
  assert.equal(expired.reuse, false);
  assert.equal(expired.reason, "LICENSE_EXPIRED");

  const suppressed = evaluateChannelReuse({
    channelValue: "a@b.co",
    observedAt: new Date().toISOString(),
    licenseRetentionClass: LICENSE_RETENTION_CLASS.OPT_OUT,
  });
  assert.equal(suppressed.reuse, false);
});

test("customer_access_restrictions", () => {
  const gate = evaluateChannelReuse(
    {
      channelValue: "a@b.co",
      observedAt: new Date().toISOString(),
      licenseRetentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
      publicationState: "CUSTOMER_BLOCKED",
    },
    { requireCustomerAccess: true },
  );
  assert.equal(gate.reuse, false);
  assert.equal(gate.reason, "CUSTOMER_ACCESS_RESTRICTED");
});

test("provider_usage_accounting_no_pii", () => {
  const ev = buildUsageLedgerEvent({
    provider: "findymail",
    outcome: USAGE_OUTCOMES.LOOKUP_FOUND,
    creditsCharged: 1,
    personIdentityKeyHash: "abc123",
    hotelId: "hotel_z",
    opportunityId: "opp_z",
    email: "MUST_NOT_PERSIST@example.com",
    name: "MUST_NOT_PERSIST",
  });
  assert.ok(ev.lookupEventId);
  assert.equal(ev.creditsCharged, 1);
  assert.equal(ev.email, undefined);
  assert.equal(ev.name, undefined);
  const json = JSON.stringify(ev);
  assert.equal(/MUST_NOT_PERSIST/i.test(json), false);
});

test("authorized_lookup_prepares_channel_upsert_without_sales_ready", () => {
  const plan = planEmailChannelUpsert(
    {
      name: "Riley Chen",
      organization: "Incentive Co",
      domain: "incentive.example",
      hotelId: "hotel_m",
      opportunityId: "opp_m",
      title: "Program Director",
    },
    { channels: [], usageEvents: [] },
    {
      authorizeProviderLookup: true,
      providerResult: {
        outcome: "FOUND",
        email: "riley.chen@incentive.example",
        requestId: "fm_req_fixture_1",
        creditsCharged: 1,
      },
      licenseRetentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
    },
  );
  assert.equal(plan.ok, true);
  assert.equal(plan.stage, "PREPARE_CHANNEL_UPSERT");
  assert.equal(plan.salesReadyPromotion, false);
  const ops = plan.ops.map((o) => o.op);
  assert.ok(ops.includes("UPSERT_HI_ORGANIZATION"));
  assert.ok(ops.includes("UPSERT_HI_ORG_CONTACT"));
  assert.ok(ops.includes("UPSERT_HI_CONTACT_CHANNEL"));
  assert.ok(ops.includes("RECORD_USAGE_EVENT"));
  assert.equal(
    plan.ops.some((o) => o.op === "UPSERT_HI_HOTEL_ORG_RELATIONSHIP"),
    false,
  );
});

test("gdi_email_projection_preserves_named_person_without_email", () => {
  const projected = projectVerifiedEmailOntoOpportunity(
    {
      primaryContactName: "Morgan Blake",
      primaryContact: { name: "Morgan Blake", customerFacingNamed: true },
      tileContactName: "Morgan Blake",
      salesReady: false,
    },
    {},
  );
  assert.equal(projected.primaryContactName, "Morgan Blake");
  assert.equal(projected.tileContactName, "Morgan Blake");
  assert.equal(projected.salesReady, false);
  assert.match(projected.contactDisplayNote || "", /without email/i);
});

test("gdi_email_projection_sets_email_without_sales_ready_promotion", () => {
  const projected = projectVerifiedEmailOntoOpportunity(
    {
      primaryContactName: "Morgan Blake",
      primaryContact: { name: "Morgan Blake", customerFacingNamed: true },
      salesReady: false,
    },
    {
      channelValue: "morgan.blake@example.org",
      verificationStatus: "Verified Direct",
      provider: "Findymail",
      licenseRetentionClass:
        LICENSE_RETENTION_CLASS.PROVIDER_LICENSED_INTERNAL,
      publicationState: "CUSTOMER_BLOCKED",
    },
    { customerDisplayPermission: false },
  );
  assert.equal(projected.primaryContactEmail, "morgan.blake@example.org");
  assert.equal(projected.emailCustomerVisible, null);
  assert.equal(projected.salesReady, false);
  assert.equal(projected.salesReadyUnchanged, true);
});

test("surfe_session_policy_untouched_marker", () => {
  // Certification marker: this module never imports Surfe live gate / writers.
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js",
    ),
    "utf8",
  );
  assert.equal(/gdi-surfe-live-gate|from ["'].*surfe/i.test(src), false);
  assert.match(src, /Surfe remains session-only/);
});

test("live_schema_snapshot_present_readonly", () => {
  const snapPath = path.join(REPORT, "LIVE_SCHEMA_SNAPSHOT_READONLY.json");
  assert.ok(fs.existsSync(snapPath), "live schema snapshot missing");
  const snap = JSON.parse(fs.readFileSync(snapPath, "utf8"));
  assert.equal(snap.mode, "read_only_meta");
  const channels = snap.tables.find((t) => t.name === "HI Contact Channels");
  assert.ok(channels?.present);
  const names = channels.fields.map((f) => f.name);
  assert.ok(names.includes("Provider"));
  assert.ok(names.includes("Access / Publication Restriction"));
  assert.equal(names.includes("Last Checked At"), false);
  assert.equal(names.includes("License / Retention Class"), false);
});

const failed = results.filter((r) => !r.ok);
fs.mkdirSync(REPORT, { recursive: true });
fs.writeFileSync(
  path.join(REPORT, "ZERO_WRITE_CERTIFICATION.json"),
  JSON.stringify(
    {
      contract: GDI_FINDYMAIL_CHANNEL_PERSISTENCE_V1_1,
      at: new Date().toISOString(),
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      governance: {
        findymailApiCalls: 0,
        airtableWrites: 0,
        schemaMutations: 0,
        surfeChanges: 0,
      },
    },
    null,
    2,
  ),
);

if (failed.length) {
  console.error(`\n${failed.length} failing certification(s)`);
  process.exit(1);
}
console.log(`\nOK ${results.length} zero-write certifications`);
