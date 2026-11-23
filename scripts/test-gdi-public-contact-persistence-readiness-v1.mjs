/**
 * Persistence readiness gates — writes must stay disabled.
 *
 *   node scripts/test-gdi-public-contact-persistence-readiness-v1.mjs
 */
import assert from "node:assert/strict";
import {
  GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED,
  PERSISTENCE_BLOCK_REASONS,
  assessPublicContactPersistenceReadiness,
  persistPublicContactChannelsIfAuthorized,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js";
import { PUBLIC_CONTACT_VERIFICATION_STATUS } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("live write flag is hard-off", () => {
  assert.equal(GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED, false);
});

check("provider origin is never persistence-eligible", () => {
  const r = assessPublicContactPersistenceReadiness({
    opportunity: {
      id: "opp-1",
      organizationName: "Lopez Events",
      contactDataOrigin: "SURFE_PROVIDER_EPHEMERAL",
      primaryContact: {
        name: "Maria Lopez",
        role: "Director of Housing",
        email: "maria@lopez-events.example",
        sourceUrl: "https://lopez-events.example/team",
      },
    },
    founderAuthorizeLiveWrite: true,
  });
  assert.equal(r.execute, false);
  assert.ok(r.blocks.includes(PERSISTENCE_BLOCK_REASONS.PROVIDER_EPHEMERAL_ORIGIN));
  assert.ok(r.blocks.includes(PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED));
});

check("verified public contact still blocked while writes disabled", () => {
  const r = assessPublicContactPersistenceReadiness({
    opportunity: {
      id: "opp-2",
      organizationName: "Lopez Events",
      contactDataOrigin: "PUBLIC_SOURCE",
      primaryContact: {
        name: "Maria Lopez",
        role: "Director of Housing",
        title: "Director of Housing",
        email: "maria@lopez-events.example",
        sourceUrl: "https://lopez-events.example/team",
        channelSourceUrl: "https://lopez-events.example/team",
      },
    },
    verification: {
      hasVerifiedDirectEmail: true,
      hasVerifiedDirectPhone: false,
      hasVerifiedOrgEmailRoute: false,
      hasVerifiedOrgPhoneRoute: false,
      overallStatus: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
      email: {
        status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
        sourceUrl: "https://lopez-events.example/team",
        origin: "PUBLIC_SOURCE",
        channelKind: "DIRECT_WORK_EMAIL",
      },
      phone: { status: PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND },
    },
  });
  assert.equal(r.execute, false);
  assert.equal(r.gatePassed, false);
  assert.ok(r.blocks.includes(PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED));
  assert.equal(
    r.proposedDestination.tableStrategy,
    "EXISTING_GDO_AND_RESEARCH_INVENTORY_EMBEDDED_PRIMARY_CONTACT",
  );
  assert.equal(r.proposedDestination.createNewContactsTable, false);
  assert.equal(r.airtableSchemaChangesRequired, false);
});

check("persist hook no-ops without founder authorization", async () => {
  const out = await persistPublicContactChannelsIfAuthorized({
    opportunity: {
      id: "opp-3",
      organizationName: "Lopez Events",
      primaryContact: { name: "Maria Lopez", role: "Director" },
    },
  });
  assert.equal(out.ok, false);
  assert.equal(out.skipped, true);
  assert.ok(out.reason);
});

check("hypothetical readiness can exclude LIVE_WRITES_DISABLED only", () => {
  const r = assessPublicContactPersistenceReadiness({
    opportunity: {
      id: "opp-4",
      organizationName: "Lopez Events",
      contactDataOrigin: "PUBLIC_SOURCE",
      primaryContact: {
        name: "Maria Lopez",
        role: "Director of Housing",
        title: "Director of Housing",
        email: "maria@lopez-events.example",
        sourceUrl: "https://lopez-events.example/team",
        channelSourceUrl: "https://lopez-events.example/team",
      },
    },
    verification: {
      hasVerifiedDirectEmail: true,
      hasVerifiedDirectPhone: false,
      hasVerifiedOrgEmailRoute: false,
      hasVerifiedOrgPhoneRoute: false,
      overallStatus: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
      email: {
        status: PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT,
        sourceUrl: "https://lopez-events.example/team",
        origin: "PUBLIC_SOURCE",
        channelKind: "DIRECT_WORK_EMAIL",
      },
      phone: { status: PUBLIC_CONTACT_VERIFICATION_STATUS.NOT_FOUND },
    },
    excludeLiveWritesDisabledBlock: true,
  });
  assert.equal(r.execute, false);
  assert.equal(r.gatePassed, true);
  assert.equal(r.hypotheticalGatePassedExcludingLiveWrite, true);
  assert.ok(!r.blocks.includes(PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED));
});

console.log(`\n${passed} checks passed`);
