/**
 * Permanent public-source contact discovery — regression tests.
 *
 *   node scripts/test-gdi-public-source-contact-discovery-permanent-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PUBLIC_CONTACT_VERIFICATION_STATUS,
  classifyPublicContactChannel,
  assessPublicContactVerification,
  decideEnrichmentChannels,
  publicContactPersonKey,
  isRoleOrGenericMailbox,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import {
  runPublicContactDiscoveryWorkflow,
  clearPublicContactResearchCacheForTests,
  putCachedPublicContactResearch,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js";
import {
  recordPublicContactResearchEvent,
  summarizePublicContactResearchMetrics,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-research-metrics-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("person key requires first+last + organization", () => {
  assert.ok(
    publicContactPersonKey({
      name: "Maria Lopez",
      organization: "Lopez Events",
    }),
  );
  assert.equal(
    publicContactPersonKey({ name: "Maria", organization: "Lopez Events" }),
    null,
  );
  assert.notEqual(
    publicContactPersonKey({
      name: "Maria Lopez",
      organization: "Lopez Events",
    }),
    publicContactPersonKey({
      name: "Maria Lopez",
      organization: "Other Corp",
    }),
  );
});

check("guessed email without source is CANDIDATE_UNVERIFIED", () => {
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "maria.lopez@lopez-events.example",
    personName: "Maria Lopez",
    organizationName: "Lopez Events",
    contact: {},
    opportunity: {},
  });
  assert.equal(
    c.status,
    PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
  );
});

check("info@ mailbox is VERIFIED_ORGANIZATION_ROUTE with source", () => {
  assert.equal(isRoleOrGenericMailbox("info@lopez-events.example"), true);
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "info@lopez-events.example",
    personName: "Maria Lopez",
    organizationName: "Lopez Events",
    contact: {
      channelSourceUrl: "https://lopez-events.example/contact",
      contactDataOrigin: "PUBLIC_SOURCE",
    },
  });
  assert.equal(
    c.status,
    PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
  );
});

check("named email with source is VERIFIED_DIRECT", () => {
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "maria.lopez@lopez-events.example",
    personName: "Maria Lopez",
    organizationName: "Lopez Events",
    contact: {
      channelSourceUrl: "https://lopez-events.example/team/maria",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
    },
  });
  assert.equal(c.status, PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT);
});

check("provider origin never verified as public", () => {
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "secret@provider.example",
    personName: "Maria Lopez",
    organizationName: "Lopez Events",
    contact: {
      contactDataOrigin: "PROVIDER_EPHEMERAL",
      channelSourceUrl: "https://example.com",
    },
  });
  assert.equal(
    c.status,
    PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED,
  );
  assert.equal(c.value, null);
  assert.equal(c.providerOrigin, true);
});

check("enrichment decision offers Surfe only for missing channels", () => {
  const verified = assessPublicContactVerification({
    organizationName: "Lopez Events",
    primaryContact: {
      name: "Maria Lopez",
      email: "maria.lopez@lopez-events.example",
      phone: "+52 33 1234 5678",
      channelSourceUrl: "https://lopez-events.example/team/maria",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
      phoneExtensions: ["101"],
    },
  });
  const d = decideEnrichmentChannels(verified);
  assert.equal(d.workEmail.action, "USE_PUBLIC_VERIFIED");
  assert.equal(d.workEmail.surfeAvailable, false);
  assert.equal(d.telephone.surfeAvailable, false);
  assert.equal(d.telephone.fullEnrichPhoneAvailable, false);

  const missing = assessPublicContactVerification({
    organizationName: "Lopez Events",
    primaryContact: { name: "Maria Lopez" },
  });
  const d2 = decideEnrichmentChannels(missing);
  assert.equal(d2.workEmail.surfeAvailable, true);
  assert.equal(d2.workEmail.fullEnrichAvailable, false);
});

await (async () => {
  clearPublicContactResearchCacheForTests();
  const personKey = publicContactPersonKey({
    name: "Maria Lopez",
    organization: "Lopez Events",
  });
  putCachedPublicContactResearch(personKey, {
    hotelId: "hotel_a",
    opportunityId: "opp_a",
    found: {
      email: "maria.lopez@lopez-events.example",
      phone: "+52 33 1234 5678",
      sourceUrl: "https://lopez-events.example/team/maria",
    },
  });

  const result = await runPublicContactDiscoveryWorkflow({
    hotelId: "hotel_b",
    opportunity: {
      id: "opp_b",
      organizationName: "Lopez Events",
      primaryContact: { name: "Maria Lopez" },
    },
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
  });

  check("cross-hotel cache reuse avoids redundant research", () => {
    assert.equal(result.metrics.crossHotelReuse, true);
    assert.equal(result.metrics.surfeAttempted, false);
    assert.equal(result.metrics.fullEnrichAttempted, false);
    assert.equal(result.metrics.paidPublicSearchUsed, false);
    assert.equal(result.after.hasVerifiedDirectEmail, true);
  });

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pub-metrics-"));
  const env = { GDI_PUBLIC_CONTACT_METRICS_DIR: dir };
  recordPublicContactResearchEvent(result, env);
  const summary = summarizePublicContactResearchMetrics(env);
  check("metrics ledger is non-PII", () => {
    assert.equal(summary.privacy.includesEmails, false);
    assert.equal(summary.privacy.includesPhones, false);
    assert.ok(summary.totals.namedContactsResearched >= 1);
    assert.ok(summary.totals.crossHotelReuses >= 1);
    const raw = fs.readFileSync(
      path.join(dir, "gdi-public-contact-research-metrics-v1.json"),
      "utf8",
    );
    assert.doesNotMatch(raw, /maria\.lopez@/i);
    assert.doesNotMatch(raw, /\+52 33 1234/);
  });
})();

check("modules do not call Surfe or FullEnrich clients", () => {
  for (const rel of [
    "lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js",
    "lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js",
    "lib/group-demand-intelligence/presentation/gdi-public-contact-research-metrics-v1.js",
  ]) {
    const src = fs.readFileSync(path.join(ROOT, rel), "utf8");
    assert.doesNotMatch(src, /createSurfe|getSurfeCredits|fullenrich\.io|FULLENRICH_API_KEY/);
  }
});

console.log(
  `\ntest-gdi-public-source-contact-discovery-permanent-v1: PASS (${passed})`,
);
