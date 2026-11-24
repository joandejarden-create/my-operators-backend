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
  emailLocalPartMatchesPerson,
  isPersonLinkedInProfileUrl,
  PUBLIC_CONTACT_CHANNEL_KIND,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import { assessWhoHowCompletenessGates } from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
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

check("shared org inbox contacto@ is never DIRECT_WORK_EMAIL", () => {
  assert.equal(isRoleOrGenericMailbox("contacto@amegendoscopia.org.mx"), true);
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "contacto@amegendoscopia.org.mx",
    personName: "Mayra Membrillo Garduño",
    organizationName: "AMEG",
    contact: {
      channelSourceUrl:
        "https://amegendoscopia.org.mx/eventos/ecos-endoscopicos-internacionales",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_EMAIL_ROUTE);
  assert.equal(
    c.status,
    PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
  );
  assert.notEqual(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL);
});

check("person-specific email retains DIRECT_WORK_EMAIL", () => {
  assert.equal(
    emailLocalPartMatchesPerson(
      "llawrence@grenadaco-opbank.com",
      "Larry Lawrence",
    ),
    true,
  );
  const c = classifyPublicContactChannel({
    channel: "email",
    value: "llawrence@grenadaco-opbank.com",
    personName: "Larry Lawrence",
    organizationName: "Grenada Co-operative Bank",
    contact: {
      channelSourceUrl: "https://imrtgrenada.com/",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.DIRECT_WORK_EMAIL);
  assert.equal(c.status, PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_DIRECT);
});

check("organization switchboard phone is not verified personal direct", () => {
  const c = classifyPublicContactChannel({
    channel: "phone",
    value: "+1 473 440 2111",
    personName: "Larry Lawrence",
    organizationName: "Grenada Co-operative Bank",
    contact: {
      phoneType: "switchboard",
      channelSourceUrl: "https://imrtgrenada.com/",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
    },
  });
  assert.equal(c.kind, PUBLIC_CONTACT_CHANNEL_KIND.ORGANIZATION_PHONE_ROUTE);
  assert.equal(
    c.status,
    PUBLIC_CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATION_ROUTE,
  );
});

check("person LinkedIn /in/ accepted; company /posts/ rejected", () => {
  assert.equal(
    isPersonLinkedInProfileUrl("https://linkedin.com/in/giovanni-conca-684bab12"),
    true,
  );
  assert.equal(
    isPersonLinkedInProfileUrl(
      "https://www.linkedin.com/posts/afcona-additives_salesmeeting-teamspirit-lisbon-activity-7445077546477670401-DI27",
    ),
    false,
  );
});

check("missing personal email does not invalidate WHO; Sales Ready independent", () => {
  const gates = assessWhoHowCompletenessGates({
    organizationName: "Advantage Travel Partnership",
    primaryContact: {
      name: "Pelin Dede",
      role: "Head of Supplier Partnerships, Lodging",
      organization: "Advantage Travel Partnership",
      sourceUrl:
        "https://www.advantagetravelpartnership.com/new-partnership-with-shr",
    },
    secondaryContact: {
      name: "Sonia Michaels",
      role: "Head of Events",
      organization: "Advantage Travel Partnership",
    },
    howContactResearchAttempted: true,
    howContactResearchStatus: "HOW_ATTEMPTED_NO_CHANNEL",
  });
  assert.equal(gates.whoComplete, true);
  assert.equal(gates.whoSecondaryComplete, true);
  assert.equal(gates.channels.emailDirect, false);
  assert.equal(gates.salesReady, false);
});

check("org inbox route does not set emailDirect or auto Sales Ready", () => {
  const gates = assessWhoHowCompletenessGates({
    organizationName: "AMEG",
    primaryContact: {
      name: "Mayra Membrillo Garduño",
      email: "contacto@amegendoscopia.org.mx",
      organization: "AMEG",
      channelSourceUrl:
        "https://amegendoscopia.org.mx/eventos/ecos-endoscopicos-internacionales",
      contactDataOrigin: "PUBLIC_SOURCE",
      channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
    },
    howContactResearchAttempted: true,
  });
  assert.equal(gates.whoComplete, true);
  assert.equal(gates.verification.hasVerifiedOrgEmailRoute, true);
  assert.equal(gates.verification.hasVerifiedDirectEmail, false);
  assert.equal(gates.channels.emailDirect, false);
  assert.equal(gates.salesReady, false);
  assert.equal(gates.howResearchStatus, "HOW_PARTIAL_CHANNELS");
});

console.log(
  `\ntest-gdi-public-source-contact-discovery-permanent-v1: PASS (${passed})`,
);
