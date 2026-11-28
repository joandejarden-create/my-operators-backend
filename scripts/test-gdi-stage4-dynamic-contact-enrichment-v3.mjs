#!/usr/bin/env node
/**
 * GDI Stage 4 + shared DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3 regression.
 * Mocked only — no live Findymail / Surfe spend.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
  computeMissingContactChannels,
  planMissingChannelEnrichment,
  classifyPartialChannelResult,
  CONTACT_PROVIDER_OUTCOMES,
} from "../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";
import {
  assessFindymailEmailEligibility,
  assessFindymailPhoneEligibility,
  enrichPersonWithFindymailMissingChannels,
  GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "dealality-dynamic-contact-enrichment-v3",
);
fs.mkdirSync(OUT, { recursive: true });

const results = [];
function check(id, fn) {
  try {
    fn();
    results.push({ id, ok: true });
  } catch (err) {
    results.push({
      id,
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

check("CONTACT_DYNAMIC_MISSING_CHANNEL_DETECTION", () => {
  const both = computeMissingContactChannels({ name: "A B" });
  assert.equal(both.needsEmail, true);
  assert.equal(both.needsPhone, true);
  assert.equal(both.requestChannel, "both");
  const phoneOnly = computeMissingContactChannels({
    name: "A B",
    email: "a@b.com",
  });
  assert.equal(phoneOnly.requestChannel, "phone");
  const none = computeMissingContactChannels({
    name: "A B",
    email: "a@b.com",
    phone: "+12674754267",
  });
  assert.equal(none.requestChannel, "none");
  assert.equal(none.dimmedComplete, true);
});

check("CONTACT_FINDYMAIL_MISSING_CHANNEL_ONLY", () => {
  const plan = planMissingChannelEnrichment({
    person: {
      name: "Jeff Mullen",
      organization: "Marriott",
      email: "jeff@example.com",
      linkedinUrl: "https://www.linkedin.com/in/jeffmullen",
    },
    provider: "findymail",
    remainingCredits: 40,
  });
  assert.equal(plan.version, DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3);
  assert.ok(
    plan.findymailActions.every((s) => s.action !== "FIND_EMAIL_FROM_NAME"),
  );
  assert.ok(
    plan.findymailActions.some((s) => s.action === "FIND_PHONE_FROM_LINKEDIN"),
  );
});

check("CONTACT_NO_LOOKUP_WHEN_COMPLETE", () => {
  const plan = planMissingChannelEnrichment({
    person: {
      name: "Katherine Button",
      email: "k@example.com",
      phone: "+12025550100",
    },
    provider: "findymail",
  });
  assert.ok(
    plan.findymailActions.every((s) =>
      String(s.action).startsWith("SKIP_"),
    ),
  );
});

check("CONTACT_SURFE_PHONE_ONLY_REQUEST", () => {
  const plan = planMissingChannelEnrichment({
    person: { name: "X Y", email: "x@y.com" },
    provider: "surfe",
  });
  const surfe = plan.steps.find((s) => s.provider === "surfe");
  assert.equal(surfe.autoExecute, false);
  assert.equal(surfe.surfeInclude.email, false);
  assert.equal(surfe.surfeInclude.mobile, true);
});

check("CONTACT_SURFE_EMAIL_ONLY_REQUEST", () => {
  const plan = planMissingChannelEnrichment({
    person: { name: "X Y", phone: "+12025550100" },
    provider: "surfe",
  });
  const surfe = plan.steps.find((s) => s.provider === "surfe");
  assert.equal(surfe.surfeInclude.email, true);
  assert.equal(surfe.surfeInclude.mobile, false);
});

check("CONTACT_SURFE_BOTH_CHANNEL_REQUEST", () => {
  const plan = planMissingChannelEnrichment({
    person: { name: "Meinrad Lang III" },
    provider: "surfe",
  });
  const surfe = plan.steps.find((s) => s.provider === "surfe");
  assert.equal(surfe.surfeInclude.email, true);
  assert.equal(surfe.surfeInclude.mobile, true);
  assert.equal(plan.surfeAutoForbidden, true);
});

check("CONTACT_PARTIAL_RESULT_HANDLING", () => {
  assert.equal(
    classifyPartialChannelResult({
      requestedEmail: true,
      requestedPhone: true,
      gotEmail: true,
      gotPhone: false,
    }),
    CONTACT_PROVIDER_OUTCOMES.PARTIAL_SUCCESS,
  );
  assert.equal(
    classifyPartialChannelResult({ pending: true }),
    CONTACT_PROVIDER_OUTCOMES.PENDING,
  );
});

check("CONTACT_CUMULATIVE_HARD_CAP", () => {
  const budget = { used: 14, hardCap: 15 };
  const plan = planMissingChannelEnrichment({
    person: {
      name: "Test Person",
      organization: "Acme",
      linkedinUrl: "https://www.linkedin.com/in/testperson",
    },
    provider: "findymail",
    remainingCredits: 15 - 14,
    findymailPhoneCreditCost: 10,
  });
  // Remaining 1 credit → phone (10) budget-skipped; email (1) may still plan.
  const phone = plan.findymailActions.find(
    (s) => s.channel === "phone" || String(s.action).includes("PHONE"),
  );
  assert.ok(phone);
  assert.equal(phone.action, "SKIP_BUDGET");
  assert.equal(budget.used, 14);
});

check("CONTACT_VERIFIED_LINKEDIN_IDENTITY", () => {
  const elig = assessFindymailPhoneEligibility({
    name: "Meinrad Lang III",
    organization: "Marriott",
    linkedinUrl: "https://www.linkedin.com/in/meinradlang",
  });
  assert.equal(elig.eligible, true);
  const noLi = assessFindymailPhoneEligibility({
    name: "Meinrad Lang III",
    organization: "Marriott",
  });
  assert.equal(noLi.eligible, false);
  assert.equal(noLi.reason, "LINKEDIN_REQUIRED_FOR_FINDYMAIL_PHONE");
});

check("GDI_STAGE4_SHARED_ENRICHMENT_INTEGRATED_SOURCE", () => {
  const stageSrc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js",
    ),
    "utf8",
  );
  assert.match(stageSrc, /enrichPersonWithFindymailMissingChannels/);
  assert.match(stageSrc, /allowFindymailPhone/);
  assert.match(stageSrc, /refuses automatic Surfe|authorizeSurfe/);
});

check("GDI_NO_AUTOMATIC_SURFE", () => {
  const ps1 = fs.readFileSync(
    path.join(ROOT, "scripts/gdi/Invoke-GdiContactResearch.ps1"),
    "utf8",
  );
  assert.match(ps1, /Surfe is never[\s\S]{0,40}auto-invoked/);
  assert.match(ps1, /DYNAMIC_CONTACT_ENRICHMENT_V3/);
  const surfeProv = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js",
    ),
    "utf8",
  );
  assert.match(surfeProv, /resolveEnrichmentChannelRequest/);
});

check("GDI_PHONE_COUNTRY_FORMAT_VALIDATED", () => {
  const phoneLib = fs.readFileSync(
    path.join(
      ROOT,
      "lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js",
    ),
    "utf8",
  );
  assert.match(phoneLib, /DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1/);
});

check("CONTACT_EMAIL_ALREADY_PRESENT_SKIP", () => {
  const elig = assessFindymailEmailEligibility({
    name: "Jeff Mullen",
    organization: "Marriott",
    email: "jeff@marriott.com",
  });
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, "EMAIL_ALREADY_PRESENT");
});

const dryRunPerson = {
  name: "Dry Run Person",
  organization: "Example Hotels",
  organizationDomain: "examplehotels.com",
  linkedinUrl: "https://www.linkedin.com/in/dryrunperson",
};
let dryRunRes;
try {
  dryRunRes = await enrichPersonWithFindymailMissingChannels(
    dryRunPerson,
    { hotelId: "recLuxvwwxID7U2B8", id: "opp_test" },
    {
      allowFindymail: true,
      dryRun: true,
      findymailCreditBudget: { used: 0, hardCap: 40 },
    },
  );
  assert.equal(dryRunRes.version, GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1);
  assert.equal(dryRunRes.contract, DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3);
  assert.equal(dryRunRes.surfeUsed, false);
  assert.equal(dryRunRes.surfePersisted, false);
  assert.ok(dryRunRes.plan);
  results.push({ id: "GDI_STAGE4_DRY_RUN_ENRICH", ok: true });
} catch (err) {
  results.push({
    id: "GDI_STAGE4_DRY_RUN_ENRICH",
    ok: false,
    error: String(err && err.message ? err.message : err),
  });
}

const failed = results.filter((r) => !r.ok);
const summary = {
  contract: DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  generatedAt: new Date().toISOString(),
  liveProviderSpend: 0,
  validationMode: "MOCKED",
};
fs.writeFileSync(
  path.join(OUT, "GDI_STAGE4_RESULTS.json"),
  JSON.stringify(summary, null, 2),
);
console.log(
  JSON.stringify(
    { ok: summary.ok, passed: summary.passed, failed: summary.failed },
    null,
    2,
  ),
);
if (!summary.ok) {
  console.error(failed);
  process.exit(1);
}
