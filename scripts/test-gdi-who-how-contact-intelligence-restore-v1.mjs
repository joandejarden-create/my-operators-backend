/**
 * Permanent certification: existing WHO + HOW restored in Full E2E (mocked).
 *
 *   node scripts/test-gdi-who-how-contact-intelligence-restore-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1,
  WHO_HOW_FUNCTION_INVENTORY,
  WHO_HOW_FULL_E2E_SEQUENCE,
  WHO_COMPLETENESS,
  HOW_COMPLETENESS,
  HOW_CONTACT_RESEARCH_STATUS,
  assessWhoHowCompletenessGates,
  runRestoredHowContactChannelDiscovery,
  assertWhoHowStagesInFullE2e,
  annotateWhoHowCompletenessFromEvidence,
} from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { evaluateNamedContactCoverageStandard } from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-who-how-contact-intelligence-restore-v1",
);

let passed = 0;
function check(name, fn) {
  const ret = fn();
  if (ret && typeof ret.then === "function") {
    throw new Error(`Use checkAsync for ${name}`);
  }
  passed += 1;
  console.log(`  PASS ${name}`);
}

async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

check("1_inventory_documents_who_and_how_functions", () => {
  assert.ok(WHO_HOW_FUNCTION_INVENTORY.who.length >= 5);
  assert.ok(WHO_HOW_FUNCTION_INVENTORY.how.length >= 5);
  const howBypass = WHO_HOW_FUNCTION_INVENTORY.how.find((h) =>
    /runPublicContactDiscoveryWorkflow/.test(h.fn),
  );
  assert.ok(howBypass?.priorBypass);
  assert.ok(
    WHO_HOW_FULL_E2E_SEQUENCE.indexOf("WHO_IDENTIFICATION") <
      WHO_HOW_FULL_E2E_SEQUENCE.indexOf("HOW_CONTACT_CHANNEL_DISCOVERY"),
  );
});

check("2_who_complete_without_email", () => {
  const g = assessWhoHowCompletenessGates({
    primaryContact: {
      name: "Lori Raleigh",
      role: "Executive Director",
      organization: "ISHC",
      sourceUrl: "https://www.ishc.com/team",
    },
    secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
  });
  assert.equal(g.who, WHO_COMPLETENESS.WHO_COMPLETE);
  assert.equal(g.whoComplete, true);
  assert.equal(g.howComplete, false);
  assert.equal(g.how, HOW_COMPLETENESS.HOW_INCOMPLETE);
  assert.equal(g.salesReady, false);
  assert.equal(g.channelFound, false);
});

check("3_email_not_required_for_who_gate", () => {
  const coverage = evaluateNamedContactCoverageStandard({
    organizationName: "Salesforce",
    primaryContact: {
      name: "Daniel Roy",
      role: "Sourcing Partner",
      organization: "Salesforce",
    },
  });
  assert.equal(coverage.finalizeEligible, true);
  assert.equal(coverage.namedCount, 1);
});

check("4_how_before_who_fails_stage_assert", () => {
  const bad = assertWhoHowStagesInFullE2e([
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
    { stage: "CONTACT_RESEARCH", investigated: 1 },
  ]);
  assert.equal(bad.ok, false);
  assert.equal(bad.code, "HOW_BEFORE_WHO");
});

check("5_how_stage_required", () => {
  const missing = assertWhoHowStagesInFullE2e([
    { stage: "BUYER_WHO_RESEARCH", executed: true },
    { stage: "CONTACT_RESEARCH", investigated: 1 },
  ]);
  assert.equal(missing.ok, false);
  assert.equal(missing.code, "HOW_STAGE_MISSING");

  const ok = assertWhoHowStagesInFullE2e([
    { stage: "BUYER_WHO_RESEARCH", executed: true },
    { stage: "CONTACT_RESEARCH", investigated: 1 },
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
  ]);
  assert.equal(ok.ok, true);
});

await checkAsync("6_how_skips_when_who_incomplete", async () => {
  const r = await runRestoredHowContactChannelDiscovery({
    organizationName: "Acme",
    primaryContact: { name: "Contact Research Required" },
  });
  assert.equal(r.skipped, true);
  assert.equal(r.reason, "WHO_INCOMPLETE");
  assert.equal(
    r.opportunity.howContactResearchStatus,
    HOW_CONTACT_RESEARCH_STATUS.SKIPPED_WHO_INCOMPLETE,
  );
});

await checkAsync("7_how_runs_public_first_mocked_channel", async () => {
  const html = `
    <html><body>
      <h1>Lori Raleigh</h1>
      <p>Executive Director</p>
      <a href="mailto:lori.raleigh@ishc.example">lori.raleigh@ishc.example</a>
      <a href="https://www.linkedin.com/in/lori-raleigh-example">LinkedIn</a>
    </body></html>
  `;
  const r = await runRestoredHowContactChannelDiscovery(
    {
      organizationName: "International Society of Hospitality Consultants",
      primaryContact: {
        name: "Lori Raleigh",
        role: "Executive Director",
        organization: "ISHC",
        sourceUrl: "https://www.ishc.example/team",
      },
      secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
      sources: [{ url: "https://www.ishc.example/team" }],
    },
    {
      authorizePaidPublicSearch: false,
      authorizeSurfe: false,
      authorizeFindymail: false,
      skipDns: true,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => html,
        headers: { get: () => "text/html" },
      }),
    },
  );
  assert.equal(r.executed, true);
  assert.equal(r.providers.findymail, false);
  assert.equal(r.providers.surfe, false);
  assert.equal(r.opportunity.howContactResearchAttempted, true);
  const gates = assessWhoHowCompletenessGates(r.opportunity);
  assert.equal(gates.whoComplete, true);
  // Channel may or may not extract depending on hierarchy; HOW must be attempted.
  assert.ok(
    gates.howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.CHANNELS_VERIFIED ||
      gates.howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.PARTIAL_CHANNELS ||
      gates.howResearchStatus === HOW_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_CHANNEL,
  );
  assert.equal(gates.howComplete, true);
});

await checkAsync("8_refuses_surfe_findymail_activation", async () => {
  await assert.rejects(
    () =>
      runRestoredHowContactChannelDiscovery(
        {
          primaryContact: {
            name: "Lori Raleigh",
            role: "Executive Director",
            organization: "ISHC",
          },
        },
        { authorizeSurfe: true },
      ),
    /Findymail\/Surfe/,
  );
});

check("9_design_hotels_18_who_how_audit_shape", () => {
  const store = createGdiPublicationPackageStore();
  const hotels = [
    ["rece0NdV9FCnaFlVS", "Condesa"],
    ["recaokE8psIbuVeuH", "LaLuna"],
    ["rec1t35e3sPerzxYu", "Memmo"],
  ];
  const summary = {
    total: 0,
    primaryNamed: 0,
    secondaryNamed: 0,
    publicEmail: 0,
    publicPhone: 0,
    linkedin: 0,
    noChannel: 0,
    salesReady: 0,
    whoComplete: 0,
    howComplete: 0,
    howIncomplete: 0,
  };
  for (const [hotelId] of hotels) {
    const ptr = store.readPointer(hotelId, "STAGED");
    assert.ok(ptr?.publicationVersion, `STAGED missing for ${hotelId}`);
    const load = store.loadPackage(hotelId, ptr.publicationVersion);
    for (const o of load.package.opportunities || []) {
      summary.total += 1;
      const annotated = annotateWhoHowCompletenessFromEvidence(o);
      const g = assessWhoHowCompletenessGates(annotated);
      if (g.whoComplete) summary.whoComplete += 1;
      if (g.whoSecondaryComplete) summary.secondaryNamed += 1;
      if (g.namedCount >= 1) summary.primaryNamed += 1;
      if (g.channels.email) summary.publicEmail += 1;
      if (g.channels.phone) summary.publicPhone += 1;
      if (g.channels.linkedin) summary.linkedin += 1;
      if (!g.channelFound) summary.noChannel += 1;
      if (g.salesReady) summary.salesReady += 1;
      if (g.howComplete) summary.howComplete += 1;
      else summary.howIncomplete += 1;
    }
  }
  assert.equal(summary.total, 18);
  assert.equal(summary.primaryNamed, 18);
  assert.equal(summary.secondaryNamed, 0);
  assert.equal(summary.salesReady, 0);
  // Prior recovery did not run HOW — WHO complete, HOW incomplete, no channels.
  assert.equal(summary.whoComplete, 18);
  assert.equal(summary.howComplete, 0);
  assert.equal(summary.howIncomplete, 18);
  assert.equal(summary.publicEmail, 0);
  assert.equal(summary.publicPhone, 0);
  assert.equal(summary.linkedin, 0);
  assert.equal(summary.noChannel, 18);
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "DESIGN_HOTELS_18_WHO_HOW_AUDIT.json"),
    JSON.stringify({ at: new Date().toISOString(), summary }, null, 2),
  );
});

check("10_version_constant", () => {
  assert.equal(
    GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1,
    "gdi-who-how-contact-intelligence-restore-v1",
  );
});

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "CERTIFICATION.json"),
  JSON.stringify(
    {
      at: new Date().toISOString(),
      version: GDI_WHO_HOW_CONTACT_INTELLIGENCE_RESTORE_V1,
      status: "CERTIFICATION_PASS",
      checksPassed: passed,
      sequence: WHO_HOW_FULL_E2E_SEQUENCE,
      inventory: WHO_HOW_FUNCTION_INVENTORY,
      providers: { findymail: false, surfe: false, exaSpent: 0 },
    },
    null,
    2,
  ),
);

console.log(`\n${passed} checks evaluated\nCERTIFICATION_PASS`);
