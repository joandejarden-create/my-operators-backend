/**
 * Permanent Full E2E call-graph certification for WHO → ALT → HOW.
 *
 * Proves processOpportunityThroughOriginalChain wires the stages in order
 * (not merely that modules exist). $0 mocked / offline only.
 *
 *   node scripts/test-gdi-full-e2e-contact-architecture-call-graph-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { processOpportunityThroughOriginalChain } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { BUYER_ALTERNATIVE_PERSON_STAGE } from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";
import { assertBuyerAlternativePersonStage } from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";
import { assertWhoHowStagesInFullE2e } from "../lib/group-demand-intelligence/workflow/gdi-who-how-contact-intelligence-restore-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS } from "../lib/group-demand-intelligence/workflow/gdi-buyer-alternative-person-research-v1.js";
import { BUYER_WHO_REQUIRED_WORKFLOWS } from "../lib/group-demand-intelligence/workflow/gdi-buyer-who-permanent-full-e2e-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-full-e2e-contact-architecture-call-graph-v1",
);

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

function stageNames(stages) {
  return (stages || []).map((s) => s.stage).filter(Boolean);
}

function assertOrder(names, required) {
  let last = -1;
  for (const r of required) {
    const idx = names.indexOf(r);
    assert.ok(idx >= 0, `missing stage ${r} in ${JSON.stringify(names)}`);
    assert.ok(idx > last, `stage order broken: ${r} at ${idx} after ${last}`);
    last = idx;
  }
}

await checkAsync("1_call_graph_contact_subsequence_order", async () => {
  const result = await processOpportunityThroughOriginalChain(
    {
      id: "disc_call_graph_cert_1",
      hotelId: "rece0NdV9FCnaFlVS",
      hotelName: "Condesa DF",
      organizationName: "Acme Meetings Inc",
      title: "Annual Planner Summit 2027",
      researchSegment: "associations_events",
      namedStakeholders: [
        {
          name: "Hotel Booking Software",
          title: "Solutions",
        },
        {
          name: "Dana Meetings",
          title: "Director of Meetings",
          organization: "Acme Meetings Inc",
          sourceUrl: "https://acme-meetings.example/team",
        },
      ],
      evidence: [
        {
          url: "https://acme-meetings.example/team",
          statement:
            "Dana Meetings is Director of Meetings at Acme Meetings Inc and manages hotel sourcing.",
        },
      ],
    },
    {
      skipDeepResearch: true,
      authorizePaidResearch: false,
      authorizePaidAlternativePersonResearch: false,
      authorizePaidPublicContactSearch: false,
      ensureBuyerAlternativePersonResearch: true,
      ensureHowContactDiscovery: true,
      allowOfficialSourceContactFollowUp: false,
      howSkipDns: true,
      howFetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () =>
          "<html><body>Dana Meetings <a href='mailto:dana@acme-meetings.example'>email</a></body></html>",
        headers: { get: () => "text/html" },
      }),
    },
  );

  const names = stageNames(result.stages);
  assertOrder(names, [
    "CONTACT_RESEARCH",
    BUYER_ALTERNATIVE_PERSON_STAGE,
    "HOW_CONTACT_CHANNEL_DISCOVERY",
  ]);

  const altOk = assertBuyerAlternativePersonStage(result.stages, {
    requireExecuted: true,
  });
  assert.equal(altOk.ok, true, altOk.reason);

  const howOk = assertWhoHowStagesInFullE2e(result.stages, {
    requireBuyerWho: false,
    requireHowExecuted: true,
  });
  assert.equal(howOk.ok, true, howOk.reason);

  assert.equal(
    result.opportunity?.primaryContact?.name ||
      result.opportunity?.contactName ||
      null,
    "Dana Meetings",
  );

  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, "CALL_GRAPH_TRACE.json"),
    JSON.stringify(
      {
        at: new Date().toISOString(),
        stages: result.stages,
        stageNames: names,
        primary: result.opportunity?.primaryContact || null,
        secondary: result.opportunity?.secondaryContact || null,
        alt: result.opportunity?.chainBuyerAlternativePersonResearch || null,
        how: result.opportunity?.chainHowContactDiscovery || null,
      },
      null,
      2,
    ),
  );
});

await checkAsync("2_homepage_cannot_finalize_via_chain", async () => {
  const result = await processOpportunityThroughOriginalChain(
    {
      id: "disc_call_graph_org_only",
      hotelId: "rece0NdV9FCnaFlVS",
      organizationName: "Ghost Org",
      title: "Unknown program",
      namedStakeholders: [{ name: "Organization Contact Desk", title: "Info" }],
      primaryContact: {
        name: "Organization Contact Desk",
        officialContactUrl: "https://ghost.example/",
      },
    },
    {
      skipDeepResearch: true,
      authorizePaidResearch: false,
      allowOfficialSourceContactFollowUp: false,
      ensureHowContactDiscovery: true,
      howSkipDns: true,
      howFetchImpl: async () => ({
        ok: true,
        status: 200,
        text: async () => "<html></html>",
        headers: { get: () => "text/html" },
      }),
    },
  );
  const names = stageNames(result.stages);
  assertOrder(names, [
    "CONTACT_RESEARCH",
    BUYER_ALTERNATIVE_PERSON_STAGE,
    "HOW_CONTACT_CHANNEL_DISCOVERY",
  ]);
  const named =
    result.opportunity?.primaryContact?.name || result.opportunity?.contactName;
  assert.ok(
    !named ||
      /contact desk|organization|research required/i.test(String(named)),
    "org route must not become a verified named person",
  );
});

check("3_design_hotels_staged_counts_preserved", () => {
  const store = createGdiPublicationPackageStore();
  const expected = {
    rece0NdV9FCnaFlVS: 4,
    recaokE8psIbuVeuH: 3,
    rec1t35e3sPerzxYu: 11,
  };
  for (const [hotelId, count] of Object.entries(expected)) {
    const ptr = store.readPointer(hotelId, "STAGED");
    assert.ok(ptr?.publicationVersion, `STAGED missing ${hotelId}`);
    const load = store.loadPackage(hotelId, ptr.publicationVersion);
    assert.equal(
      (load.package.opportunities || []).length,
      count,
      `${hotelId} STAGED count`,
    );
    for (const o of load.package.opportunities || []) {
      assert.ok(
        o.primaryContact?.name,
        `${hotelId} ${o.id} missing primary name`,
      );
      assert.ok(
        !/contact research required|view research/i.test(
          JSON.stringify(o.primaryContact || {}),
        ),
      );
    }
  }
});

check("4_mode_workflow_lists_include_alt_and_who", () => {
  for (const w of BUYER_WHO_REQUIRED_WORKFLOWS) {
    assert.ok(typeof w === "string");
  }
  for (const w of ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS) {
    assert.ok(typeof w === "string");
  }
  assert.ok(ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS.includes("cohort_execute"));
});

check("5_missing_alt_stage_fails_assert", () => {
  const bad = assertBuyerAlternativePersonStage([
    { stage: "CONTACT_RESEARCH" },
    { stage: "HOW_CONTACT_CHANNEL_DISCOVERY", executed: true },
  ]);
  assert.equal(bad.ok, false);
});

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "CERTIFICATION.json"),
  JSON.stringify(
    {
      at: new Date().toISOString(),
      status: "CERTIFICATION_PASS",
      checksPassed: passed,
      requiredOrder: [
        "CONTACT_RESEARCH",
        BUYER_ALTERNATIVE_PERSON_STAGE,
        "HOW_CONTACT_CHANNEL_DISCOVERY",
      ],
      paidCalls: 0,
      stagedMutated: false,
    },
    null,
    2,
  ),
);

console.log(`\n${passed} checks evaluated\nCERTIFICATION_PASS`);
