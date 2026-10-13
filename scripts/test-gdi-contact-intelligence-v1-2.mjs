/**
 * GDI Contact Intelligence V1.2 tests — gap classify, skip policy, soft-stop, wiring.
 */
import assert from "node:assert/strict";
import {
  CONTACT_GAP_REASON,
  CONTACT_RESEARCH_STATUS,
  shouldSkipDeepContactResearch,
  classifyContactGapReason,
  resolveContactGapV12,
} from "../lib/group-demand-intelligence/contact-intelligence-v1-2.js";
import { CONTACT_TIER } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { resolveOpportunityContact } from "../lib/group-demand-intelligence/weekly-contact-resolution-v1-2.js";

let passed = 0;
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
    passed += 1;
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    failed += 1;
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
    passed += 1;
  } catch (err) {
    console.error(`FAIL ${name}: ${err.message}`);
    failed += 1;
  }
}

check("disqualified_skipped", () => {
  const r = shouldSkipDeepContactResearch({
    id: "gdi_opp_foo_disqualified",
    priority: "DISQUALIFIED",
    title: "Already contracted elsewhere",
  });
  assert.equal(r.skip, true);
  assert.equal(r.reason, CONTACT_GAP_REASON.LOW_VALUE_NO_DEEP_RESEARCH);
});

check("marriott_account_watch_soft_stop", () => {
  const r = shouldSkipDeepContactResearch({
    id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
    title: "Corporate offsite / training demand near Marriott HQ corridor — needs account-level evidence",
    priority: "WATCHLIST",
    opportunityType: "FUTURE_CYCLE",
  });
  assert.equal(r.skip, false);
  assert.equal(r.softStop, true);
  assert.equal(r.reason, CONTACT_GAP_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED);
});

check("gap_reason_no_official_domain", () => {
  const reason = classifyContactGapReason({
    id: "x",
    title: "Some association conference",
    organizationName: "Example Association",
    opportunityType: "OVERFLOW_HOUSING",
    priority: "MEDIUM_PRIORITY",
  });
  assert.equal(reason, CONTACT_GAP_REASON.NO_OFFICIAL_DOMAIN);
});

check("gap_reason_only_generic", () => {
  const reason = classifyContactGapReason({
    id: "x",
    title: "Conference",
    organizationName: "NADO",
    officialSource: "https://www.nado.org/events/",
    primaryContactEmail: "info@nado.org",
    primaryContactName: "Meetings desk",
  });
  assert.equal(reason, CONTACT_GAP_REASON.ONLY_GENERIC_CONTACT);
});

await checkAsync("soft_stop_persists_unresolved_reason", async () => {
  const r = await resolveContactGapV12(
    {
      id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
      title: "Corporate offsite near Marriott HQ corridor — needs account-level evidence",
      organizationName: "Corporate meetings near Marriott HQ / Bethesda business district (category watch)",
      opportunityType: "FUTURE_CYCLE",
      priority: "WATCHLIST",
    },
    { jevShadow: false }
  );
  assert.equal(r.softStopped, true);
  assert.equal(r.afterTier, CONTACT_TIER.NO_CONTACT);
  assert.equal(r.opportunity.unresolvedContactReason, CONTACT_GAP_REASON.ACCOUNT_LEVEL_EVIDENCE_REQUIRED);
  assert.equal(r.opportunity.contactResearchStatus, CONTACT_RESEARCH_STATUS.STOPPED_LOW_VALUE);
  assert.match(r.opportunity.nextBestContactPath, /ACCOUNT_LEVEL/);
});

await checkAsync("disqualified_not_researched_in_weekly", async () => {
  const r = await resolveOpportunityContact({
    id: "gdi_opp_aan_2027_disqualified",
    title: "AAN — disqualified",
    priority: "DISQUALIFIED",
    opportunityType: "OVERFLOW_HOUSING",
  });
  assert.equal(r.skipped, true);
  assert.equal(r.fetchesUsed, 0);
  assert.equal(r.opportunity.contactResearchStatus, "SKIPPED");
});

await checkAsync("housing_page_reuse_upgrades_without_network", async () => {
  const html = `
    <html><body>
      <h1>Hotel Accommodations</h1>
      <p>Housing Coordinator: Jane Housing</p>
      <p>Email: housing@example.org</p>
      <a href="/contact">Contact us</a>
    </body></html>`;
  const r = await resolveContactGapV12(
    {
      id: "gdi_opp_test_housing",
      title: "Example Association Annual Meeting",
      organizationName: "Example Association",
      opportunityType: "OVERFLOW_HOUSING",
      priority: "MEDIUM_PRIORITY",
      officialSource: "https://example.org/housing",
    },
    {
      jevShadow: false,
      allowNetworkDomainResolution: false,
      fetchPage: async (url) => ({
        ok: true,
        status: 200,
        finalUrl: url,
        html,
        fetchesUsed: 1,
        url,
        sourceType: "HOUSING",
      }),
    }
  );
  assert.ok(
    tierRank(r.afterTier) >= tierRank(CONTACT_TIER.ORGANIZATION_PATH),
    `expected at least ORG_PATH, got ${r.afterTier}`
  );
  assert.ok(
    r.improved ||
      r.afterTier === CONTACT_TIER.FUNCTIONAL_CONTACT ||
      r.afterTier === CONTACT_TIER.NAMED_PARTIAL ||
      r.afterTier === CONTACT_TIER.NAMED_DIRECT ||
      r.afterTier === CONTACT_TIER.ORGANIZATION_PATH,
    "recovery should leave a durable public path"
  );
  assert.ok(r.opportunity.lastContactResearchAt);
  assert.ok(r.opportunity.contactResearchStatus);
});

await checkAsync("surfe_pii_stripped_on_gap_resolve", async () => {
  const r = await resolveContactGapV12(
    {
      id: "gdi_opp_test_surfe",
      title: "Example medical conference 2027",
      organizationName: "Example Medical Association",
      opportunityType: "OVERFLOW_HOUSING",
      priority: "MEDIUM_PRIORITY",
      officialSource: "https://example.org/contact",
      primaryContact: {
        name: "Pat Example",
        email: "pat@example.org",
        surfeEnriched: true,
        contactDataOrigin: "SURFE",
      },
      primaryContactEmail: "pat@example.org",
    },
    {
      jevShadow: false,
      allowNetworkDomainResolution: false,
      fetchPage: async (url) => ({
        ok: true,
        status: 200,
        finalUrl: url,
        html: "<html><body><p>Meetings Director Pat Example</p><p>meetings@example.org</p></body></html>",
        fetchesUsed: 1,
        url,
        sourceType: "OFFICIAL_CONTACT_PAGE",
      }),
    }
  );
  assert.equal(r.softStopped, false);
  assert.equal(r.skipped, false);
  const origin =
    r.opportunity.primaryContact?.contactDataOrigin ||
    r.opportunity.contactDataOrigin ||
    null;
  if (origin) {
    assert.notEqual(origin, "SURFE");
  }
  assert.ok(!r.opportunity.primaryContact?.surfeEnriched);
});

function tierRank(t) {
  return {
    [CONTACT_TIER.NO_CONTACT]: 0,
    [CONTACT_TIER.GENERIC_ONLY]: 1,
    [CONTACT_TIER.ORGANIZATION_PATH]: 2,
    [CONTACT_TIER.FUNCTIONAL_CONTACT]: 3,
    [CONTACT_TIER.NAMED_PARTIAL]: 4,
    [CONTACT_TIER.NAMED_DIRECT]: 5,
  }[t] ?? 0;
}

console.log(`\nAll GDI Contact Intelligence V1.2 checks: ${passed} pass, ${failed} fail`);
process.exit(failed ? 1 : 0);
