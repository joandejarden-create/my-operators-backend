/**
 * GDI Contact Intelligence V1.1 — fixture tests.
 */
import assert from "node:assert/strict";
import {
  DOMAIN_CONFIDENCE,
  ORG_FAMILY,
  CONTACT_SOURCE_PATH,
  CONTACT_FOLLOWUP_TYPE,
  STOP_CONTACT_RESEARCH,
  FUNCTIONAL_SUFFICIENCY,
  NAMED_PERSON_WORTH,
  resolveDomainFromOpportunity,
  classifyOrgFamily,
  computeGdiContactRouting,
  buildCandidateRecoveryUrls,
  recoverOfficialContactSources,
  contactPlaybookPaths,
  isAcceptableOfficialUrl,
} from "../lib/group-demand-intelligence/contact-source-recovery-v1-1.js";
import {
  evaluateContactJevShadow,
  CONTACT_JEV_DECISION,
} from "../lib/group-demand-intelligence/contact-jev-shadow-v1-1.js";
import {
  extractContactIntelligenceFromSource,
  CONTACT_DATA_ORIGIN,
} from "../lib/group-demand-intelligence/extract-contact-intelligence-from-source.js";
import {
  stripSurfeProviderPii,
  CONTACT_TIER,
  classifyContactTier,
} from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { isValidChoice, JEV_DECISION_TYPE } from "../lib/group-demand-intelligence/jev/jev-types.js";

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

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

check("official_domain_missing_routes_to_domain_resolution", () => {
  const opp = { id: "x", title: "UMD alumni weekend watch" };
  const domain = resolveDomainFromOpportunity(opp);
  assert.equal(domain.confidence, DOMAIN_CONFIDENCE.NO_OFFICIAL_DOMAIN);
  const route = computeGdiContactRouting(opp, domain);
  assert.equal(route.sourcePath, CONTACT_SOURCE_PATH.DOMAIN_RESOLUTION);
  assert.equal(route.followupType, CONTACT_FOLLOWUP_TYPE.SEARCH_OFFICIAL_DOMAIN);
  assert.equal(classifyOrgFamily(opp), ORG_FAMILY.UNIVERSITY);
});

check("functional_path_already_strong_stops", () => {
  const opp = {
    id: "y",
    title: "ACCP Annual Meeting",
    organizationName: "ACCP",
    officialSource: "https://accp1.org/travel",
    primaryContact: {
      name: "ACCP Travel Desk",
      email: "meetings@accp1.org",
      functionalEntity: true,
    },
    primaryContactEmail: "meetings@accp1.org",
    contactFunctionalEntity: true,
  };
  const domain = resolveDomainFromOpportunity(opp);
  assert.equal(domain.confidence, DOMAIN_CONFIDENCE.OFFICIAL_CONFIRMED);
  const route = computeGdiContactRouting(opp, domain);
  assert.equal(route.functionalPathSufficient, FUNCTIONAL_SUFFICIENCY.SUFFICIENT);
  assert.ok(
    route.stopContactResearch === STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH ||
      route.namedPersonWorthPursuing !== NAMED_PERSON_WORTH.YES
  );
});

check("playbook_paths_association_include_staff_housing", () => {
  const paths = contactPlaybookPaths(ORG_FAMILY.ASSOCIATION);
  assert.ok(paths.some((p) => /staff/i.test(p)));
  assert.ok(paths.some((p) => /housing/i.test(p)));
});

check("candidate_urls_built_from_domain", () => {
  const urls = buildCandidateRecoveryUrls({
    domainState: { host: "example.org", urls: ["https://example.org/event"], confidence: "OFFICIAL_CONFIRMED" },
    orgFamily: ORG_FAMILY.ASSOCIATION,
    sourcePath: CONTACT_SOURCE_PATH.OFFICIAL_STAFF,
    maxUrls: 5,
  });
  assert.ok(urls.includes("https://example.org/event"));
  assert.ok(urls.some((u) => /staff/i.test(u)));
});

await checkAsync("named_owner_from_staff_page_via_recovery", async () => {
  const STAFF_HTML = `<html><body>
    <h1>Conference Staff</h1>
    <p>Jane Smith, Conference Director <a href="mailto:jane.smith@assoc.org">jane.smith@assoc.org</a></p>
  </body></html>`;
  const opp = {
    id: "z",
    title: "Annual Association Meeting",
    organizationName: "Assoc Org",
    officialSource: "https://assoc.org/event",
  };
  const result = await recoverOfficialContactSources(opp, {
    allowNetworkDomainResolution: false,
    budget: { maxAdditionalFetches: 2 },
    fetchPage: async (url) => ({
      ok: true,
      status: 200,
      finalUrl: url,
      html: STAFF_HTML,
      fetchesUsed: 1,
      sourceType: "OFFICIAL_STAFF",
    }),
  });
  assert.ok(result.improved || result.afterTier === CONTACT_TIER.NAMED_PARTIAL || result.afterTier === CONTACT_TIER.NAMED_DIRECT);
  assert.ok(/Jane Smith/i.test(result.opportunity.primaryContactName || result.opportunity.primaryContact?.name || ""));
});

check("speaker_rejected_as_primary", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://assoc.org/speakers" },
    pageContent: `<html><body><p>Speaker: John Keynote, Keynote Speaker</p></body></html>`,
  });
  assert.ok(!ex.people.some((p) => /Keynote/i.test(p.personName)));
});

check("random_executive_rejected", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://venue.org/about" },
    pageContent: `<html><body><p>CEO: Bob Boss, Chief Executive Officer</p>
      <p>Email <a href="mailto:events@venue.org">events@venue.org</a></p></body></html>`,
  });
  assert.ok(!ex.people.some((p) => /Bob Boss/i.test(p.personName)));
  assert.ok(ex.functionalContacts.some((f) => /events@venue.org/i.test(f.email || "")));
});

check("housing_coordinator_accepted", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://event.org/housing" },
    pageContent: `<html><body><p>Mary Smith, Housing Coordinator — <a href="mailto:housing@event.org">housing@event.org</a></p></body></html>`,
  });
  assert.ok(ex.people.some((p) => /Mary Smith/i.test(p.personName)));
});

check("jev_contact_types_registered", () => {
  assert.ok(JEV_DECISION_TYPE.CONTACT_SOURCE_PATH);
  assert.ok(isValidChoice("CONTACT_SOURCE_PATH", "OFFICIAL_STAFF"));
  assert.ok(isValidChoice("STOP_CONTACT_RESEARCH", "STOP_SUFFICIENT_PATH"));
  assert.ok(isValidChoice("NAMED_PERSON_WORTH_PURSUING", "NO"));
});

await checkAsync("jev_unavailable_falls_back_to_gdi_route", async () => {
  const opp = {
    id: "j1",
    title: "NRC RIC",
    organizationName: "NRC",
    officialSource: "https://www.nrc.gov/conferences",
    primaryContactEmail: "housing@nrc.gov",
    primaryContact: { email: "housing@nrc.gov", functionalEntity: true },
    contactFunctionalEntity: true,
  };
  const domain = resolveDomainFromOpportunity(opp);
  const gdiRoute = computeGdiContactRouting(opp, domain);
  const shadow = await evaluateContactJevShadow({
    opportunity: opp,
    domainState: domain,
    gdiRoute,
    forceShadow: true,
  });
  assert.equal(shadow.shadow, true);
  assert.equal(shadow.calls, 5);
  assert.equal(shadow.productionRoute.sourcePath, gdiRoute.sourcePath);
  // Without API key, technical fallbacks expected; production unchanged
  assert.ok(shadow.techFallbacks >= 0);
});

await checkAsync("jev_says_named_but_functional_sufficient_gdi_stops", async () => {
  const opp = {
    id: "j2",
    title: "Venue partnership",
    organizationName: "Woman's Club",
    officialSource: "https://womansclub.org/events",
    primaryContact: { name: "Events Team", email: "events@womansclub.org", functionalEntity: true },
    primaryContactEmail: "events@womansclub.org",
    contactFunctionalEntity: true,
  };
  const domain = resolveDomainFromOpportunity(opp);
  const route = computeGdiContactRouting(opp, domain);
  assert.equal(route.functionalPathSufficient, FUNCTIONAL_SUFFICIENCY.SUFFICIENT);
  // Even if Jev would say YES for named, production stop remains GDI
  const shadow = await evaluateContactJevShadow({ opportunity: opp, domainState: domain, gdiRoute: route });
  assert.equal(shadow.productionRoute.stopContactResearch, route.stopContactResearch);
});

check("public_email_origin_public_source", () => {
  const ex = extractContactIntelligenceFromSource({
    source: { url: "https://org.org/staff" },
    pageContent: `<html><body><p>Ada Planner, Director of Meetings <a href="mailto:ada@org.org">ada@org.org</a></p></body></html>`,
  });
  assert.ok(ex.people.length);
  assert.equal(ex.people[0].contactDataOrigin, CONTACT_DATA_ORIGIN.PUBLIC_SOURCE);
});

check("surfe_email_does_not_persist", () => {
  const stripped = stripSurfeProviderPii(
    { name: "Ada Planner", email: "secret@x.com", phone: "1", surfeEnriched: true, provider: "surfe" },
    { surfeUsed: true }
  );
  assert.equal(stripped.email, undefined);
  assert.equal(stripped.name, "Ada Planner");
});

check("hotel_brand_bleed_rejected_for_adjacent_hq_watch", () => {
  assert.equal(
    isAcceptableOfficialUrl(
      "https://www.marriott.com/about/privacy.mi",
      { title: "Corporate offsite near Marriott HQ corridor — watch" }
    ),
    false
  );
  assert.equal(
    isAcceptableOfficialUrl(
      "https://www.marriottvacationsworldwide.com/contact-us/",
      { title: "Corporate offsite near Marriott HQ corridor — watch" }
    ),
    false
  );
  assert.equal(
    isAcceptableOfficialUrl("https://alumni.umd.edu/", {
      title: "University of Maryland alumni weekend watch",
    }),
    true
  );
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI Contact Intelligence V1.1 checks passed.");
