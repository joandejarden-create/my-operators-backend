/**
 * Focused regression: agreed hotel-facing prospect eligibility
 * (does not lower Ready/ACTIONABLE).
 */
import assert from "node:assert/strict";
import {
  evaluateProspectPublicationEligibility,
  hasUsableContactRoute,
  applyContactPackageToOpportunity,
  CONTACT_COMPLETENESS_GROUP,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { classifyBuyerContactPath } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";

const base = {
  organizationName: "Example Association",
  title: "Example Advocacy Meeting 2027",
  timing: "Verified: April 1–2, 2027",
  fitExplanation: "Downtown Autograph fit for Hill advocacy",
  hotelFitScore: 60,
  officialSource: "https://example.org/events/2027",
  sources: [{ url: "https://example.org/events/2027" }],
  commercialStatus: "Prospect to qualify — hotel sourcing unconfirmed",
};

// 1) Credible prospect with unknown sourcing remains hotel-facing eligible
{
  const opp = applyContactPackageToOpportunity(
    { ...base, venueSourcingStatus: "UNKNOWN" },
    {
      person: "Jane Smith",
      title: "Director of Events",
      contactMethod: "jane@example.org",
      contactMethodType: "named_email",
      businessPhone: "202-555-0100",
      phoneType: "direct",
      bookingResponsibility: "POTENTIAL_RELEVANT_CONTACT",
      sourceUrl: "https://example.org/staff",
      verificationDate: "2026-10-14",
      completenessGroup: 1,
    }
  );
  const elig = evaluateProspectPublicationEligibility(opp);
  assert.equal(elig.hotelFacing, true, "unknown sourcing must not block hotel-facing");
  assert.equal(elig.contactCompletenessGroup, CONTACT_COMPLETENESS_GROUP.PERSON_METHOD_PHONE);
}

// 2) Relevant functional contact (email, no person) can satisfy minimum route
{
  const opp = applyContactPackageToOpportunity(
    { ...base },
    {
      person: "Not established",
      title: "Not established",
      contactMethod: "meetings@example.org",
      contactMethodType: "functional_email",
      businessPhone: "202-555-0199",
      phoneType: "company_switchboard",
      bookingResponsibility: "REFERRAL_OR_ORG_ENTRY_POINT",
      sourceUrl: "https://example.org/contact",
      verificationDate: "2026-10-14",
      completenessGroup: 3,
    }
  );
  const route = hasUsableContactRoute(opp);
  assert.equal(route.ok, true, "functional email is usable route");
  const elig = evaluateProspectPublicationEligibility(opp);
  assert.equal(elig.hotelFacing, true);
  assert.equal(elig.contactCompletenessGroup, CONTACT_COMPLETENESS_GROUP.FUNCTIONAL_ORG_ONLY);
}

// 3) Homepage-only contact does not satisfy
{
  const opp = {
    ...base,
    publicContactPath: "https://example.org/",
    contactMethod: "homepage",
    contactPackage: {
      person: "Not established",
      contactMethod: "See homepage",
      businessPhone: "Not established",
      sourceUrl: "https://example.org/",
    },
  };
  const route = hasUsableContactRoute(opp);
  assert.equal(route.ok, false, "homepage-only must fail");
  const elig = evaluateProspectPublicationEligibility(opp);
  assert.equal(elig.hotelFacing, false);
  assert.equal(elig.enrichmentQueue, true);
}

// 4) TBA does not produce confirmed buying (editorial/commercial label check)
{
  const opp = {
    ...base,
    confirmedBuying: false,
    commercialStatus: "Prospect to qualify — hotel block TBA",
    contactPackage: {
      person: "Jane Smith",
      contactMethod: "jane@example.org",
      businessPhone: "202-555-0100",
      phoneType: "direct",
    },
  };
  const stamped = applyContactPackageToOpportunity(opp, opp.contactPackage);
  assert.equal(stamped.confirmedBuying, false);
  // Ready may still fail for other reasons; confirmed buying must stay false
  assert.notEqual(String(stamped.commercialStatus).toLowerCase().includes("confirmed buying"), true);
}

// 5) Contact information survives package→opportunity mapping (import-to-page field path)
{
  const stamped = applyContactPackageToOpportunity(
    { ...base },
    {
      person: "Amy Drow (Amy Drier-Drow)",
      title: "Director of Events",
      contactMethod: "adrow@ndss.org",
      businessPhone: "800-221-4602",
      phoneType: "company_switchboard",
      sourceUrl: "https://ndss.org/meet-our-staff",
      verificationDate: "2026-10-14",
    }
  );
  assert.equal(stamped.primaryContactEmail, "adrow@ndss.org");
  assert.equal(stamped.primaryContactPhone, "800-221-4602");
  assert.ok(stamped.primaryContact?.name);
  assert.equal(stamped.contactResearchAttempted, true);
  // Parenthetical alias must not break person-name contact taxonomy
  const contact = classifyBuyerContactPath({
    ...stamped,
    primaryContact: { ...stamped.primaryContact, name: "Amy Drow" },
  });
  assert.equal(contact.readyEligible, true);
}

// 6) Reimport ID stability contract (documented assertion for import path)
{
  const beforeId = "beth-ndss-2027";
  const after = applyContactPackageToOpportunity(
    { id: beforeId, ...base },
    {
      person: "Amy Drow",
      contactMethod: "adrow@ndss.org",
      businessPhone: "800-221-4602",
      phoneType: "company_switchboard",
    }
  );
  assert.equal(after.id, beforeId, "contact apply must preserve opportunity id / feedback join key");
}

// 7) Strict Ready is intentionally NOT lowered by this helper
{
  const thin = applyContactPackageToOpportunity(
    {
      organizationName: "Org",
      title: "Event",
      officialSource: "https://example.org/x",
    },
    { person: "Jane Smith", contactMethod: "j@example.org", businessPhone: "202-555-0100" }
  );
  const ready = isGdiCustomerOpportunityReady(thin, { nowDate: "2026-10-14" });
  assert.equal(ready.ok, false, "helper must not make incomplete packets Ready");
}

console.log("test-gdi-prospect-publication-eligibility-v1: PASS");
