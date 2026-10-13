/**
 * Smoke test: Mexico Explorer demo deep-research ownership reports.
 * node scripts/test-mexico-explorer-demo-deep-research.mjs
 */
import assert from "node:assert/strict";
import {
  buildMexicoExplorerDemoOwnershipReport,
  buildMexicoExplorerOwnershipGroupProfile,
  clearMexicoExplorerDemoCohortCache,
  SHERATON_GDL_AIRTABLE_ID,
  REAL_INN_CANCUN_AIRTABLE_ID,
} from "../lib/hotel-intelligence/ownership/golden-demo/mexico-explorer-demo-cohort.js";
import { buildHotelOwnershipReport } from "../lib/hotel-intelligence/ownership/golden-demo/gsf-cohort.js";

clearMexicoExplorerDemoCohortCache();

const sheraton = buildMexicoExplorerDemoOwnershipReport(SHERATON_GDL_AIRTABLE_ID);
assert.equal(sheraton.ok, true);
assert.equal(sheraton.intelligence_case, "A");
assert.ok(sheraton.deep_research);
assert.equal(sheraton.report.ownership_and_control.legal_property_owner_propco.name.includes("HNF"), true);
assert.equal(sheraton.report.ownership_and_control.operator.name, "Aimbridge LATAM");
assert.ok((sheraton.report.decision_authority.people || []).length >= 4);
assert.ok(
  (sheraton.report.decision_authority.people || []).some(
    (p) => p.professional_profile_verified && /julieta-fregoso/i.test(p.professional_profile_url || "")
  )
);
assert.ok(sheraton.report.corporate_contacts.headquarters);
assert.match(String(sheraton.report.corporate_contacts.website || ""), /marriott/i);
assert.ok(!(/aimbridgelatam\.com\/hotel\//i.test(String(sheraton.report.corporate_contacts.website || ""))));
assert.ok((sheraton.report.property_history || []).length >= 5);
assert.ok(
  (sheraton.report.decision_authority.people || []).some(
    (p) => /alex-fiz/i.test(p.professional_profile_url || p.linkedin_url || "")
  )
);

const org = buildMexicoExplorerOwnershipGroupProfile("inmobiliaria-hnf");
assert.equal(org.ok, true);
assert.ok(org.group);
assert.ok((org.portfolio || []).length >= 1);

const realInn = buildMexicoExplorerDemoOwnershipReport(REAL_INN_CANCUN_AIRTABLE_ID);
assert.equal(realInn.ok, true);
assert.equal(realInn.intelligence_case, "A");
assert.ok(realInn.deep_research);
assert.match(
  String(realInn.report.ownership_and_control.economic_owner_or_group.name || ""),
  /Alliance/i
);
assert.equal(realInn.report.ownership_and_control.legal_property_owner_propco.known, false);
assert.equal(realInn.report.ownership_and_control.operator.name, "Aimbridge LATAM");
assert.match(String(realInn.hotel.brand_display || ""), /voco/i);
assert.ok((realInn.report.decision_authority.people || []).some((p) => /rolftweeten/i.test(p.linkedin_url || p.professional_profile_url || "")));
assert.ok((realInn.report.decision_authority.people || []).some((p) => p.bio && p.bio.length > 40));

const viaRouter = buildHotelOwnershipReport(SHERATON_GDL_AIRTABLE_ID);
assert.equal(viaRouter.intelligence_case, "A");
assert.equal(viaRouter.deep_research_version, "sheraton-gdl-expo-deep-research-v1");

console.log("ok mexico-explorer-demo-deep-research");
console.log(
  JSON.stringify(
    {
      sheraton: {
        case: sheraton.intelligence_case,
        propco: sheraton.report.ownership_and_control.legal_property_owner_propco.name,
        people: sheraton.report.decision_authority.people.length,
        sources: sheraton.report.evidence.source_count,
      },
      real_inn: {
        case: realInn.intelligence_case,
        owner: realInn.report.ownership_and_control.economic_owner_or_group.name,
        brand: realInn.hotel.brand_display,
        people: realInn.report.decision_authority.people.length,
        linkedin_verified: realInn.report.decision_authority.people.filter((p) => p.professional_profile_verified)
          .length,
      },
    },
    null,
    2
  )
);
