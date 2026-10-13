/**
 * GDI Contact Completeness V1 — unit tests (no live network).
 */
import assert from "node:assert/strict";
import {
  CONTACT_GRADE,
  gradeContactCompleteness,
  classifyCommercialMotion,
  preferredRolesForOpportunity,
  selectContactResearchPopulation,
  summarizeContactBaseline,
  buildCustomerContactDrawerModel,
  splitWhoHow,
  mapGapToCeilingReason,
  computeNextContactResearchAt,
  PUBLIC_CONTACT_CEILING_REASON,
  COMMERCIAL_MOTION,
  attachCompletenessFields,
} from "../lib/group-demand-intelligence/contact-completeness-v1.js";
import {
  canSafeApplyJevDecision,
  JEV_CONTACT_SAFE_APPLY_TYPES,
  JEV_CONTACT_SHADOW_TYPES,
  CONTACT_JEV_DECISION,
} from "../lib/group-demand-intelligence/contact-jev-routing-v1.js";
import { GDI_CONTACT_ROLE } from "../lib/group-demand-intelligence/contact-candidate/ontology.js";
import { STOP_CONTACT_RESEARCH } from "../lib/group-demand-intelligence/contact-source-recovery-v1-1.js";

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

check("grade_named_with_email_is_A_or_B", () => {
  const g = gradeContactCompleteness({
    primaryContactName: "Jane Smith",
    primaryContactEmail: "jane@assoc.org",
    primaryContactRole: "Director of Meetings",
  });
  assert.ok(g.grade === CONTACT_GRADE.A || g.grade === CONTACT_GRADE.B);
  assert.ok(g.tier === "NAMED_DIRECT" || g.tier === "NAMED_PARTIAL");
});

check("grade_no_contact_is_E", () => {
  const g = gradeContactCompleteness({ title: "Blank opp" });
  assert.equal(g.grade, CONTACT_GRADE.E);
});

check("commercial_motion_overflow", () => {
  const m = classifyCommercialMotion({
    opportunityType: "OVERFLOW_HOUSING",
    title: "Conference overflow housing",
  });
  assert.equal(m, COMMERCIAL_MOTION.OVERFLOW);
});

check("role_ladder_housing_prefers_housing_owner", () => {
  const r = preferredRolesForOpportunity({
    opportunityType: "OVERFLOW_HOUSING",
    title: "Housing block",
  });
  assert.equal(r.preferredRoles[0], GDI_CONTACT_ROLE.HOUSING_OWNER);
});

check("research_population_skips_A_and_disqualified", () => {
  const pop = selectContactResearchPopulation([
    {
      id: "strong",
      primaryContactName: "Jane Smith",
      primaryContactEmail: "jane@assoc.org",
      primaryContactRole: "Meetings Director",
    },
    {
      id: "gdi_opp_foo_disqualified",
      priority: "DISQUALIFIED",
      title: "DQ",
    },
    {
      id: "weak",
      title: "Weak association conference",
      organizationName: "Example Assoc",
      opportunityType: "OVERFLOW_HOUSING",
      priority: "MEDIUM_PRIORITY",
    },
  ]);
  assert.equal(pop.length, 1);
  assert.equal(pop[0].id, "weak");
});

check("baseline_counts", () => {
  const s = summarizeContactBaseline([
    {
      primaryContactName: "Jane Smith",
      primaryContactEmail: "jane@x.org",
      primaryContactRole: "Director",
    },
    { officialSource: "https://example.org/events" },
    {},
  ]);
  assert.equal(s.TOTAL, 3);
  assert.ok(s.NAMED_DIRECT + s.NAMED_PARTIAL >= 1);
  assert.ok(s.ORGANIZATION_PATH + s.NO_CONTACT >= 1);
});

check("drawer_never_shows_NO_CONTACT_code", () => {
  const d = buildCustomerContactDrawerModel({
    title: "Future symposium",
    opportunityType: "FUTURE_CYCLE",
    organizationName: "NIH",
  });
  const customerText = [
    d.primaryContact,
    d.role,
    d.organization,
    d.whyRelevant,
    d.functionalPath,
    d.publicCeilingCopy,
    d.recommendedAction,
  ]
    .filter(Boolean)
    .join(" ");
  assert.ok(!/NO_CONTACT/i.test(customerText));
  assert.equal(
    d.publicCeilingCopy,
    "Named event contact not publicly identified yet"
  );
  assert.ok(d.recommendedAction);
});

check("who_how_separation", () => {
  const wh = splitWhoHow({
    primaryContactName: "Meg Novak",
    primaryContactRole: "Meetings Manager",
    primaryContactEmail: null,
  });
  assert.equal(wh.whoResolved, true);
  assert.equal(wh.howComplete, false);
  assert.equal(wh.whoResolvedHowMissing, true);
});

check("ceiling_future_cycle", () => {
  const r = mapGapToCeilingReason("EVENT_OWNER_NOT_OBSERVABLE", {
    opportunityType: "FUTURE_CYCLE",
    title: "2029 Annual Meeting destination TBD",
  });
  assert.equal(r, PUBLIC_CONTACT_CEILING_REASON.FUTURE_CYCLE_TOO_EARLY);
});

check("next_research_at_is_future", () => {
  const at = computeNextContactResearchAt(
    { opportunityType: "FUTURE_CYCLE" },
    PUBLIC_CONTACT_CEILING_REASON.FUTURE_CYCLE_TOO_EARLY
  );
  assert.ok(Date.parse(at) > Date.now());
});

check("jev_safe_apply_types_exclude_person", () => {
  assert.ok(
    JEV_CONTACT_SAFE_APPLY_TYPES.includes(CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH)
  );
  assert.ok(
    !JEV_CONTACT_SAFE_APPLY_TYPES.includes(
      CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING
    )
  );
  assert.ok(
    JEV_CONTACT_SHADOW_TYPES.includes(
      CONTACT_JEV_DECISION.NAMED_PERSON_WORTH_PURSUING
    )
  );
});

check("jev_cannot_safe_apply_stop_sufficient_when_continue", () => {
  const g = canSafeApplyJevDecision({
    decisionType: CONTACT_JEV_DECISION.STOP_CONTACT_RESEARCH,
    jevChoice: STOP_CONTACT_RESEARCH.STOP_SUFFICIENT_PATH,
    deterministicChoice: STOP_CONTACT_RESEARCH.CONTINUE,
    confidence: 0.9,
    domainState: { host: "example.org", confidence: "OFFICIAL_LIKELY" },
  });
  assert.equal(g.ok, false);
});

check("jev_safe_apply_source_path_ok", () => {
  const g = canSafeApplyJevDecision({
    decisionType: CONTACT_JEV_DECISION.CONTACT_SOURCE_PATH,
    jevChoice: "OFFICIAL_STAFF",
    deterministicChoice: "HOUSING",
    confidence: 0.8,
    domainState: { host: "example.org", confidence: "OFFICIAL_LIKELY" },
  });
  assert.equal(g.ok, true);
});

check("attach_completeness_named_person", () => {
  const next = attachCompletenessFields({
    id: "x",
    title: "Test",
    primaryContactName: "Pat Lee",
    primaryContactRole: "Conference Director",
    primaryContactEmail: "pat@assoc.org",
  });
  assert.ok(next.contactGrade === CONTACT_GRADE.A || next.contactGrade === CONTACT_GRADE.B);
  assert.ok(next.customerContactDrawer);
  assert.equal(next.customerContactDrawer.primaryContact, "Pat Lee");
  assert.equal(next.customerContactDrawer.surfeEligible, false);
});

check("regression_strong_contact_not_in_population", () => {
  const pop = selectContactResearchPopulation([
    {
      id: "sabrina",
      primaryContactName: "Sabrina Bracken",
      primaryContactEmail: "sabrina@example.org",
      primaryContactRole: "Director of Meetings",
      officialSource: "https://example.org",
    },
  ]);
  assert.equal(pop.length, 0);
});

check("webmaster_desk_rejected_as_named", async () => {
  const { isRejectedDeskPersona } = await import(
    "../lib/group-demand-intelligence/contact-desk-persona-gate.js"
  );
  const { hasNamedPerson } = await import(
    "../lib/group-demand-intelligence/contact-resolution.js"
  );
  assert.equal(
    isRejectedDeskPersona({
      name: "Frederick Webmaster",
      email: "ncifwebmaster@nih.gov",
    }).reject,
    true
  );
  assert.equal(hasNamedPerson({ name: "Frederick Webmaster" }), false);
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
