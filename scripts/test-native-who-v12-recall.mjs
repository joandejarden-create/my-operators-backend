#!/usr/bin/env node
/**
 * Offline regression tests for WHO Recall V12 strategies.
 * Pattern-based — no hotel/person/event hardcodes as gold answers.
 */
import assert from "node:assert/strict";
import {
  classifyWhoRecallGapV12,
  buildGapDirectedQueriesV12,
  domainsFromFunctionalContacts,
  confirmWhoRecallCandidateV12,
  recallStagesForGapV12,
  WHO_RECALL_GAP_V12,
  RECALL_STAGE_V12,
} from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/who-recall-v12.js";
import { personTypeGateV11 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/person-boundary-v11.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("functional-only gap classification", () => {
  const g = classifyWhoRecallGapV12({
    currentNoWhoReason: "FUNCTIONAL_ONLY",
    functionalContacts: [{ email: "events@example.org" }],
  });
  assert.equal(g.primaryGap, WHO_RECALL_GAP_V12.FUNCTIONAL_ONLY);
});

test("alias-domain failure when prior confirmed people exist", () => {
  const g = classifyWhoRecallGapV12({
    priorV9: { researchState: "NAMED_PERSON_CONFIRMED", people: ["Alex Example"] },
    functionalContacts: [],
    currentNoWhoReason: "NO_WHO",
  });
  assert.equal(g.primaryGap, WHO_RECALL_GAP_V12.ALIAS_DOMAIN_FAILURE);
});

test("functional domains extracted structurally", () => {
  const d = domainsFromFunctionalContacts([
    { email: "events@acme-events.org", sourceUrl: "https://acme-events.org/contact" },
    { email: "info@gmail.com" },
  ]);
  assert.ok(d.includes("acme-events.org"));
  assert.ok(!d.includes("gmail.com"));
});

test("gap-directed queries include site: staff deep-links", () => {
  const qs = buildGapDirectedQueriesV12({
    eventName: "Sample Annual Conference",
    organization: "Sample Association",
    functionalContacts: [{ email: "meetings@sample.org" }],
    currentNoWhoReason: "FUNCTIONAL_ONLY",
  });
  assert.ok(qs.some((q) => /site:sample\.org/i.test(q)));
  assert.ok(qs.some((q) => /staff|team|meetings/i.test(q)));
});

test("functional-only stages lead with pivot", () => {
  const stages = recallStagesForGapV12(WHO_RECALL_GAP_V12.FUNCTIONAL_ONLY);
  assert.equal(stages[0], RECALL_STAGE_V12.STAGE_7_FUNCTIONAL_PIVOT);
});

test("confirm accepts coherent staff person on official source", () => {
  const r = confirmWhoRecallCandidateV12(
    {
      name: "Jane Smith",
      role: "Meetings Manager",
      sectionKind: "STAFF",
      sourceUrl: "https://sample.org/staff",
      onOfficialDomain: true,
      domainClass: "ORGANIZATION_OFFICIAL",
      evidenceQuote: "Meetings Manager Jane Smith",
    },
    { organization: "Sample Association", opportunityName: "Sample Annual Conference" }
  );
  assert.equal(r.accept, true);
});

test("confirm rejects title-as-person under V11", () => {
  const r = confirmWhoRecallCandidateV12(
    {
      name: "Retired Associate Professor",
      role: "Executive Director",
      sectionKind: "STAFF",
      sourceUrl: "https://sample.org/staff",
      onOfficialDomain: true,
      domainClass: "ORGANIZATION_OFFICIAL",
    },
    { organization: "Sample Association" }
  );
  assert.equal(r.accept, false);
});

test("confirm rejects aggregator-only WHO", () => {
  const r = confirmWhoRecallCandidateV12(
    {
      name: "Alex Example",
      role: "Director of Marketing",
      sectionKind: "STAFF",
      sourceUrl: "https://infosec-conferences.com/about/",
      domainClass: "UNKNOWN",
      evidenceQuote: "Director of Marketing Alex Example",
    },
    { organization: "Sample Dev Conference", opportunityName: "Sample Dev Conference" }
  );
  assert.equal(r.accept, false);
  assert.equal(r.reason, "AGGREGATOR_WHO_WITHOUT_CORROBORATION");
});

test("confirm rejects honorific fragment", () => {
  const r = confirmWhoRecallCandidateV12(
    {
      name: "Dr Kseniya",
      role: "Executive Director",
      sectionKind: "CONTACT",
      sourceUrl: "https://example.org/contact",
      onOfficialDomain: true,
      domainClass: "EVENT_OFFICIAL",
      evidenceQuote:
        "Executive Director: Dr Kseniya Kizilova Institute for Comparative Survey Research Vienna",
    },
    { organization: "Example Org" }
  );
  // Either reject fragment or recover full name — never accept incomplete
  if (r.accept) {
    assert.match(r.name, /Kseniya\s+Kizilova/i);
  } else {
    assert.ok(r.reason);
  }
});

test("V11 person gate still blocks org phrase during recall", () => {
  const g = personTypeGateV11({
    name: "Comparative Survey",
    role: "Director",
    sectionKind: "CONTACT",
    evidenceQuote: "Institute for Comparative Survey Research",
  });
  assert.equal(g.reject, true);
});

test("operator stages include operator/housing stage", () => {
  const stages = recallStagesForGapV12(WHO_RECALL_GAP_V12.OPERATOR_UNRESOLVED);
  assert.ok(stages.includes(RECALL_STAGE_V12.STAGE_6_OPERATOR_HOUSING));
});

test("PDF gap stages lead with official PDF", () => {
  const stages = recallStagesForGapV12(WHO_RECALL_GAP_V12.PDF_CONTACT_MISSED);
  assert.equal(stages[0], RECALL_STAGE_V12.STAGE_5_OFFICIAL_PDF);
});

let pass = 0;
let fail = 0;
const failures = [];
for (const t of tests) {
  try {
    t.fn();
    pass += 1;
    console.log("PASS", t.name);
  } catch (err) {
    fail += 1;
    failures.push({ name: t.name, error: String(err.message || err) });
    console.log("FAIL", t.name, err.message || err);
  }
}
console.log(JSON.stringify({ suite: "who-recall-v12", pass, fail, total: tests.length, failures }, null, 2));
process.exit(fail ? 1 : 0);
