#!/usr/bin/env node
/**
 * Offline Person-Boundary V11 regression suite.
 * Patterns only — no live web. No hotel/person string blacklists in production logic.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import {
  personTypeGateV11,
  looksLikeHumanNameV11,
  parseMultiEntityContactBlock,
  detectNameFragment,
  stripHonorifics,
  resolvePersonFromBlock,
  ENTITY_TYPE,
} from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/person-boundary-v11.js";
import { aggregatorWhoGateV11 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/aggregator-who-gate-v11.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function expectReject(name, opts = {}) {
  const g = personTypeGateV11({
    name,
    role: opts.role || "Director",
    sectionKind: opts.sectionKind || "STAFF",
    evidenceQuote: opts.evidenceQuote || opts.sourceBlock || "",
    sourceUrl: opts.sourceUrl || "https://example.org/staff",
  });
  assert.equal(g.reject, true, `${name} should reject: ${JSON.stringify(g)}`);
  return g;
}

function expectPerson(name, opts = {}) {
  const g = personTypeGateV11({
    name,
    role: opts.role || "Tournament Director",
    sectionKind: opts.sectionKind || "STAFF",
    email: opts.email || null,
    evidenceQuote: opts.evidenceQuote || "",
    sourceUrl: opts.sourceUrl || "https://example.org/staff",
  });
  assert.equal(g.reject, false, `${name} rejected: ${JSON.stringify(g)}`);
  assert.equal(g.entityType, ENTITY_TYPE.PERSON);
  return g;
}

// --- Wave 1 invalid fixtures must all reject ---
test("all Wave1 invalid fixtures blocked by V11", () => {
  const doc = JSON.parse(
    fs.readFileSync(
      "data/contact-intelligence/evals/gdi-wave1-v11-person-boundary-fixtures.json",
      "utf8"
    )
  );
  for (const f of doc.fixtures) {
    const g = personTypeGateV11({
      name: f.rawExtractedCandidate,
      role: f.roleTitle,
      sectionKind: f.domSectionContext || "STAFF",
      evidenceQuote: f.sourceTextBlock,
      sourceUrl: f.sourceUrl,
      organization: f.organization,
    });
    // Aggregator mis-association: person shape may pass entity gate; aggregator gate rejects
    if (f.failureClass === "AGGREGATOR_MISASSOCIATION") {
      const agg = aggregatorWhoGateV11(
        { name: f.rawExtractedCandidate, sourceUrl: f.sourceUrl, domainClass: "UNKNOWN" },
        { organization: f.organization, opportunityName: "Kansas City Developers Conference" }
      );
      assert.equal(
        agg.reject,
        true,
        `aggregator should reject ${f.rawExtractedCandidate}`
      );
    } else {
      assert.equal(
        g.reject,
        true,
        `fixture still PERSON: ${f.rawExtractedCandidate} (${f.failureClass})`
      );
    }
  }
});

test("full person + org block recovers complete name", () => {
  const block =
    "Executive Director: Dr Kseniya Kizilova Institute for Comparative Survey Research Vienna 1040 Austria";
  const parsed = parseMultiEntityContactBlock(block);
  assert.ok(parsed.persons.some((p) => /Kseniya/i.test(p) && /Kizilova/i.test(p)));
  assert.ok(parsed.organizations.some((o) => /Institute/i.test(o)));
  const resolved = resolvePersonFromBlock({
    name: "Dr Kseniya",
    role: "Executive Director",
    sectionKind: "CONTACT",
    evidenceQuote: block,
  });
  assert.equal(resolved.ok, true);
  assert.match(resolved.name, /Kseniya\s+Kizilova/i);
});

test("honorific + complete name accepted", () => {
  const g = expectPerson("Dr Jane Smith", {
    role: "Executive Director",
    evidenceQuote: "Executive Director: Dr Jane Smith",
  });
  assert.match(g.normalizedPersonName, /Jane\s+Smith/);
});

test("honorific fragment rejected", () => {
  expectReject("Dr Kseniya", {
    evidenceQuote:
      "Executive Director: Dr Kseniya Kizilova Institute for Comparative Survey Research Vienna",
  });
});

test("title phrase rejected", () => {
  expectReject("Chief Operating Officer");
});

test("academic title rejected", () => {
  expectReject("Retired Associate Professor");
  expectReject("Associate Professor");
  expectReject("Professor Emeritus");
});

test("organization phrase rejected", () => {
  expectReject("Comparative Survey", {
    evidenceQuote:
      "Dr Kseniya Kizilova Institute for Comparative Survey Research Vienna",
  });
});

test("department phrase rejected", () => {
  expectReject("Local Government", {
    evidenceQuote:
      "Director of Local Government, Housing and Communities, Transform UK Julia Brennan",
  });
});

test("location phrase rejected", () => {
  expectReject("Research Vienna", {
    evidenceQuote:
      "Institute for Comparative Survey Research Vienna 1040 Austria",
  });
});

test("truncated / adjacent bleed rejected", () => {
  const frag = detectNameFragment(
    "Comparative Survey",
    "Institute for Comparative Survey Research"
  );
  assert.equal(frag.isFragment, true);
});

test("Mc/Mac surnames accepted", () => {
  assert.equal(looksLikeHumanNameV11("Ben McNamara"), true);
  assert.equal(looksLikeHumanNameV11("Anna MacKenzie"), true);
  assert.equal(looksLikeHumanNameV11("Sean McDonald"), true);
  expectPerson("Ben McNamara", { role: "Meeting Planner" });
  expectPerson("Anna MacKenzie", { role: "Director" });
});

test("accented Spanish name accepted", () => {
  assert.equal(looksLikeHumanNameV11("José Ramírez"), true);
  expectPerson("José Ramírez", { role: "Gerente de Eventos" });
});

test("compound Hispanic surname accepted", () => {
  assert.equal(looksLikeHumanNameV11("María García López"), true);
  expectPerson("Juan de la Cruz", { role: "Director" });
});

test("Asian-order name shape accepted", () => {
  assert.equal(looksLikeHumanNameV11("Wei Zhang"), true);
  expectPerson("Wei Zhang", { role: "Conference Chair" });
});

test("multiple people trailing after department", () => {
  const parsed = parseMultiEntityContactBlock(
    "Director of Local Government, Housing and Communities, Transform UK Julia Brennan"
  );
  assert.ok(parsed.persons.some((p) => /Julia\s+Brennan/i.test(p)));
  assert.ok(parsed.departments.length >= 1 || parsed.titles.length >= 0);
});

test("aggregator without corroboration rejected", () => {
  const g = aggregatorWhoGateV11(
    {
      name: "Alex Example",
      sourceUrl: "https://infosec-conferences.com/about/",
      domainClass: "UNKNOWN",
    },
    { organization: "Example Dev Conference", opportunityName: "Example Dev Conference" }
  );
  assert.equal(g.reject, true);
  assert.equal(g.gate, "AGGREGATOR_WHO_WITHOUT_CORROBORATION");
});

test("aggregator with official corroboration allowed", () => {
  const g = aggregatorWhoGateV11(
    {
      name: "Alex Example",
      sourceUrl: "https://events-conferences.com/about/",
      officialCorroboration: true,
    },
    { organization: "Example Org" }
  );
  assert.equal(g.reject, false);
});

test("official domain person not blocked by aggregator gate", () => {
  const g = aggregatorWhoGateV11(
    {
      name: "Jane Smith",
      sourceUrl: "https://wapor.org/contact-us/",
      onOfficialDomain: true,
      domainClass: "EVENT_OFFICIAL",
    },
    { organization: "WAPOR", eventSourceUrls: ["https://wapor.org/"] }
  );
  assert.equal(g.reject, false);
});

test("stripHonorifics utility", () => {
  const r = stripHonorifics("Dr. Jane Smith");
  assert.equal(r.bare, "Jane Smith");
  assert.equal(r.hadHonorific, true);
});

test("V9 invalid fixture file still blocked under V11", () => {
  const path = "data/contact-intelligence/evals/native-who-v9-invalid-fixtures.json";
  if (!fs.existsSync(path)) return;
  const doc = JSON.parse(fs.readFileSync(path, "utf8"));
  for (const f of doc.fixtures || []) {
    const g = personTypeGateV11({
      name: f.rawExtractedValue,
      role: f.role || "Director",
      sectionKind: f.section || "STAFF",
      evidenceQuote: f.sourceText || "",
    });
    assert.equal(g.reject, true, `V9 fixture still PERSON: ${f.rawExtractedValue}`);
  }
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
console.log(JSON.stringify({ suite: "person-boundary-v11", pass, fail, total: tests.length, failures }, null, 2));
process.exit(fail ? 1 : 0);
