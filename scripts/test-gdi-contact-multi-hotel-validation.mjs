#!/usr/bin/env node
/**
 * GDI contact multi-hotel validation — synthetic fixtures only.
 * No paid research. No production writes.
 *
 * Usage: npm run test:gdi-contact-multi-hotel-validation
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createCanonicalPersonRegistry,
  createCanonicalField,
  evaluateContactFieldMerge,
  applyHotelFeedback,
  FIELD_SOURCE_CLASS,
  FIELD_MERGE_ACTION,
  CANONICAL_FIELD_KIND,
  MERGE_WRITE_MODE,
  HOTEL_FEEDBACK_STATE,
  REUSE_OUTCOME,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js";
import { IDENTITY_DECISION as ID } from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import {
  calculateGdiContactCoverage,
  classifyReachabilityNeed,
  REACHABILITY_NEED,
} from "../lib/group-demand-intelligence/contact-coverage.js";
import { scoreContactCandidate } from "../lib/group-demand-intelligence/contact-candidate/index.js";
import { PRIMARY_KIND } from "../lib/group-demand-intelligence/contact-candidate/person-discovery-states.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const OUT_MD = path.join(
  root,
  "reports/group-demand-intelligence/gdi-contact-multi-hotel-validation.md"
);

let failed = 0;
const findings = [];
function check(name, fn) {
  try {
    fn();
    findings.push(`PASS ${name}`);
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    findings.push(`FAIL ${name}: ${err.message}`);
    console.error("FAIL", name, err.message);
  }
}

/** Synthetic Hotel A — urban full-service business */
const HOTEL_A = {
  hotelId: "recSynthUrbanBusiness001",
  hotelName: "Harborview Business Hotel",
  geography: ["miami", "florida"],
  opportunities: [
    {
      id: "gdi_opp_synth_coastal_annual_2027",
      priority: "HIGH_PRIORITY",
      title: "Coastal Association Annual Meeting 2027",
      opportunityType: "PRIMARY_PURSUIT",
      organizationName: "Coastal Association",
      geographyHints: ["miami", "florida"],
      primaryContact: {
        name: "Casey Planner",
        role: "Director of Meetings",
        organization: "Coastal Association",
        email: "casey@coastal.org",
      },
    },
  ],
};

/** Synthetic Hotel B — resort / group-heavy */
const HOTEL_B = {
  hotelId: "recSynthResortGroup002",
  hotelName: "Palm Bay Resort & Conference",
  geography: ["tampa", "florida"],
  opportunities: [
    {
      id: "gdi_opp_synth_bay_cup_2027",
      priority: "HIGH_PRIORITY",
      title: "Bay Cup 2027 Overflow Housing",
      opportunityType: "OVERFLOW_HOUSING",
      organizationName: "Bay Soccer",
      geographyHints: ["tampa"],
      primaryContact: {
        name: "Harbor Housing Co",
        role: "Official housing partner",
        organization: "Harbor Housing Co",
        email: "support@harborhousing.example",
        functionalEntity: true,
        targetRoleMatch: "HOUSING_SOURCING_CONTACT",
      },
    },
    {
      id: "gdi_opp_synth_coastal_board_2027",
      priority: "MEDIUM_PRIORITY",
      title: "Coastal Association Board Retreat 2027",
      opportunityType: "PRIMARY_PURSUIT",
      organizationName: "Coastal Association",
      geographyHints: ["tampa"],
      primaryContact: {
        name: "Casey Planner",
        role: "Director of Meetings",
        organization: "Coastal Association",
      },
    },
  ],
};

check("different hotel IDs and geographies load as data", () => {
  assert.notEqual(HOTEL_A.hotelId, HOTEL_B.hotelId);
  assert.ok(HOTEL_A.geography.includes("miami"));
  assert.ok(HOTEL_B.geography.includes("tampa"));
});

check("WHO scoring portable across geographyHints", () => {
  const base = {
    name: "Casey Planner",
    role: "Director of Meetings",
    organization: "Coastal Association",
    claimKind: "FACT",
    sourceUrl: "https://coastal.example/staff",
    eventSpecificEvidence: true,
  };
  const a = scoreContactCandidate(base, {
    title: HOTEL_A.opportunities[0].title,
    opportunityType: "PRIMARY_PURSUIT",
    organizationName: "Coastal Association",
    geographyHints: HOTEL_A.geography,
  });
  const b = scoreContactCandidate(base, {
    title: HOTEL_B.opportunities[1].title,
    opportunityType: "PRIMARY_PURSUIT",
    organizationName: "Coastal Association",
    geographyHints: HOTEL_B.geography,
  });
  assert.equal(a.breakdown.roleRelevance, b.breakdown.roleRelevance);
});

check("coverage calculator hotel-agnostic for both hotels", () => {
  const covA = calculateGdiContactCoverage({
    opportunities: HOTEL_A.opportunities,
    discoveryRows: [
      {
        opportunityId: HOTEL_A.opportunities[0].id,
        primaryKind: PRIMARY_KIND.NAMED_PERSON,
        primaryCandidate: {
          name: "Casey Planner",
          email: "casey@coastal.org",
          candidateConfidence: "HIGH",
        },
      },
    ],
  });
  const covB = calculateGdiContactCoverage({
    opportunities: HOTEL_B.opportunities,
    discoveryRows: [
      {
        opportunityId: HOTEL_B.opportunities[0].id,
        primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
        primaryCandidate: {
          name: "Harbor Housing Co",
          email: "support@harborhousing.example",
          functionalEntity: true,
          candidateConfidence: "HIGH",
        },
      },
      {
        opportunityId: HOTEL_B.opportunities[1].id,
        primaryKind: PRIMARY_KIND.NAMED_PERSON,
        primaryCandidate: {
          name: "Casey Planner",
          candidateConfidence: "HIGH",
        },
      },
    ],
  });
  assert.equal(covA.namedPersonPrimaries, 1);
  assert.equal(covB.functionalEntityPrimaries, 1);
  assert.equal(covB.namedPersonPrimaries, 1);
});

check("Hotel A establishes person → Hotel B reuses without Surfe", () => {
  const reg = createCanonicalPersonRegistry();
  // Hotel A research result
  reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    title: "Director of Meetings",
    fields: {
      EMAIL: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.EMAIL,
        value: "casey@coastal.org",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
        hotelId: HOTEL_A.hotelId,
        opportunityId: HOTEL_A.opportunities[0].id,
        canonicalStatus: "CANONICAL",
      }),
      MOBILE: createCanonicalField({
        kind: CANONICAL_FIELD_KIND.MOBILE,
        value: "3055550199",
        sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
        hotelId: HOTEL_A.hotelId,
        canonicalStatus: "CANONICAL",
      }),
    },
  });
  reg.addRelationship({
    hotelId: HOTEL_A.hotelId,
    opportunityId: HOTEL_A.opportunities[0].id,
    opportunityTitle: HOTEL_A.opportunities[0].title,
    eventRole: "Director of Meetings",
    whyThisPerson: "Official staff page",
  });

  // Hotel B encounters same person
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
    needPhone: true,
  });
  assert.equal(reuse.outcome, REUSE_OUTCOME.CONTACT_REUSED_FROM_CANONICAL);
  assert.equal(reuse.requestEmail, false);
  assert.equal(reuse.requestMobile, false);
  assert.ok(reuse.providerCallsAvoided >= 2);

  reg.addRelationship({
    hotelId: HOTEL_B.hotelId,
    opportunityId: HOTEL_B.opportunities[1].id,
    opportunityTitle: HOTEL_B.opportunities[1].title,
    eventRole: "Board retreat planner",
    whyThisPerson: "Same org meetings director",
  });

  const snap = reg.snapshot();
  assert.equal(snap.people.length, 1);
  const hotels = new Set(snap.relationships.map((r) => r.hotelId));
  assert.ok(hotels.has(HOTEL_A.hotelId) && hotels.has(HOTEL_B.hotelId));
  // Event roles differ; person title remains org-level
  assert.equal(snap.people[0].title, "Director of Meetings");
  assert.notEqual(
    snap.relationships[0].eventRole,
    snap.relationships[1].eventRole
  );
});

check("merge engine rejects ambiguous identity identically for any hotel", () => {
  const d = evaluateContactFieldMerge({
    canonicalPerson: { displayName: "X", organization: "Y" },
    canonicalField: null,
    incomingField: createCanonicalField({ value: "x@y.org" }),
    identityDecision: ID.AMBIGUOUS,
  });
  assert.equal(d.action, FIELD_MERGE_ACTION.REJECT_FIELD);
});

check("functional entity not enrichment-eligible (portable)", () => {
  const need = classifyReachabilityNeed({
    name: "Harbor Housing Co",
    email: "support@harborhousing.example",
    phone: "8135550100",
    candidateConfidence: "HIGH",
    primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
    functionalEntity: true,
  });
  assert.equal(need, REACHABILITY_NEED.NOT_ELIGIBLE);
});

check("core merge module has no Bethesda hotelId hardcode", () => {
  const txt = fs.readFileSync(
    path.join(root, "lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js"),
    "utf8"
  );
  assert.ok(!/recLuxvwwxID7U2B8/.test(txt));
  assert.ok(!/Bethesda Premier Cup/.test(txt));
  assert.ok(!/Potomac Memorial/.test(txt));
});

check("dry-run write mode never sets mutated=true via applyMergeDecision", () => {
  const reg = createCanonicalPersonRegistry();
  const person = reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
  });
  const d = evaluateContactFieldMerge({
    canonicalPerson: person,
    canonicalField: null,
    incomingField: createCanonicalField({
      value: "casey@coastal.org",
      sourceClass: FIELD_SOURCE_CLASS.PROVIDER_ACCEPTED,
    }),
    identityDecision: ID.ACCEPTED,
  });
  const applied = reg.applyMergeDecision({
    person,
    fieldKind: "EMAIL",
    decision: d,
    writeMode: MERGE_WRITE_MODE.DRY_RUN,
  });
  assert.equal(applied.mutated, false);
});

check("hotel feedback wrong-person blocks cross-hotel reuse", () => {
  const reg = createCanonicalPersonRegistry();
  let person = reg.upsertPerson({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    fields: {
      EMAIL: createCanonicalField({ value: "casey@coastal.org" }),
    },
  });
  person = applyHotelFeedback(person, { state: HOTEL_FEEDBACK_STATE.WRONG_PERSON });
  reg.upsertPerson(person);
  const reuse = reg.resolveReuse({
    name: "Casey Planner",
    organization: "Coastal Association",
    needEmail: true,
  });
  assert.equal(reuse.outcome, null);
});

const verdict = failed ? "COUPLING FOUND" : "PORTABLE";

const md = `# GDI Contact Multi-Hotel Validation

**Verdict: ${verdict}**

Synthetic fixtures only — no paid research.

## Hotels

| | Hotel A | Hotel B |
|---|---|---|
| ID | \`${HOTEL_A.hotelId}\` | \`${HOTEL_B.hotelId}\` |
| Type | Urban full-service business | Resort / group-heavy |
| Geography | ${HOTEL_A.geography.join(", ")} | ${HOTEL_B.geography.join(", ")} |

## Checks

${findings.map((f) => `- ${f}`).join("\n")}

## Proven reuse scenario

1. Hotel A discovers **Casey Planner** with accepted email + mobile  
2. Hotel B encounters same person on a different opportunity  
3. Canonical lookup reuses reachability → **Surfe not called**  
4. Hotel B stores its own opportunity relationship  
5. Person-canonical fields remain shared; event roles stay separate  

## Operating Law

| Issue | Classification | Implementation |
|---|---|---|
| Merge / reuse engine | REUSABLE_PRODUCT_LOGIC | \`canonical-merge-policy.js\` |
| Synthetic hotel IDs / events | HOTEL_SPECIFIC_DATA | this validation script fixtures |
| Bethesda pilot eval | HOTEL_SPECIFIC_DATA | \`evals/bethesda-*\` |

## Regression

\`\`\`bash
npm run test:canonical-contact-merge-policy
npm run test:gdi-contact-multi-hotel-validation
\`\`\`
`;

fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
fs.writeFileSync(OUT_MD, md);

console.log(JSON.stringify({ verdict, failed, outMd: OUT_MD }, null, 2));
if (failed) process.exit(1);
