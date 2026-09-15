#!/usr/bin/env node
/**
 * External-safety regressions for GDI canonical contact share projection + validation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  projectCanonicalContactForExternal,
  overlayCanonicalContactOnOpportunity,
  assertNoProviderLeak,
  EXTERNAL_PHONE_TYPE,
} from "../lib/group-demand-intelligence/canonical-contact-external-projection.js";
import {
  buildCanonicalPersonEntry,
  saveCanonicalContacts,
  resolveCanonicalOverlayForOpportunity,
  applyCanonicalOverlaysToOpportunities,
  loadCanonicalContacts,
} from "../lib/group-demand-intelligence/canonical-contacts-store.js";
import {
  saveShareValidationItem,
  loadShareValidation,
  mapValidationToFeedbackEvents,
} from "../lib/group-demand-intelligence/share-validation.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import { isPaidEnrichmentEnabled } from "../lib/hotel-intelligence/contact-intelligence/policy.js";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-share-ext-"));
process.env.GDI_DATA_ROOT = tmpRoot; // if repository honored it — otherwise write via hotelDir under data

// Use real hotelDir under data with a disposable hotel id
const hotelA = "recTEST_GDI_SHARE_A";
const hotelB = "recTEST_GDI_SHARE_B";

function seedHotel(hotelId, people, bindings) {
  saveCanonicalContacts(hotelId, {
    hotelId,
    people,
    opportunityBindings: bindings,
  });
}

const amy = buildCanonicalPersonEntry({
  displayName: "Amy Drow",
  organization: "National Down Syndrome Society",
  title: "Director of Events",
  identityDecision: "ACCEPTED",
  fields: {
    EMAIL: {
      value: "adrow@ndss.org",
      canonicalStatus: "CANONICAL",
      provenance: { provider: "surfe", surfeOutcome: "NEW_DIRECT_WORK_EMAIL" },
    },
    MOBILE: {
      value: "+14074962293",
      canonicalStatus: "CANONICAL",
      provenance: { provider: "surfe", surfeOutcome: "NEW_MOBILE_PROFESSIONAL" },
    },
  },
});

check("1. applied canonical email displays", () => {
  const proj = projectCanonicalContactForExternal({
    canonicalPerson: amy,
    relationship: { eventRole: "Director of Events" },
  });
  assert.equal(proj.email, "adrow@ndss.org");
});

check("2. applied canonical mobile displays", () => {
  const proj = projectCanonicalContactForExternal({
    canonicalPerson: amy,
    relationship: { eventRole: "Director of Events" },
  });
  assert.equal(proj.phone, "+14074962293");
  assert.equal(proj.phoneType, EXTERNAL_PHONE_TYPE.MOBILE);
  assert.equal(proj.phoneTypeLabel, "Mobile");
});

check("3. rolled-back field does not display", () => {
  const ben = buildCanonicalPersonEntry({
    displayName: "Ben Hawkins",
    organization: "Alexandria Soccer Association",
    identityDecision: "ACCEPTED",
    fields: {
      EMAIL: {
        value: "ben@alexandria-soccer.org",
        canonicalStatus: "CANONICAL",
        status: "ROLLED_BACK",
        rolledBack: true,
      },
    },
  });
  // buildCanonicalPersonEntry should strip rolled-back
  assert.equal(ben.fields.EMAIL, undefined);
  const withMeta = {
    ...ben,
    fields: {
      EMAIL: {
        value: "ben@alexandria-soccer.org",
        canonicalStatus: "CANONICAL",
        rolledBack: true,
      },
    },
  };
  const proj = projectCanonicalContactForExternal({ canonicalPerson: withMeta });
  assert.equal(proj?.email ?? null, null);
});

check("4. ambiguous provider result does not display", () => {
  seedHotel(hotelA, [
    {
      ...amy,
      identityDecision: "AMBIGUOUS",
    },
  ], [{ opportunityId: "opp_1", personId: amy.personId, active: true }]);
  const { projection } = resolveCanonicalOverlayForOpportunity(hotelA, {
    id: "opp_1",
    title: "Test",
    primaryContact: null,
  });
  assert.equal(projection, null);
});

check("5. rejected field does not display", () => {
  const person = {
    displayName: "X",
    organization: "Y",
    identityDecision: "ACCEPTED",
    fields: {
      EMAIL: {
        value: "x@y.org",
        canonicalStatus: "CANONICAL",
        mergeDecision: "REJECT_FIELD",
      },
    },
  };
  const proj = projectCanonicalContactForExternal({ canonicalPerson: person });
  assert.equal(proj?.email ?? null, null);
});

check("6. provider name never appears externally", () => {
  const proj = projectCanonicalContactForExternal({
    canonicalPerson: amy,
    relationship: { eventRole: "Director of Events" },
  });
  const overlaid = overlayCanonicalContactOnOpportunity(
    { id: "opp", title: "T", primaryContact: null },
    proj
  );
  const sanitized = sanitizeOpportunityForShare(overlaid);
  assertNoProviderLeak(sanitized);
  assert.ok(!JSON.stringify(sanitized).toLowerCase().includes("surfe"));
});

check("7. provider credits never appear externally", () => {
  const proj = projectCanonicalContactForExternal({ canonicalPerson: amy });
  assert.ok(!JSON.stringify(proj).includes("credits"));
  assertNoProviderLeak(proj);
});

check("8. validation stored separately from research", () => {
  const beforePeople = loadCanonicalContacts(hotelA).people;
  saveShareValidationItem(hotelA, {
    opportunityId: "opp_1",
    familiarityStatus: "NEVER_SEEN_BEFORE",
    commercialValue: "WORTH_PURSUING_NOW",
    contactPersonAssessment: "RIGHT_PERSON",
    emailAssessment: "USEFUL",
    phoneAssessment: "DIRECT_USABLE",
  });
  const val = loadShareValidation(hotelA);
  assert.equal(val.items.length, 1);
  assert.equal(val.items[0].doesNotOverwriteCanonicalFacts, true);
  // research package path not touched — canonical people unchanged
  assert.deepEqual(loadCanonicalContacts(hotelA).people, beforePeople);
});

check("9. hotel feedback does not silently mutate canonical record", () => {
  const before = JSON.stringify(loadCanonicalContacts(hotelA));
  saveShareValidationItem(hotelA, {
    opportunityId: "opp_1",
    familiarityStatus: "ALREADY_KNOWN",
    commercialValue: "NOT_WORTH_PURSUING",
    contactPersonAssessment: "WRONG_PERSON",
    emailAssessment: "WRONG",
    phoneAssessment: "WRONG",
  });
  const events = loadShareValidation(hotelA).feedbackEvents;
  assert.ok(events.some((e) => e.type === "WRONG_PERSON"));
  assert.ok(events.some((e) => e.type === "EMAIL_WRONG"));
  assert.equal(JSON.stringify(loadCanonicalContacts(hotelA)), before);
});

check("10. same validation surface works for another hotel fixture", () => {
  const casey = buildCanonicalPersonEntry({
    displayName: "Casey Planner",
    organization: "Coastal Association",
    identityDecision: "ACCEPTED",
    fields: {
      EMAIL: { value: "casey@coastal.org", canonicalStatus: "CANONICAL" },
      MOBILE: {
        value: "3055550199",
        canonicalStatus: "CANONICAL",
        provenance: { surfeOutcome: "NEW_MOBILE_PROFESSIONAL" },
      },
    },
  });
  seedHotel(hotelB, [casey], [
    { opportunityId: "opp_b1", personId: casey.personId, eventRole: "Director", active: true },
  ]);
  const { opportunity, projection } = resolveCanonicalOverlayForOpportunity(hotelB, {
    id: "opp_b1",
    title: "Bay Cup",
    primaryContact: { name: "Generic", email: "info@coastal.org" },
  });
  assert.equal(projection.email, "casey@coastal.org");
  assert.equal(opportunity.primaryContact.email, "casey@coastal.org");
  saveShareValidationItem(hotelB, {
    opportunityId: "opp_b1",
    familiarityStatus: "NEVER_SEEN_BEFORE",
    commercialValue: "WORTH_PURSUING_NOW",
    phoneAssessment: "DIRECT_USABLE",
  });
  assert.equal(loadShareValidation(hotelB).items[0].opportunityId, "opp_b1");
});

check("Surfe remains globally OFF", () => {
  assert.equal(isPaidEnrichmentEnabled(), false);
});

check("mapValidationToFeedbackEvents covers phone useful", () => {
  const events = mapValidationToFeedbackEvents({
    hotelId: hotelA,
    opportunityId: "opp_x",
    id: "sv1",
    phoneAssessment: "DIRECT_USABLE",
    emailAssessment: "USEFUL",
    contactPersonAssessment: "RIGHT_PERSON",
  });
  assert.ok(events.some((e) => e.type === "PHONE_USEFUL"));
  assert.ok(events.some((e) => e.type === "EMAIL_USEFUL"));
});

if (failed) {
  console.error(`\n${failed} failing`);
  process.exit(1);
}

// Cleanup disposable hotel fixtures written under data/
for (const id of [hotelA, hotelB]) {
  const dir = path.join(
    process.cwd(),
    "data/group-demand-intelligence/hotels",
    id
  );
  if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
}

console.log("\nAll GDI share external-safety tests passed.");
