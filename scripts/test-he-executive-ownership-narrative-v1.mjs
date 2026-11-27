#!/usr/bin/env node
/**
 * Customer-language + ownership-classification gates for Executive Ownership Summary.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildExecutiveOwnershipNarrative,
  classifyOwnershipParty,
  containsInternalOwnershipTerminology,
  sanitizeCustomerPartyName,
} from "../lib/hotel-intelligence/ownership/he-executive-ownership-narrative-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const beth = buildExecutiveOwnershipNarrative({
  hotelName: "Bethesda Marriott",
  legalOwner: { name: "Marbeth Acquisition", known: true, status: "VALIDATED" },
  economicOwner: {
    name: "Mullen Real Estate Capital",
    known: true,
    status: "PROPOSED",
    note: "Sponsor attribution — not certified",
  },
  brand: "Marriott Hotels",
  operator: "Marriott International",
});

assert.match(beth.text, /Bethesda Marriott's legal property company is Marbeth Acquisition/i);
assert.match(
  beth.text,
  /Mullen Real Estate Capital has been identified as a potential economic sponsor/i
);
assert.match(
  beth.text,
  /ultimate ownership interest has not been independently confirmed/i
);
assert.match(
  beth.text,
  /operates under the Marriott Hotels brand and is managed by Marriott International/i
);
assert.equal(beth.classification.economicProposed, true);
assert.equal(beth.classification.economicVerified, false);
assert.equal(beth.classification.legalVerified, true);
assert.ok(!/confirmed ultimate|validated economic owner|UBO|HI graph|Airtable/i.test(beth.text));
assert.ok(beth.sentences.length >= 2 && beth.sentences.length <= 3);
assert.equal(
  sanitizeCustomerPartyName("Marriott International (Managed)"),
  "Marriott International"
);
const bethManagedSuffix = buildExecutiveOwnershipNarrative({
  hotelName: "Bethesda Marriott",
  legalOwner: { name: "Marbeth Acquisition", known: true, status: "VALIDATED" },
  economicOwner: {
    name: "Mullen Real Estate Capital",
    known: true,
    status: "PROPOSED",
    note: "Sponsor attribution — not certified",
  },
  brand: "Marriott Hotels",
  operator: "Marriott International (Managed)",
});
assert.equal(
  bethManagedSuffix.text,
  "Bethesda Marriott's legal property company is Marbeth Acquisition. Mullen Real Estate Capital has been identified as a potential economic sponsor, although its ultimate ownership interest has not been independently confirmed. The hotel operates under the Marriott Hotels brand and is managed by Marriott International."
);

const verifiedEcon = buildExecutiveOwnershipNarrative({
  hotelName: "Example Hotel",
  legalOwner: { name: "Example PropCo", known: true, status: "VALIDATED" },
  economicOwner: { name: "Example Capital", known: true, status: "VALIDATED" },
  brand: "Example Brand",
  operator: "Example Operator",
});
assert.match(verifiedEcon.text, /Economic ownership is attributed to Example Capital/);
assert.equal(verifiedEcon.classification.economicVerified, true);

assert.equal(
  classifyOwnershipParty({
    name: "Mullen Real Estate Capital",
    known: true,
    status: "VALIDATED",
    note: "sponsor attribution; UBO unresolved",
  }).proposed,
  true
);

assert.equal(
  containsInternalOwnershipTerminology(
    "Source: HI Organizations + HI Hotel–Org Relationships (live Airtable)."
  ),
  true
);
assert.equal(containsInternalOwnershipTerminology(beth.text), false);

const ws = read("public/js/hotel-intelligence-workspace.js");
assert.match(ws, /buildCustomerExecutiveOwnershipHtml/);
assert.match(ws, /HeExecutiveOwnershipNarrative/);
assert.doesNotMatch(ws, /Source: HI Organizations \+ HI Hotel–Org Relationships/);
assert.doesNotMatch(ws, /Phase 3 Proposed Preview — staged ownership overlay/);

const api = read("api/golden-demo-ownership.js");
assert.match(api, /buildExecutiveOwnershipNarrative/);
assert.doesNotMatch(api, /HI graph lists/);
assert.doesNotMatch(api, /Validated HI graph economic owner/);
assert.doesNotMatch(api, /HI graph includes/);
assert.doesNotMatch(api, /Validated HI graph ownership edges/);
assert.doesNotMatch(api, /HI graph evidence and relationship edges/);
assert.doesNotMatch(api, /Source documents persisted in HI Graph Evidence/);

const browser = read("public/js/he-executive-ownership-narrative-v1.js");
assert.match(browser, /HeExecutiveOwnershipNarrative/);
assert.match(browser, /potential economic sponsor/);

const radar = read("public/deal-capture-radar-with-ranked-list.html");
const preview = read("public/hotel-explorer-preview.html");
assert.match(radar, /he-executive-ownership-narrative-v1\.js/);
assert.match(preview, /he-executive-ownership-narrative-v1\.js/);

const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-executive-ownership-narrative-v1/);

console.log("ok - he-executive-ownership-narrative-v1");
