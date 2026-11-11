import assert from "node:assert/strict";
import { classifyEvidenceRecovery, shadowQualification } from "./gdi-complete-hotel-inventory-audit-v1.mjs";

const empty = classifyEvidenceRecovery({});
assert.equal(empty.evidenceClass, "D");

const generated = classifyEvidenceRecovery({
  hotelOpportunityThesis: "The group needs a guestroom block at this hotel.",
  sources: [{ url: "https://example.org/event" }],
});
assert.equal(generated.evidenceClass, "C");

const connected = classifyEvidenceRecovery({
  evidenceQuote: "The housing bureau published a guestroom block for delegates.",
  sources: [{ url: "https://example.org/housing", quote: "guestroom block" }],
});
assert.equal(connected.evidenceClass, "A");

const speculative = shadowQualification({
  hotelOpportunityThesis: "The group needs guestrooms.",
  organizationName: "Example Association",
});
assert.notEqual(speculative.layer, "SALES_READY");

console.log(JSON.stringify({ ok: true }));
