/**
 * Offline gate — Prospeo email-only client + GDI verification (no live calls).
 */
import assert from "node:assert/strict";
import {
  buildProspeoEnrichPersonBody,
  normalizeProspeoEnrichPersonResponse,
  PROSPEO_CREDIT_COSTS,
} from "../lib/prospeo/client.js";
import {
  verifyProspeoIdentityMatch,
  enrichGdiContactViaProspeo,
} from "../lib/group-demand-intelligence/presentation/gdi-prospeo-contact-enrichment-v1.js";

function testBodyEmailOnly() {
  const body = buildProspeoEnrichPersonBody({
    firstName: "Olivia",
    lastName: "Jallits",
    companyName: "IAADFS",
    companyDomain: "iaadfs.org",
  });
  assert.equal(body.enrich_mobile, false);
  assert.equal(body.only_verified_email, true);
  assert.equal(body.data.company_website, "iaadfs.org");
}

function testNormalizeCredits() {
  const n = normalizeProspeoEnrichPersonResponse({
    ok: true,
    http_status: 200,
    payload: {
      error: false,
      free_enrichment: false,
      response: {
        person: {
          full_name: "Olivia Jallits",
          email: "olivia@iaadfs.org",
          company_name: "IAADFS",
        },
      },
    },
  });
  assert.equal(n.email, "olivia@iaadfs.org");
  assert.equal(n.credits_consumed_estimate, PROSPEO_CREDIT_COSTS.email_found);
}

function testVerifyRejectGeneric() {
  const v = verifyProspeoIdentityMatch({
    expectedName: "Laura Keeley",
    expectedEmployer: "PDA",
    normalized: {
      full_name: "Laura Keeley",
      company_name: "PDA",
      email: "info@pda.org",
    },
  });
  assert.equal(v.accepted, false);
  assert.ok(v.reasons.includes("GENERIC_MAILBOX"));
}

function testNotAuthorized() {
  return enrichGdiContactViaProspeo({
    authorizeLiveProspeo: false,
    contact: { name: "Test Person", employerName: "Acme", employerDomain: "acme.com" },
  }).then((r) => {
    assert.equal(r.skipped, true);
    assert.equal(r.reason, "PROSPEO_NOT_AUTHORIZED");
  });
}

testBodyEmailOnly();
testNormalizeCredits();
testVerifyRejectGeneric();
await testNotAuthorized();
console.log("test-gdi-prospeo-contact-enrichment-v1: PASS");
