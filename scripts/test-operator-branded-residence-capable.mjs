#!/usr/bin/env node
/**
 * Unit checks for branded-residence capability derivation.
 * Usage: node scripts/test-operator-branded-residence-capable.mjs
 */
import assert from "node:assert/strict";
import { deriveBrandedResidentialCapable } from "../lib/operator-branded-residence-capable.js";

function row(fields) {
  return { fields };
}

assert.equal(
  deriveBrandedResidentialCapable({
    commercial: row({ "Branded Residences Allowed": "No" }),
    profile: row({ "Service Models Supported": ["Branded residential / mixed-use"] }),
  }),
  false,
  "explicit No should override service model"
);

assert.equal(
  deriveBrandedResidentialCapable({
    commercial: row({ "Branded Residences Allowed": "Yes" }),
  }),
  true,
  "Yes on allowed"
);

assert.equal(
  deriveBrandedResidentialCapable({
    profile: row({ "Service Models Supported": ["Branded residential / mixed-use"] }),
  }),
  true,
  "service model match"
);

assert.equal(
  deriveBrandedResidentialCapable({
    commercial: row({ "Branded Residence Experience Level": "Moderate" }),
  }),
  true,
  "experience level"
);

assert.equal(
  deriveBrandedResidentialCapable({
    caseStudyRows: [row({ hotel_type: "Branded Residences / Condo Hotel" })],
  }),
  true,
  "case study proof"
);

assert.equal(
  deriveBrandedResidentialCapable({}),
  false,
  "empty profile"
);

console.log("test-operator-branded-residence-capable: ok");
