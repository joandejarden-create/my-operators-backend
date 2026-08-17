#!/usr/bin/env node
/**
 * Unit checks for lib/profile-governance/normalize-profile-governance.js
 */
import assert from "node:assert/strict";
import { normalizeProfileGovernance } from "../lib/profile-governance/normalize-profile-governance.js";

function gov(fields, options = {}) {
  return normalizeProfileGovernance(fields, options);
}

function assertNoConfidenceInSubtitle(r) {
  assert.ok(!r.displaySubtitle?.includes("Confidence:"), "subtitle must not include confidence");
}

// blank governance → no displayLabel + internal warning
{
  const r = gov({});
  assert.equal(r.displayLabel, null);
  assert.ok(r.internalWarnings.some((w) => w.includes("Governance Not Set")));
}

// Company Validated without checkbox/date → no display + warning
{
  const r = gov({
    "Validation Status": "Company Validated",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(r.displayLabel, null);
  assert.ok(
    r.internalWarnings.some((w) => w.includes("Company Validated status requires"))
  );
}

// Company Validated with checkbox/date + Show Trust Label
{
  const r = gov({
    "Validation Status": "Company Validated",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
    "Company Validated": true,
    "Company Validation Date": "2026-05-01",
  });
  assert.equal(r.displayLabel, "Company-Validated Profile");
  assert.equal(r.companyValidationDate, "2026-05-01");
  assert.equal(r.validationStatus, "Company Validated");
  assertNoConfidenceInSubtitle(r);
}

// Company Reviewed → Company-Reviewed Profile
{
  const r = gov({
    "Validation Status": "Company Reviewed",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
    "Last Reviewed Date": "2026-05-01",
  });
  assert.equal(r.displayLabel, "Company-Reviewed Profile");
  assertNoConfidenceInSubtitle(r);
}

// Needs Review → no external label
{
  const r = gov({
    "Validation Status": "Needs Review",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(r.displayLabel, null);
  assert.ok(r.internalWarnings.some((w) => w.includes("blocks external display")));
}

// Do Not Use → no external label
{
  const r = gov({
    "Validation Status": "Do Not Use",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(r.displayLabel, null);
}

// Internal Only → no external label
{
  const r = gov({
    "Validation Status": "Source-Informed",
    "Usage Permission": "Internal Only",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(r.displayLabel, null);
  assert.ok(r.internalWarnings.some((w) => w.includes("Usage permission")));
}

// Kimpton-like: Company Published → AI-Assisted Profile (not raw Company Published)
{
  const r = gov({
    "Validation Status": "Company Published",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
    "Source Region": "Global Reference",
    "Last Reviewed Date": "2026-06-12",
    "Confidence Level": "High",
  });
  assert.equal(r.displayLabel, "AI-Assisted Profile");
  assert.notEqual(r.displayLabel, "Company Published");
  assert.equal(r.sourceBasis, "Company Materials");
  assert.equal(r.confidenceLevel, "High");
  assert.ok(r.displaySubtitle?.includes("Last Reviewed: Jun 12, 2026"));
  assert.ok(r.displaySubtitle?.includes("Source Basis: Company Materials"));
  assert.ok(r.displaySubtitle?.includes("Region: Global Reference"));
  assertNoConfidenceInSubtitle(r);
}

// Arbor-like: Source-Informed → Source-Informed Profile
{
  const r = gov(
    {
      "Validation Status": "Source-Informed",
      "Usage Permission": "Platform Display Allowed",
      "External Display Status": "Show Trust Label",
      "Source Region": "CALA-Specific",
      "Last Reviewed Date": "2026-06-10",
      "Data Confidence Level": "Medium",
    },
    { entityType: "operator", sourceTable: "Operator Setup - Master" }
  );
  assert.equal(r.displayLabel, "Source-Informed Profile");
  assert.equal(r.sourceBasis, "Reviewed Sources");
  assert.equal(r.confidenceLevel, "Medium");
  assert.ok(r.displaySubtitle?.includes("Last Reviewed: Jun 10, 2026"));
  assert.ok(r.displaySubtitle?.includes("Source Basis: Reviewed Sources"));
  assert.ok(r.displaySubtitle?.includes("Region: CALA-specific"));
  assertNoConfidenceInSubtitle(r);
}

// Operator Data Confidence Level fallback (internal only)
{
  const r = gov(
    { "Data Confidence Level": "High" },
    { entityType: "operator", sourceTable: "Operator Setup - Master" }
  );
  assert.equal(r.confidenceLevel, "High");
}

// Last Updated Date does not become Last Reviewed Date
{
  const r = gov(
    { "Last Updated Date": "2026-01-15" },
    { entityType: "operator", sourceTable: "Operator Setup - Master" }
  );
  assert.equal(r.lastReviewedDate, null);
  assert.ok(
    r.internalWarnings.some((w) => w.includes("not the same as last reviewed date"))
  );
}

// Profile Last Reviewed alias
{
  const r = gov({
    "Profile Last Reviewed": "2026-03-10",
    "Validation Status": "Source-Informed",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(r.lastReviewedDate, "2026-03-10");
  assert.ok(r.displaySubtitle?.includes("Last Reviewed"));
  assert.ok(r.displaySubtitle?.includes("Source Basis: Reviewed Sources"));
}

// AI-Assisted external label + source basis
{
  const r = gov({
    "Validation Status": "AI-Assisted",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
    "Source Region": "CALA-Specific",
  });
  assert.equal(r.displayLabel, "AI-Assisted Profile");
  assert.equal(r.sourceBasis, "AI-assisted research");
  assert.ok(r.displaySubtitle?.includes("Region: CALA-specific"));
  assertNoConfidenceInSubtitle(r);
}

// Stale / Owner-Provided — no external label (conservative)
{
  const stale = gov({
    "Validation Status": "Stale / Refresh Needed",
    "Usage Permission": "Platform Display Allowed",
    "External Display Status": "Show Trust Label",
  });
  assert.equal(stale.displayLabel, null);
}

console.log("test-profile-governance-normalizer: ok");
