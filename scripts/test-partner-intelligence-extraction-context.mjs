#!/usr/bin/env node
/**
 * Brand extraction context guardrails — wrong-brand fallback prevention.
 */
import assert from "node:assert/strict";
import {
  resolveBrandExtractionContext,
  isWrongBrandIdentityLeak,
  buildIdentityFieldHint,
} from "../lib/partner-intelligence/brand-extraction-context.js";
import { extractBrandFactsFromText } from "../lib/partner-intelligence/brand-extract-rules.js";
import { getBrandFieldHints } from "../lib/partner-intelligence/brand-field-extraction-hints.js";

const CURIO_ID = "receQkxgjlezsc1xg";
const KIMPTON_ID = "recCKuXCmGvxHPfb3";

const identityFields = [
  {
    fieldKey: "be.identity.brandName",
    explorerSection: "Identity",
    displayLabel: "Brand Name",
    sourceRoles: ["any"],
  },
  {
    fieldKey: "be.identity.parentCompany",
    explorerSection: "Identity",
    displayLabel: "Parent Company",
    sourceRoles: ["any"],
  },
];

// 1. Curio target context must not receive Kimpton/IHG identity fallback
{
  const ctx = resolveBrandExtractionContext({ brandId: CURIO_ID });
  assert.equal(ctx.pilotKey, "curioCollection");
  const text = "Curio Collection by Hilton franchise disclosure document excerpt.";
  const facts = extractBrandFactsFromText(text, identityFields, {
    sourceRole: "fdd",
    localFilePath: "Hilton/fdd/2025 US Curio FDD.pdf",
    brandContext: ctx,
  });
  const brand = facts.find((f) => f.fieldKey === "be.identity.brandName");
  const parent = facts.find((f) => f.fieldKey === "be.identity.parentCompany");
  assert.ok(!/kimpton/i.test(brand?.extractedValue || ""));
  assert.ok(!/\bihg\b/i.test(parent?.extractedValue || ""));
  assert.ok(
    /curio/i.test(brand?.extractedValue || "") || brand?.dataGap === "Yes",
    "brandName should be Curio or gap"
  );
}

// 2. Wrong-brand registry fallback is skipped on Curio when hints are Kimpton-only
{
  const ctx = resolveBrandExtractionContext({ brandId: CURIO_ID });
  const kimptonHints = getBrandFieldHints("be.identity.brandName", {
    pilotKey: "kimptonHotels",
    brandName: "Kimpton Hotels",
  });
  assert.equal(kimptonHints?.fixedValue, "Kimpton Hotels");

  const curioHints = getBrandFieldHints("be.identity.brandName", ctx);
  assert.equal(curioHints?.fixedValue, "Curio Collection by Hilton");

  const text = "Generic Hilton franchise filing with no brand-specific identity strings.";
  const facts = extractBrandFactsFromText(text, identityFields, {
    sourceRole: "fdd",
    brandContext: ctx,
  });
  const brand = facts.find((f) => f.fieldKey === "be.identity.brandName");
  assert.ok(!/kimpton/i.test(brand?.extractedValue || ""));
  if (brand?._registryFallback) {
    assert.match(brand.extractedValue, /Curio/i);
    assert.match(brand.evidenceText, /Curio Collection by Hilton/);
  }
}

// 3. Valid matching Kimpton context can still supply identity fallback
{
  const ctx = resolveBrandExtractionContext({ brandId: KIMPTON_ID });
  assert.equal(ctx.pilotKey, "kimptonHotels");
  const text = "Kimpton Hotels development brochure excerpt.";
  const facts = extractBrandFactsFromText(text, identityFields, {
    sourceRole: "development_brochure",
    brandContext: ctx,
  });
  const brand = facts.find((f) => f.fieldKey === "be.identity.brandName");
  const parent = facts.find((f) => f.fieldKey === "be.identity.parentCompany");
  assert.equal(brand?.extractedValue, "Kimpton Hotels");
  assert.equal(parent?.extractedValue, "IHG Hotels & Resorts");
}

// 4. Missing/ambiguous brand context does not apply identity fallback
{
  const ctx = resolveBrandExtractionContext({});
  assert.equal(ctx.resolved, false);
  const text = "Kimpton Hotels and IHG Hotels & Resorts mentioned.";
  const facts = extractBrandFactsFromText(text, identityFields, {
    sourceRole: "fdd",
    brandContext: ctx,
  });
  const brand = facts.find((f) => f.fieldKey === "be.identity.brandName");
  assert.equal(brand?.dataGap, "Yes");
  assert.ok(!brand?._registryFallback);
}

// 5. Wrong-brand leak detector blocks Kimpton/IHG on Curio
{
  const ctx = resolveBrandExtractionContext({ brandId: CURIO_ID });
  assert.ok(isWrongBrandIdentityLeak("be.identity.brandName", "Kimpton Hotels", ctx));
  assert.ok(isWrongBrandIdentityLeak("be.identity.parentCompany", "IHG Hotels & Resorts", ctx));
  assert.ok(!isWrongBrandIdentityLeak("be.identity.brandName", "Curio Collection by Hilton", ctx));
}

// Footprint pattern should not capture FDD date fragment "Global 06 January"
{
  const ctx = resolveBrandExtractionContext({ brandId: CURIO_ID });
  const footprintField = {
    fieldKey: "be.footprint.globalHotels",
    explorerSection: "Footprint",
    displayLabel: "Global Hotel Count",
    sourceRoles: ["fdd"],
  };
  const text =
    "EXHIBIT H-1 Curio - Hotels - Brand Standards - Global 06 January 2025 Curio - Hotels Standards";
  const facts = extractBrandFactsFromText(text, [footprintField], {
    sourceRole: "fdd",
    brandContext: ctx,
  });
  const fp = facts[0];
  assert.notEqual(fp?.extractedValue, "06");
  assert.equal(fp?.dataGap, "Yes");
}

// buildIdentityFieldHint uses pilot metadata
{
  const ctx = resolveBrandExtractionContext({ brandId: CURIO_ID });
  const hint = buildIdentityFieldHint("be.identity.brandName", ctx);
  assert.equal(hint.fixedValue, "Curio Collection by Hilton");
  assert.equal(hint.pilotKey, "curioCollection");
}

console.log("test-partner-intelligence-extraction-context: ok");
