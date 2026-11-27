#!/usr/bin/env node
/**
 * Regression: Sources & Evidence SUPPORTS column must use customer-facing labels.
 * Forbid Airtable record IDs, HI graph jargon, and relationship-edge terminology.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  customerSupportsLabel,
  containsForbiddenSupportsTerminology,
  isGuestSentimentOnlyCitation,
  FORBIDDEN_SUPPORTS_PATTERNS,
} from "../lib/hotel-intelligence/sources/he-customer-supports-label-v1.js";
import { projectEvidenceToSourcesCitations } from "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import { HE_CANONICAL_JS } from "../lib/hotel-intelligence/he-canonical-explorer-assets-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const read = (rel) => fs.readFileSync(path.join(root, rel), "utf8");

const RECORD_ID = "recLuxvwwxID7U2B8";
const BAD_REGISTER_CLAIM = `Validated HI graph ownership edges for ${RECORD_ID}`;

// --- Unit: scrub + map -------------------------------------------------------
{
  const bad = customerSupportsLabel({
    claim: BAD_REGISTER_CLAIM,
    supports: "ownership",
  });
  assert.equal(bad, "Hotel Ownership Structure");
  assert.equal(containsForbiddenSupportsTerminology(bad), false);
  assert.equal(containsForbiddenSupportsTerminology(BAD_REGISTER_CLAIM), true);
}

{
  const legal = customerSupportsLabel({
    claims: ["property_company: Marbeth Acquisition LLC"],
    supports: "ownership",
  });
  assert.equal(legal, "Legal Property Ownership");

  const economic = customerSupportsLabel({
    claims: ["economic_owner: Mullen Real Estate Capital"],
    supports: "ownership",
  });
  assert.equal(economic, "Economic Sponsorship");

  const mgmt = customerSupportsLabel({
    claims: ["asset_agreement_profile: Managed by MHM"],
    supports: "ownership",
  });
  assert.equal(mgmt, "Hotel Management");

  const history = customerSupportsLabel({
    claims: ["property_history: 2019 sale announced"],
    supports: "history",
  });
  assert.equal(history, "Property Acquisition");

  const saleAndMgmt = customerSupportsLabel({
    claims: [
      "property_history / asset_agreement_profile: Sale announced; continues managed by Marriott International",
    ],
    supports: "history",
  });
  assert.ok(saleAndMgmt.includes("Property Acquisition"));
  assert.ok(saleAndMgmt.includes("Hotel Management"));

  const multi = customerSupportsLabel({
    claims: [
      "property_company: Marbeth Acquisition LLC",
      "economic_owner: Mullen Real Estate Capital",
    ],
    supports: "ownership",
  });
  assert.ok(multi.includes("Legal Property Ownership"));
  assert.ok(multi.includes("Economic Sponsorship"));
  assert.notEqual(legal, economic);
  assert.notEqual(legal, mgmt);
}

{
  const leadership = customerSupportsLabel({
    supports: "contact_research",
    title: "General Manager LinkedIn",
  });
  assert.equal(leadership, "Hotel Leadership");

  // Title cue must win over coarse supports:"ownership" when claims are absent.
  const linkedInOwnershipBucket = customerSupportsLabel({
    supports: "ownership",
    title: "LinkedIn — Norma Garcia, Directora de Ventas Krystal Grand Puerto Vallarta",
  });
  assert.equal(linkedInOwnershipBucket, "Hotel Leadership");

  const brand = customerSupportsLabel({
    supports: "brand_census",
    claim: "Census identity fields for brand affiliation and management company",
    claims: ["brand:affiliation", "operator:management company"],
  });
  assert.ok(/Brand Affiliation|Hotel Management/.test(brand));
}

{
  const neutral = customerSupportsLabel({
    title: "Unclassified press clipping",
    excerpt: "A general article mentioning the hotel.",
  });
  assert.equal(neutral, "Property Research Evidence");
  assert.equal(containsForbiddenSupportsTerminology(neutral), false);
}

{
  assert.equal(
    isGuestSentimentOnlyCitation({ supports: "guest_sentiment", category: "sentiment" }),
    true
  );
  assert.equal(
    isGuestSentimentOnlyCitation({ supports: "ownership", claims: ["economic_owner: X"] }),
    false
  );
}

// --- Projection → customer labels (no shared register overwrite) -------------
{
  const projected = projectEvidenceToSourcesCitations([
    {
      sourceUrl: "https://example.com/sdat",
      sourceName: "SDAT",
      sourceType: "Government Record",
      fieldName: "property_company",
      valueObserved: "Marbeth Acquisition LLC",
      evidenceSnippet: "Legal owner filing",
      current: true,
      airtableId: RECORD_ID,
    },
    {
      sourceUrl: "https://example.com/mrec",
      sourceName: "MREC",
      sourceType: "Company Website",
      fieldName: "economic_owner",
      valueObserved: "Mullen Real Estate Capital",
      evidenceSnippet: "Sponsor portfolio",
      current: true,
      airtableId: "recOtherOwner01",
    },
    {
      sourceUrl: "https://example.com/sale",
      sourceName: "Hotel News",
      sourceType: "Trade Press",
      fieldName: "property_history",
      valueObserved: "Sale announced 2019",
      current: false,
      airtableId: "recHistory0001",
    },
  ]);

  const labels = projected.map((s) => customerSupportsLabel(s));
  assert.deepEqual(
    new Set(labels),
    new Set([
      "Legal Property Ownership",
      "Economic Sponsorship",
      "Property Acquisition",
    ])
  );
  for (const label of labels) {
    assert.equal(containsForbiddenSupportsTerminology(label), false);
    assert.ok(!/\brec[a-zA-Z0-9]{10,}\b/i.test(label));
  }
  // Must not collapse every source to one generic ownership string.
  assert.ok(new Set(labels).size >= 3);
}

// --- Wiring: shared renderer + canonical assets + no leaked API claim --------
{
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("resolveCustomerSupportsLabel"));
  assert.ok(ws.includes("source_claim"));
  assert.ok(ws.includes("isGuestSentimentEnabled()"));
  // Must not overwrite every citation claim with the shared register claim only.
  assert.ok(/perSourceClaim/.test(ws));
  assert.ok(ws.includes("HeCustomerSupportsLabel"));

  const adapter = read("public/js/he-customer-supports-label-v1.js");
  assert.ok(adapter.includes("customerSupportsLabel"));
  assert.ok(adapter.includes("isGuestSentimentOnlyCitation"));

  assert.ok(
    HE_CANONICAL_JS.includes("/js/he-customer-supports-label-v1.js"),
    "canonical HE JS must load customer SUPPORTS adapter before workspace"
  );
  const wsIdx = HE_CANONICAL_JS.indexOf("/js/hotel-intelligence-workspace.js");
  const labelIdx = HE_CANONICAL_JS.indexOf("/js/he-customer-supports-label-v1.js");
  assert.ok(labelIdx >= 0 && labelIdx < wsIdx);

  const radar = read("public/deal-capture-radar-with-ranked-list.html");
  assert.ok(radar.includes("he-customer-supports-label-v1.js"));

  const api = read("api/golden-demo-ownership.js");
  assert.ok(!api.includes("Validated HI graph ownership edges for"));
  assert.ok(!api.includes("HI graph evidence and relationship edges for"));
}

// --- Forbidden pattern catalogue stays non-empty -----------------------------
assert.ok(FORBIDDEN_SUPPORTS_PATTERNS.length >= 5);

console.log("test-he-customer-supports-label-v1: PASS");
