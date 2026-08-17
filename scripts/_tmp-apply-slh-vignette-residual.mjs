#!/usr/bin/env node
/**
 * Residual blockers: SLH FDD scrub + Vignette openings modal case-summary fills.
 * Presentation fields only. No CV / Source / Registry / Brand Status / release / image writes.
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { scrubResidualOwnerFacingCopy } from "../lib/partner-intelligence/brand-explorer-residual-owner-copy-remediation.js";
import { evaluateExternalOwnerReadinessRule } from "../lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js";
import {
  isOwnerFacingPresentationRow,
  scanOwnerFacingForbiddenLanguage,
} from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const VIGNETTE_MODAL_FILLS = Object.freeze([
  {
    recordId: "recaBdDx1qX9eBfn3",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate heritage urban assets in CALA that need IHG distribution and One Rewards while keeping a property-specific design story under Vignette.",
      "Case Summary Owner Objective":
        "Reference for Colonial City or historic-core soft-brand underwriting—not a prototype midscale conversion play.",
      "Case Summary Interpretation":
        "Treat published listing details as directional; validate capital intensity, operating complexity, and systems cutover against the actual asset before modeling affiliation outcomes.",
    },
  },
  {
    recordId: "recd4KP2uQ1tE8o4H",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate urban independents outside the Americas that want Vignette's soft-brand posture with IHG commercial reach rather than a full lifestyle prototype rebuild.",
      "Case Summary Owner Objective":
        "Reference for established urban product with a clear guest story—not a ground-up brand-led redesign.",
      "Case Summary Interpretation":
        "Confirm local operating standards, capital gaps, and loyalty participation with brand development before using this example as an underwriting proxy.",
    },
  },
  {
    recordId: "recKM05x7t019EGTx",
    fields: {
      "Case Summary Brand Relevance":
        "Useful when owners evaluate European urban independents seeking collection identity with IHG systems without Hotel Indigo-level neighborhood prototyping.",
      "Case Summary Owner Objective":
        "Reference for design-led urban assets where ownership character stays central to the guest proposition.",
      "Case Summary Interpretation":
        "Validate conversion scope, service standards, and commercial ramp for the specific market before treating this listing as performance guidance.",
    },
  },
]);

async function fetchBrand(brandId) {
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed: ${brandId}`);
  return res.payload.brand;
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const patches = [];

  const slh = await fetchBrand("recjjSnY2opb8P4DG");
  const slhRow = (slh.brandExplorer?.blocks || []).find(
    (r) => r.slotKey === "operations.standards_philosophy" && /\bFDD\b/i.test(r.body || "")
  );
  if (slhRow) {
    const scrub = scrubResidualOwnerFacingCopy(slhRow.body, {
      slotKey: slhRow.slotKey,
      brandSlug: "small-luxury-hotels-of-the-world",
    });
    if (scrub.changed && scrub.clean) {
      patches.push({
        slug: "small-luxury-hotels-of-the-world",
        recordId: slhRow.recordId,
        fields: { Body: scrub.after },
      });
    }
  }

  for (const fill of VIGNETTE_MODAL_FILLS) {
    patches.push({
      slug: "vignette-collection",
      recordId: fill.recordId,
      fields: fill.fields,
    });
  }

  console.log(`[residual-blockers] dryRun=${!APPLY} patches=${patches.length}`);
  for (const p of patches) {
    console.log(`  ${p.slug} ${p.recordId} fields=${Object.keys(p.fields).join(",")}`);
  }

  if (!APPLY) {
    console.log("Re-run with --apply to write.");
    return;
  }

  for (const p of patches) {
    await airtablePatch({ baseId, apiKey, recordId: p.recordId, fields: p.fields });
    console.log(`  PATCHED ${p.slug} ${p.recordId}`);
  }

  for (const [slug, id] of [
    ["small-luxury-hotels-of-the-world", "recjjSnY2opb8P4DG"],
    ["vignette-collection", "recDwzv86TWnz2gGB"],
  ]) {
    const brand = await fetchBrand(id);
    const owner = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
    const rule = evaluateExternalOwnerReadinessRule(owner);
    const forbid = scanOwnerFacingForbiddenLanguage(owner);
    console.log(
      `  ${slug}: display=${brand.brandExplorerDisplayState} srf=${brand.shouldRenderFullProfile} ext=${rule.pass} forbid=${forbid.map((h) => h.id).join(",") || "-"} blockers=${(brand.brandExplorerDisplayBlockers || []).join(",")}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
