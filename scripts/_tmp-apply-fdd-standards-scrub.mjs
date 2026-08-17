#!/usr/bin/env node
/**
 * Targeted FDD scrub for operations.standards_philosophy Body fields.
 * Body-only Presentation patches. No CV / Source / Registry / Brand Status / release / image writes.
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { scrubResidualOwnerFacingCopy } from "../lib/partner-intelligence/brand-explorer-residual-owner-copy-remediation.js";
import { evaluateExternalOwnerReadinessRule } from "../lib/partner-intelligence/brand-explorer-external-owner-readiness-rules.js";
import { scanOwnerFacingForbiddenLanguage, isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");

const TARGETS = Object.freeze([
  { slug: "tribute-portfolio", brandId: "recCvV0PuZOi8c3hC" },
  { slug: "vignette-collection", brandId: "recDwzv86TWnz2gGB" },
  { slug: "bw-premier-collection", brandId: "recwXZ5gVZ8ZH8ekA" },
  { slug: "bw-signature-collection", brandId: "recdeh1NsP4gjrv80" },
  { slug: "preferred-hotels-and-resorts", brandId: "recwl5JOYxlChuCAr" },
  { slug: "radisson-blu", brandId: "recWPEvxBQxVVzSq3" },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

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

async function airtablePatch({ baseId, apiKey, table, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json();
  if (!res.ok) {
    throw new Error(`Airtable PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  }
  return json;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const planned = [];
  for (const t of TARGETS) {
    const brand = await fetchBrand(t.brandId);
    const rows = (brand.brandExplorer?.blocks || []).filter(
      (r) =>
        r.slotKey === "operations.standards_philosophy" &&
        isOwnerFacingPresentationRow(r) &&
        typeof r.body === "string" &&
        /\bFDD\b/i.test(r.body)
    );
    for (const r of rows) {
      const scrub = scrubResidualOwnerFacingCopy(r.body, {
        slotKey: r.slotKey,
        brandSlug: t.slug,
      });
      if (!scrub.changed || !scrub.clean) {
        console.warn(`skip unclean ${t.slug} ${r.recordId}`, scrub.remainingForbidden);
        continue;
      }
      planned.push({
        slug: t.slug,
        recordId: r.recordId,
        table: PRESENTATION_TABLE,
        fields: { Body: scrub.after },
        beforeSnippet: r.body.match(/[^\n]*FDD[^\n]*/i)?.[0] || "",
        afterSnippet: scrub.after.match(/[^\n]*commercial agreement materials[^\n]*/i)?.[0] || "",
      });
    }
  }

  console.log(`[fdd-standards-scrub] dryRun=${!APPLY} patches=${planned.length}`);
  for (const p of planned) {
    console.log(`  ${p.slug} ${p.recordId}: ${p.beforeSnippet} → ${p.afterSnippet}`);
  }

  if (!APPLY) {
    console.log("Re-run with --apply to write Body patches only.");
    return;
  }

  const results = [];
  for (const p of planned) {
    const json = await airtablePatch({
      baseId,
      apiKey,
      table: p.table,
      recordId: p.recordId,
      fields: p.fields,
    });
    results.push({ slug: p.slug, recordId: p.recordId, id: json.id, fields: ["Body"] });
    console.log(`  PATCHED ${p.slug} ${p.recordId}`);
  }

  console.log("\nPost-apply readiness:");
  for (const t of TARGETS) {
    const brand = await fetchBrand(t.brandId);
    const ownerRows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
    const rule = evaluateExternalOwnerReadinessRule(ownerRows);
    const forbid = scanOwnerFacingForbiddenLanguage(ownerRows);
    console.log(
      `  ${t.slug}: display=${brand.brandExplorerDisplayState} srf=${brand.shouldRenderFullProfile} externalPass=${rule.pass} forbid=${forbid.map((h) => h.id).join(",") || "-"} blockers=${(brand.brandExplorerDisplayBlockers || []).join(",")}`
    );
  }

  console.log(JSON.stringify({ applied: true, results }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
