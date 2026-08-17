#!/usr/bin/env node
/**
 * Public Profile Stabilization — Round 2 residual apply.
 * Clears remaining PVQL defects after field-gate pack apply.
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import {
  ROUND2_PRESENTATION_BY_SLUG,
  ROUND2_OPENINGS_BY_RECORD,
  ROUND2_BASICS_BY_SLUG,
  scrubConversionFriendlyText,
} from "../lib/partner-intelligence/brand-explorer-public-profile-stabilization-round2.js";
import {
  STABILIZATION_BRAND_IDENTITY,
  PRESENTATION_TABLE,
} from "../lib/partner-intelligence/brand-explorer-public-profile-stabilization.js";
import { scanForbiddenLanguage } from "../lib/partner-intelligence/brand-explorer-v40b-copy-quality-patterns.js";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const ROUND2_SLUGS = [
  "kimpton",
  "design-hotels",
  "ascend",
  "comfort-inn-suites",
  "curio-collection",
  "tribute-portfolio",
];

const REQUIRED_FLAGS = [
  "--approve-public-profile-stabilization-round2",
  "--confirm-public-full-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-release-field-changes",
  "--confirm-presentation-and-basics-audience-only",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}
function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

async function fetchBrand(slug) {
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
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  return res.payload.brand;
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `${method} ${table} ${recordId}: ${res.status}`);
  }
  return json;
}

function planBrand(brand, slug) {
  const patches = [];
  const meta = STABILIZATION_BRAND_IDENTITY[slug];
  const rows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const bySlot = new Map();
  for (const r of rows) {
    if (!bySlot.has(r.slotKey)) bySlot.set(r.slotKey, []);
    bySlot.get(r.slotKey).push(r);
  }

  // 1) Content pack rows (similar / standards / growth_fit / lifecycle)
  const pack = ROUND2_PRESENTATION_BY_SLUG[slug] || [];
  const slotIndex = new Map();
  for (const item of pack) {
    const idx = slotIndex.get(item.slotKey) || 0;
    slotIndex.set(item.slotKey, idx + 1);
    const existing = bySlot.get(item.slotKey) || [];
    const primary = existing[idx] || null;
    const body = scrubConversionFriendlyText(item.body);
    const title = scrubConversionFriendlyText(item.title || "");
    const forbidden = scanForbiddenLanguage([title, body].join("\n"));
    if (forbidden.length) continue;
    if (primary?.recordId) {
      if (nz(primary.body) === body && (!title || nz(primary.title) === title)) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: primary.recordId,
        brandSlug: slug,
        slotKey: item.slotKey,
        fields: { Body: body, ...(title ? { Title: title } : {}) },
        reason: "round2_presentation_fill",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        brandSlug: slug,
        slotKey: item.slotKey,
        fields: {
          "Slot Key": item.slotKey,
          "Brand Name": brand.name || meta?.name || slug,
          Brand: [brand.id || meta.recordId],
          Active: true,
          "Sort Order": item.sortOrder ?? 20 + idx,
          Title: title || "",
          Body: body,
        },
        reason: "round2_presentation_create",
      });
    }
  }

  // 2) Scrub conversion-friendly on all owner-facing rows
  for (const r of rows) {
    if (!r.recordId) continue;
    const beforeTitle = nz(r.title);
    const beforeBody = nz(r.body);
    const afterTitle = scrubConversionFriendlyText(beforeTitle);
    const afterBody = scrubConversionFriendlyText(beforeBody);
    if (afterTitle === beforeTitle && afterBody === beforeBody) continue;
    // Avoid double-patching same record if already in pack
    if (patches.some((p) => p.recordId === r.recordId)) {
      const p = patches.find((x) => x.recordId === r.recordId);
      if (p.fields.Body) p.fields.Body = scrubConversionFriendlyText(p.fields.Body);
      if (p.fields.Title) p.fields.Title = scrubConversionFriendlyText(p.fields.Title);
      continue;
    }
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: r.recordId,
      brandSlug: slug,
      slotKey: r.slotKey,
      fields: {
        ...(afterTitle !== beforeTitle ? { Title: afterTitle } : {}),
        ...(afterBody !== beforeBody ? { Body: afterBody } : {}),
      },
      reason: "round2_scrub_conversion_friendly",
    });
  }

  // 3) Deepen thin openings (<30 words)
  for (const r of rows.filter((x) => x.slotKey === "footprint.openings")) {
    const curated = ROUND2_OPENINGS_BY_RECORD[r.recordId];
    let body = curated || nz(r.body);
    if (!curated && words(body) < 30) {
      const title = nz(r.title) || "Property example";
      body = `${body}\n${title} illustrates a directional ${brand.name || slug} affiliation path. Confirm PIP scope, operator capacity, and participation costs directly—this card is geography and product context, not a fee schedule or performance forecast.`;
    }
    body = scrubConversionFriendlyText(body);
    if (words(body) < 30) continue;
    if (nz(r.body) === body) continue;
    const existing = patches.find((p) => p.recordId === r.recordId);
    if (existing) {
      existing.fields.Body = body;
      existing.reason = "round2_openings_depth";
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: r.recordId,
        brandSlug: slug,
        slotKey: "footprint.openings",
        fields: { Body: body },
        reason: "round2_openings_depth",
      });
    }
  }

  // 4) Basics audience / stub scrub
  const basics = ROUND2_BASICS_BY_SLUG[slug];
  if (basics && Object.keys(basics).length) {
    const fields = {};
    for (const [k, v] of Object.entries(basics)) {
      const cleaned = scrubConversionFriendlyText(v);
      if (scanForbiddenLanguage(cleaned).length) continue;
      fields[k] = cleaned;
    }
    if (Object.keys(fields).length) {
      patches.push({
        table: BASICS_TABLE,
        action: "PATCH",
        recordId: brand.id || meta.recordId,
        brandSlug: slug,
        slotKey: null,
        fields,
        reason: "round2_basics_audience_stub_scrub",
      });
    }
  }

  return patches.filter((p) => Object.keys(p.fields || {}).length > 0);
}

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  if (apply) {
    const missing = REQUIRED_FLAGS.filter((f) => !argv.includes(f));
    if (missing.length) {
      console.error("Missing flags:", missing.join(", "));
      process.exit(1);
    }
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const all = [];
  for (const slug of ROUND2_SLUGS) {
    const brand = await fetchBrand(slug);
    const patches = planBrand(brand, slug);
    all.push({ slug, patches });
    console.log(`${slug}: ${patches.length} patches`);
  }

  const total = all.reduce((n, b) => n + b.patches.length, 0);
  console.log(`Total patches: ${total} apply=${apply}`);

  if (!apply) {
    for (const b of all) {
      for (const p of b.patches.slice(0, 8)) {
        console.log(
          `  ${b.slug} ${p.action} ${p.table} ${p.slotKey || "basics"} ${p.recordId || "create"} — ${p.reason}`
        );
      }
      if (b.patches.length > 8) console.log(`  … +${b.patches.length - 8} more`);
    }
    return;
  }

  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  let applied = 0;
  for (const b of all) {
    for (const p of b.patches) {
      await airtableWrite({
        baseId,
        apiKey,
        table: p.table,
        recordId: p.recordId,
        fields: p.fields,
        method: p.action === "POST" ? "POST" : "PATCH",
      });
      applied++;
    }
  }
  console.log(`Applied ${applied} patches`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
