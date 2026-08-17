#!/usr/bin/env node
/**
 * Create missing valueOwners.overview Presentation rows for BW Premier + Signature.
 * Also retitle wrong "Marriott Bonvoy" carryover proof titles to BWH language.
 * Presentation Title/Body only. No CV/Source/Registry/Brand Status/release/image writes.
 */
import "dotenv/config";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const CREATES = [
  {
    slug: "bw-premier-collection",
    brandId: "recwXZ5gVZ8ZH8ekA",
    brandName: "BW Premier Collection",
    fields: {
      Active: true,
      Brand: ["recwXZ5gVZ8ZH8ekA"],
      "Brand Name": "BW Premier Collection",
      "Slot Key": "valueOwners.overview",
      Title: "What Owners Are Buying",
      Body: "BW Premier Collection gives owners of differentiated upscale independents a BWH soft-brand path that pairs property-specific identity with loyalty, distribution, and commercial infrastructure. The owner proposition is elevated collection positioning with more independent expression than a core Best Western flag—subject to product, design, systems, and quality obligations confirmed for the specific asset.",
      "Sort Order": 51,
    },
  },
  {
    slug: "bw-signature-collection",
    brandId: "recdeh1NsP4gjrv80",
    brandName: "BW Signature Collection",
    fields: {
      Active: true,
      Brand: ["recdeh1NsP4gjrv80"],
      "Brand Name": "BW Signature Collection",
      "Slot Key": "valueOwners.overview",
      Title: "What Owners Are Buying",
      Body: "BW Signature Collection gives owners of upper-midscale and upscale independents a more flexible BWH soft-brand path to keep a local hotel identity while connecting to distribution, loyalty, and commercial systems. The owner proposition is platform participation without a hard-brand prototype—generally below BW Premier Collection's more elevated design intensity, with obligations confirmed asset by asset.",
      "Sort Order": 51,
    },
  },
];

const TITLE_FIXES = {
  "bw-premier-collection": {
    "overview.proof.2": "Best Western Distribution And Loyalty",
    "overview.bestAt.3": "BWH Systems Without A Fixed Prototype",
  },
  "bw-signature-collection": {
    "overview.proof.2": "BWH Distribution And Loyalty Platform",
    "overview.bestAt.3": "BWH Systems Without A Fixed Prototype",
  },
};

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
  return res.payload?.brand;
}

async function airtableCreate({ baseId, apiKey, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`POST: ${res.status} ${JSON.stringify(json)}`);
  return json;
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
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  for (const c of CREATES) {
    const brand = await fetchBrand(c.brandId);
    const rows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
    const existing = rows.find((r) => r.slotKey === "valueOwners.overview");
    if (existing?.recordId) {
      console.log(`EXISTS valueOwners.overview ${c.slug} ${existing.recordId} — patching Body`);
      if (APPLY) {
        await airtablePatch({
          baseId,
          apiKey,
          recordId: existing.recordId,
          fields: { Title: c.fields.Title, Body: c.fields.Body },
        });
        console.log(`PATCHED ${existing.recordId}`);
      }
    } else {
      console.log(`CREATE valueOwners.overview ${c.slug}`);
      if (APPLY) {
        const created = await airtableCreate({ baseId, apiKey, fields: c.fields });
        console.log(`CREATED ${created.id}`);
      }
    }

    for (const [slot, title] of Object.entries(TITLE_FIXES[c.slug] || {})) {
      const row = rows.find((r) => r.slotKey === slot);
      if (!row?.recordId) {
        console.log(`MISSING title-fix target ${c.slug} ${slot}`);
        continue;
      }
      if (String(row.title || "") === title) {
        console.log(`OK title ${c.slug} ${slot}`);
        continue;
      }
      console.log(`TITLE ${c.slug} ${slot} ${row.recordId} -> ${title}`);
      if (APPLY) {
        await airtablePatch({ baseId, apiKey, recordId: row.recordId, fields: { Title: title } });
        console.log(`PATCHED title ${row.recordId}`);
      }
    }
  }

  if (!APPLY) {
    console.log("dry-run only; pass --apply to write");
    return;
  }

  for (const c of CREATES) {
    const row = await evaluateBrandPublicVisibility(c.slug);
    console.log(
      `${c.slug}: pf=${row.publicFullProfile} lockPass=${row.lockPass} fails=${(row.failures || []).join(",") || "-"}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
