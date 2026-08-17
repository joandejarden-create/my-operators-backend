#!/usr/bin/env node
/**
 * Final residual Basics + presentation patches for BW Premier/Signature and Preferred gate cleanup.
 * Allowed: Brand Basics visible owner copy + Presentation Body thicken.
 * No CV / Source / Registry / Brand Status / release / image writes.
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");
const BASICS_TABLE = "Brand Setup - Brand Basics";
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
  if (!res.payload?.brand) throw new Error(`fetch failed ${brandId}`);
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
  if (!res.ok) throw new Error(`PATCH ${table} ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  const basicsPatches = [
    {
      slug: "bw-premier-collection",
      recordId: "recwXZ5gVZ8ZH8ekA",
      fields: {
        "Guest Psychographics Description":
          "Travelers seeking an elevated independent hotel experience with distinctive design, stronger public spaces, and upscale service cues—while still valuing BWH platform distribution and loyalty reach.",
      },
    },
    {
      slug: "bw-signature-collection",
      recordId: "recdeh1NsP4gjrv80",
      fields: {
        "Guest Psychographics Description":
          "Travelers seeking a flexible soft-brand stay with independent character, practical upscale comfort, and BWH platform access—without assuming Premier-level collection intensity.",
      },
    },
    {
      slug: "preferred-hotels-and-resorts",
      recordId: "recwl5JOYxlChuCAr",
      fields: {
        "Brand Value Proposition":
          "Preferred Hotels; independent luxury; flexible affiliation; retain property identity.",
        "Guest Psychographics Description":
          "Independent-minded luxury and upscale travelers who choose hotels for distinctive character, service quality, and destination fit rather than a standardized chain prototype.",
        "Target Guest Segments": ["Experience-Oriented", "Leisure"],
      },
    },
  ];

  const preferred = await fetchBrand("recwl5JOYxlChuCAr");
  const owner = (preferred.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const presentationPatches = [];
  const thicken = {
    "valueOwners.lifecycle.3":
      "Coordinate commercial launch planning, property content, channel setup, training, and operator responsibilities with the applicable Preferred program. Confirm timing, technical requirements, and owner versus operator accountabilities before opening so representation and operations launch together cleanly.",
    "valueOwners.lifecycle.5":
      "Review guest feedback, channel mix, and commercial activity against the hotel's own target segment and competitive set. Adjust programming, sales focus, and service execution without assuming that collection participation alone determines ramp-up performance outcomes.",
    "economics.opening.step.4":
      "Coordinate commercial activation with an operating launch that delivers the promised independent experience. Resolve content, channel, guest-program, and service-readiness issues before relying on broader representation to carry opening demand.",
  };
  for (const [slot, body] of Object.entries(thicken)) {
    const row = owner.find((r) => r.slotKey === slot);
    if (row) {
      presentationPatches.push({
        slug: "preferred-hotels-and-resorts",
        recordId: row.recordId,
        fields: { Body: body },
      });
    }
  }

  console.log(`[basics-residual] dryRun=${!APPLY}`);
  console.log("basics", basicsPatches.map((p) => ({ slug: p.slug, fields: Object.keys(p.fields) })));
  console.log("presentation", presentationPatches.map((p) => p.recordId));

  if (!APPLY) return;

  for (const p of basicsPatches) {
    await airtablePatch({
      baseId,
      apiKey,
      table: BASICS_TABLE,
      recordId: p.recordId,
      fields: p.fields,
    });
    console.log(`PATCHED basics ${p.slug}`);
  }
  for (const p of presentationPatches) {
    await airtablePatch({
      baseId,
      apiKey,
      table: PRESENTATION_TABLE,
      recordId: p.recordId,
      fields: p.fields,
    });
    console.log(`PATCHED presentation ${p.recordId}`);
  }

  for (const slug of [
    "bw-premier-collection",
    "bw-signature-collection",
    "preferred-hotels-and-resorts",
  ]) {
    const row = await evaluateBrandPublicVisibility(slug);
    console.log(
      `${slug}: pf=${row.publicFullProfile} display=${row.publicDisplayState} fails=${(row.failures || []).join(",") || "-"}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
