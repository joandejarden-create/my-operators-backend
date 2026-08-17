#!/usr/bin/env node
import "../load-env.js";
import fs from "node:fs";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import {
  WAVE16A_IDENTITIES,
  WAVE16A_FLEX_HOLD,
} from "../lib/partner-intelligence/brand-explorer-wave16a-factory-plan.js";

async function fetchBasics(id) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const r = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}/${id}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  return r.json();
}

function hasImage(row) {
  const img = row.image || row.imageUrl || row.Image || row.fields?.Image;
  if (Array.isArray(img)) return img.length > 0 && !!(img[0]?.url || img[0]);
  return !!img;
}

const universe = await loadActiveUniverse({ includeDetails: false });
const flex = await fetchBasics(WAVE16A_FLEX_HOLD.recordId);
const out = {
  activeUniverseAfter: universe.totalCount,
  flex: {
    recordId: WAVE16A_FLEX_HOLD.recordId,
    brandStatus: flex.fields?.["Brand Status"],
    inActive: (universe.brands || []).some((b) => b.recordId === WAVE16A_FLEX_HOLD.recordId),
  },
  brands: {},
};

for (const slug of [
  "fairfield-by-marriott",
  "four-points-by-sheraton",
  "delta-hotels-by-marriott",
]) {
  const id = WAVE16A_IDENTITIES[slug];
  const basics = await fetchBasics(id.recordId);
  const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
  const gallery = rows.filter((r) => String(r.slotKey || "").startsWith("materials.gallery."));
  const scenario = rows.filter((r) => String(r.slotKey || "").startsWith("overview.scenario."));
  const openings = rows.filter((r) => r.slotKey === "footprint.openings");
  out.brands[slug] = {
    brandStatus: basics.fields?.["Brand Status"],
    gallery: { rows: gallery.length, withImage: gallery.filter(hasImage).length },
    scenario: { rows: scenario.length, withImage: scenario.filter(hasImage).length },
    openings: { rows: openings.length, withImage: openings.filter(hasImage).length },
    imageUrlSample: [...gallery, ...scenario, ...openings]
      .filter(hasImage)
      .slice(0, 3)
      .map((r) => ({
        slotKey: r.slotKey,
        url: (r.imageUrl || r.image || r.fields?.Image?.[0]?.url || "").toString().slice(0, 120),
      })),
  };
  console.log(
    slug,
    basics.fields?.["Brand Status"],
    "g",
    out.brands[slug].gallery,
    "s",
    out.brands[slug].scenario,
    "o",
    out.brands[slug].openings
  );
}

console.log("Active after", out.activeUniverseAfter, "Flex", out.flex);
fs.writeFileSync("reports/_tmp-wave16a-stage2b-postcheck.json", JSON.stringify(out, null, 2));
