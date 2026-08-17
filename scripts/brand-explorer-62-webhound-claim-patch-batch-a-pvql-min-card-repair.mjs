#!/usr/bin/env node
/**
 * Batch A PVQL repair: restore/soften remaining min-card + dated-card blockers.
 */
import "../load-env.js";
import fs from "node:fs";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REPORT_JSON =
  "reports/brand-explorer/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.json";

const REPAIRS = [
  {
    recordId: "reckHvQ7dCaKENpFT",
    brandSlug: "avid-hotels",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "avid hotels — IHG Development prototype reference · International Reference",
      Body: [
        "Directory",
        "International Reference. IHG Development materials describe avid as an essentials-led midscale prototype path (directory reference as of 2025). Use as brand-prototype context for owner comparison — not a dated CALA opening, signing, or pipeline announcement.",
        "https://development.ihg.com/hotel-brands/avid-hotels",
      ].join("\n\n"),
    },
  },
  {
    recordId: "rec1i5YqWebagyDeh",
    brandSlug: "city-express-by-marriott",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "City Express by Marriott brand footprint reference · CALA",
      Body: [
        "Directory",
        "CALA. Official City Express brand page describes midscale central-city stays across Mexico and broader Americas markets (directory reference as of 2025). Use as regional brand-footprint context for owner comparison — not a dated opening, signing, or country-entry announcement.",
        "https://www.marriott.com/brands/city-express.mi",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recc6Po0iBZYdOphS",
    brandSlug: "city-express-by-marriott",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "City Express by Marriott Cancun Aeropuerto — CALA property proof",
      Body: [
        "Directory",
        "CALA · Cancun, Mexico. Labeled property proof for City Express by Marriott Cancun Aeropuerto (directory reference as of 2025). Use for owner review of airport-adjacent midscale product — not a dated opening, signing, or pipeline announcement.",
        "https://www.marriott.com/en-us/hotels/cunxa-city-express-cancun-aeropuerto/overview/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recVZcLOwNQ5qHIq0",
    brandSlug: "courtyard-by-marriott",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "Courtyard by Marriott Cancun Airport — CALA property proof",
      Body: [
        "Directory",
        "CALA · Cancun, Mexico. Labeled property proof for Courtyard by Marriott Cancun Airport near airport demand generators (directory reference as of 2025). Use for owner review of Marriott select-service product — not a dated opening, signing, or pipeline announcement.",
        "https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun-airport/overview/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "rec1tQU8zejo8OjYM",
    brandSlug: "holiday-inn-express",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "Holiday Inn Express Mexico Aeropuerto — CALA property proof",
      Body: [
        "Directory",
        "CALA · Mexico City, Mexico. Labeled property proof for Holiday Inn Express Mexico Aeropuerto (directory reference as of 2025). Use for owner review of essentials select-service product — not a dated opening, signing, or pipeline announcement.",
        "https://www.ihg.com/holidayinnexpress/hotels/us/en/mexico-city/mexai/hoteldetail",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recO6VCqzJPueAU93",
    brandSlug: "holiday-inn-express",
    fields: {
      Active: true,
      "External Display Status": null,
      Title: "Holiday Inn Express & Suites Bogota DC — CALA property proof",
      Body: [
        "Directory",
        "CALA · Bogota, Colombia. Labeled property proof for Holiday Inn Express & Suites Bogota DC (directory reference as of 2025). Use for owner review of essentials select-service product — not a dated opening, signing, or pipeline announcement.",
        "https://www.ihg.com/holidayinnexpress/hotels/us/en/bogota/bogbo/hoteldetail",
      ].join("\n\n"),
    },
  },
  {
    recordId: "reclDnv9FQnuUJ4Hj",
    brandSlug: "doubletree-by-hilton",
    fields: {
      Active: true,
      "External Display Status": null,
      Body: [
        "Directory",
        "CALA · Buenos Aires, Argentina. Labeled property proof (directory reference as of 2026) for owner review of DoubleTree positioning in a CALA gateway market — not a dated opening, signing, or pipeline announcement. Confirm current brand status on the official Hilton property page.",
        "https://www.hilton.com/en/hotels/buesidt-doubletree-buenos-aires/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recZFXbjHrsFUOvvD",
    brandSlug: "doubletree-by-hilton",
    fields: {
      Body: [
        "Directory",
        "CALA · Lima, Peru. Labeled property proof (directory reference as of 2026) for owner review of DoubleTree positioning in an Andean gateway market — not a dated opening, signing, or pipeline announcement. Confirm current brand status on the official Hilton property page.",
        "https://www.hilton.com/en/hotels/limaldt-doubletree-lima-san-isidro/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recVDdjfzFVn12PQP",
    brandSlug: "homewood-suites-by-hilton",
    fields: {
      Active: true,
      "External Display Status": null,
      Body: [
        "Directory",
        "International Reference · Nashville, Tennessee, USA. Labeled property proof (directory reference as of 2026) for owner review of Homewood residential suite product and Hilton Honors reach for longer stays — not a dated opening, signing, conversion, or pipeline announcement. Confirm current brand status on the official Hilton property page.",
        "https://www.hilton.com/en/hotels/bnadwhw-homewood-suites-nashville-downtown/",
      ].join("\n\n"),
    },
  },
  {
    recordId: "recPJL8r7DVLoIZTN",
    brandSlug: "homewood-suites-by-hilton",
    fields: {
      Body: [
        "Directory",
        "International Reference · Miami, Florida, USA. Labeled property proof (directory reference as of 2026) for owner review of Homewood Suites extended-stay product, kitchens, and social breakfast/evening programming — not a dated opening announcement or CALA claim. Confirm current brand status on the official Hilton property page.",
        "https://www.hilton.com/en/hotels/miadbhw-homewood-suites-miami-downtown-brickell/",
      ].join("\n\n"),
    },
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function patch(baseId, token, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${recordId}: ${json.error?.message || res.status}`);
  return json;
}

async function main() {
  const argv = process.argv.slice(2);
  if (!argv.includes("--apply") || !argv.includes("--confirm-batch-a-pvql-min-card-repair")) {
    console.error("Require --apply --confirm-batch-a-pvql-min-card-repair");
    process.exit(2);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const results = [];
  for (const r of REPAIRS) {
    await patch(baseId, token, r.recordId, r.fields);
    results.push({ recordId: r.recordId, brandSlug: r.brandSlug, status: "patched" });
    console.log("patched", r.brandSlug, r.recordId);
    await sleep(320);
  }

  const report = JSON.parse(fs.readFileSync(REPORT_JSON, "utf8"));
  report.pvqlMinCardRepair = {
    appliedAt: new Date().toISOString(),
    reason:
      "Restore/soften Batch A rows that dropped brands below Recent Momentum min cards / dated-card section-pattern floor after initial hide.",
    results,
  };
  report.summary.airtableWrites = (report.summary.airtableWrites || 0) + results.length;
  report.summary.itemsSoftened = (report.summary.itemsSoftened || 0) + results.length;
  report.status =
    "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_complete_ready_for_batch_b_review";
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ patched: results.length }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
