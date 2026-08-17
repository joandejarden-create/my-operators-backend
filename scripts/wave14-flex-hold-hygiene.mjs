/**
 * Wave 14 Flex hold hygiene — Do Not Display unsupported openings;
 * repair F&B gallery captions. Image/status fields only.
 */
import "dotenv/config";
import {
  listPresentationRowsLight,
} from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

const TOKEN = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
const BASE = process.env.AIRTABLE_BASE_ID;
const TABLE = "Brand Setup - Brand Explorer Presentation";
const dryRun = !process.argv.includes("--apply");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function patchRecord(recordId, fields) {
  const url = `https://api.airtable.com/v0/${BASE}/${encodeURIComponent(TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Airtable ${res.status}: ${t.slice(0, 400)}`);
  }
  return res.json();
}

async function main() {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES["four-points-flex-by-sheraton"];
  const { rows = [] } = await listPresentationRowsLight(identity.recordId, identity.name);
  const patches = [];

  for (const row of rows) {
    const slot = nz(row.slotKey);
    const img = nz(row.imageUrl);
    const title = nz(row.title);
    const body = nz(row.body);
    const eds = nz(row.externalDisplayStatus);

    if (slot === "footprint.openings" && !img && !/^do not display$/i.test(eds)) {
      patches.push({
        recordId: row.recordId,
        reason: "flex_openings_hold_no_image",
        fields: {
          "External Display Status": "Do Not Display",
          Active: false,
        },
      });
      continue;
    }

    if (/^materials\.gallery\.\d+$/.test(slot) && !img && !/^do not display$/i.test(eds)) {
      patches.push({
        recordId: row.recordId,
        reason: "flex_gallery_slot_unavailable",
        fields: {
          "External Display Status": "Do Not Display",
          Active: false,
        },
      });
      continue;
    }

    // Caption role repair for buffet assets labeled as Property Setting
    if (
      /^materials\.gallery\.\d+$/.test(slot) &&
      /buffet/i.test(nz(row.imageFilename) + title + body + nz(row.imageUrl)) &&
      /property setting/i.test(title)
    ) {
      patches.push({
        recordId: row.recordId,
        reason: "flex_buffet_caption_to_fb",
        fields: {
          Title: title.replace(
            /Property Setting \/ Destination Context/i,
            "F&B / Bar / Restaurant / Local Experience"
          ),
          Body: "F&B / Bar / Restaurant / Local Experience",
        },
      });
    }
  }

  console.log(
    JSON.stringify(
      { dryRun, recordId: identity.recordId, brandName: identity.name, patchCount: patches.length, patches },
      null,
      2
    )
  );
  if (dryRun || !patches.length) return;

  for (const p of patches) {
    await patchRecord(p.recordId, p.fields);
    console.log("patched", p.recordId, p.reason);
    await new Promise((r) => setTimeout(r, 250));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
