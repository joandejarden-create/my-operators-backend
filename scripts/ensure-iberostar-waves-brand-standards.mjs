/**
 * Create + link Brand Setup - Brand Standards for Iberostar Waves, then fill
 * standards fields + Additional Amenities from profiles.
 *
 *   node scripts/ensure-iberostar-waves-brand-standards.mjs --dry-run
 *   node scripts/ensure-iberostar-waves-brand-standards.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { getBrandStandardsProfile } from "./lib/brand-standards-profiles.mjs";
import {
  BRAND_STANDARDS_SELECT_COLS,
  WRITABLE_BRAND_STANDARDS_COLS,
  buildBrandStandardsFieldsFromProfile,
} from "./lib/build-brand-standards-fields.mjs";
import { getBrandAdditionalAmenities } from "./lib/brand-additional-amenities-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_NAME = "Iberostar Waves";
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_STD = "Brand Setup - Brand Standards";
const LINK_FIELD = "Brand Setup - Brand Standards";
const BRAND_LINK_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

const apply = process.argv.includes("--apply");
const dryRun = !apply;

function extractUnknownFieldName(err) {
  const msg = String(err?.message || err || "");
  const m =
    msg.match(/Unknown field name:\s*['"](.+?)['"]/i) ||
    msg.match(/Unknown field name:\s*(.+?)(?:\s|$)/i);
  return m ? m[1].trim() : null;
}

function extractInvalidFieldName(err) {
  const msg = String(err?.message || err || "");
  const m = msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) || msg.match(/Field "([^"]+)"/);
  return m ? m[1].trim() : null;
}

async function createWithPruning(base, table, fields, { typecast = true } = {}) {
  let payload = { ...fields };
  const removed = [];
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) {
      throw new Error(`No fields left to create in ${table} (removed: ${removed.join(", ")})`);
    }
    try {
      const [created] = await base(table).create([{ fields: payload }], { typecast });
      return { created, removed };
    } catch (err) {
      const bad = extractUnknownFieldName(err) || extractInvalidFieldName(err);
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`createWithPruning exceeded retries for ${table}`);
}

async function updateWithPruning(base, table, recordId, fields, { typecast = true } = {}) {
  let payload = { ...fields };
  const removed = [];
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { updated: {}, removed };
    try {
      await base(table).update(recordId, payload, { typecast });
      return { updated: payload, removed };
    } catch (err) {
      const bad = extractUnknownFieldName(err) || extractInvalidFieldName(err);
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  return { updated: {}, removed };
}

async function createLinkedStandardsRow(base, brandName, basicsId) {
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId] },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId] },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId] },
    { "Brand Name": brandName },
  ];
  let lastErr;
  for (const fields of tries) {
    try {
      const { created, removed } = await createWithPruning(base, TABLE_STD, fields, { typecast: true });
      return { created, removed, linkFieldsUsed: Object.keys(fields).filter((k) => k !== "Brand Name") };
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr || new Error("Failed to create Standards row");
}

async function getMetaChoices(baseId, apiKey) {
  let lastErr;
  for (let attempt = 0; attempt < 6; attempt++) {
    const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (r.status === 429) {
      const wait = 2000 * (attempt + 1);
      console.warn(`Meta rate-limited; retry in ${wait}ms`);
      await new Promise((res) => setTimeout(res, wait));
      lastErr = new Error(`Meta API 429`);
      continue;
    }
    if (!r.ok) throw new Error(`Meta API ${r.status}`);
    const j = await r.json();
    const t = j.tables.find((x) => x.name === TABLE_STD);
    if (!t) throw new Error(`Table not found: ${TABLE_STD}`);
    const metaFieldNames = new Set(t.fields.map((f) => f.name));
    const metaChoices = {};
    for (const col of [...BRAND_STANDARDS_SELECT_COLS, "Additional Amenities"]) {
      const f = t.fields.find((x) => x.name === col);
      metaChoices[col] = (f?.options?.choices || [])
        .map((c) => c.name)
        .filter((c) => String(c).trim() !== "");
    }
    return { metaFieldNames, metaChoices };
  }
  throw lastErr || new Error("Meta API failed");
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const base = new Airtable({ apiKey }).base(baseId);
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  const basicsRows = await base(TABLE_BASICS)
    .select({
      filterByFormula: `{Brand Name} = "${esc}"`,
      maxRecords: 1,
      fields: [
        "Brand Name",
        "Brand Status",
        "Parent Company",
        "Hotel Chain Scale",
        "Brand Model",
        "Hotel Service Model",
        "Brand Architecture",
        LINK_FIELD,
      ],
    })
    .firstPage();

  if (!basicsRows.length) throw new Error(`Brand Basics not found: ${BRAND_NAME}`);
  const basics = basicsRows[0];
  const parent = String(basics.get("Parent Company") || "").trim();
  const existingLink = basics.get(LINK_FIELD) || [];

  const { profile, resolveSource } = getBrandStandardsProfile(BRAND_NAME, parent, {
    chainScale: basics.get("Hotel Chain Scale"),
    brandModel: basics.get("Brand Model"),
    serviceModel: basics.get("Hotel Service Model"),
    architecture: basics.get("Brand Architecture"),
  });

  // Iberostar Waves is Waves-tier AI within Iberostar — use allInclusive + brand override notes
  if (profile.segment !== "allInclusive") {
    console.warn(`Unexpected segment ${profile.segment}; expected allInclusive for Iberostar Waves`);
  }

  const { metaFieldNames, metaChoices } = await getMetaChoices(baseId, apiKey);
  const proposals = [];
  const { fields: stdFields, resolved } = buildBrandStandardsFieldsFromProfile(
    {
      ...profile,
      segment: "allInclusive",
      sourceTier: "brand-override",
      qaExpectations:
        "Iberostar Waves all-inclusive QA — multi-outlet dining, Star Camp / family programming, beach/pool recreation, and brand service standards.",
      standardsNotes:
        "Iberostar Waves — family-oriented all-inclusive beachfront product. Directionally accurate brand-typical estimate for matching—not a property-specific quote. Confirm against current brand documents.",
      otherAmenitiesText:
        "Waves-tier AI amenities: multiple F&B outlets, pools, kids/family zones, beach access; confirm property prototype.",
      otherSustainabilityText:
        "Iberostar Wave of Change / property sustainability programs as applicable.",
      otherComplianceText: "All-inclusive resort life-safety and Iberostar brand QA supplements.",
    },
    metaChoices,
    proposals,
    BRAND_NAME
  );

  const { amenities, resolveSource: amenitySrc } = getBrandAdditionalAmenities(BRAND_NAME, parent);
  const amenityAllow = new Set(metaChoices["Additional Amenities"] || []);
  const amenitiesFiltered = amenities.filter((a) => amenityAllow.has(a));

  for (const k of Object.keys(stdFields)) {
    if (!metaFieldNames.has(k) || !WRITABLE_BRAND_STANDARDS_COLS.includes(k)) delete stdFields[k];
  }
  if (amenitiesFiltered.length && metaFieldNames.has("Additional Amenities")) {
    stdFields["Additional Amenities"] = amenitiesFiltered;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "apply",
    brandName: BRAND_NAME,
    basicsId: basics.id,
    status: basics.get("Brand Status") || null,
    parentCompany: parent || null,
    existingStandardsLink: existingLink,
    resolveSource,
    amenityResolveSource: amenitySrc,
    resolved,
    fieldCount: Object.keys(stdFields).length,
    fields: stdFields,
    optionProposals: proposals,
  };

  if (dryRun) {
    report.action = existingLink[0] ? "would_update_existing_link" : "would_create_and_link";
    if (existingLink[0]) report.standardsRecordId = existingLink[0];
    console.log(JSON.stringify(report, null, 2));
  } else {
    let stdId = existingLink[0] || null;
    let created = false;
    if (!stdId) {
      const { created: child, removed, linkFieldsUsed } = await createLinkedStandardsRow(
        base,
        BRAND_NAME,
        basics.id
      );
      stdId = child.id;
      created = true;
      report.createRemovedFields = removed;
      report.linkFieldsUsed = linkFieldsUsed;
      // Wire Basics → Standards link
      await base(TABLE_BASICS).update(basics.id, { [LINK_FIELD]: [stdId] });
      report.basicsLinkUpdated = true;
    }

    const { updated, removed } = await updateWithPruning(base, TABLE_STD, stdId, stdFields, {
      typecast: true,
    });
    report.action = created ? "created_linked_and_filled" : "filled_existing";
    report.standardsRecordId = stdId;
    report.fieldCountWritten = Object.keys(updated).length;
    report.fieldsWritten = updated;
    report.updateRemovedFields = removed;
    console.log(
      `${report.action} ${BRAND_NAME} std=${stdId} fields=${report.fieldCountWritten} src=${resolveSource}`
    );
  }

  const out = path.join(
    ROOT,
    "reports",
    dryRun ? "iberostar-waves-brand-standards-dry-run.json" : "iberostar-waves-brand-standards-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
