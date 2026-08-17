#!/usr/bin/env node
/**
 * Apply Choice amenities from steward-saved property page HTML files.
 * Automated Choice property fetches are blocked (403/Akamai) — save pages manually.
 *
 * Policy: fill-blank Amenities only. Never invent labels. Dry-run by default.
 *
 *   node scripts/apply-choice-amenities-from-html.mjs
 *   node scripts/apply-choice-amenities-from-html.mjs --apply
 *   node scripts/apply-choice-amenities-from-html.mjs --apply --file reports/choice-amenity-html/mx077.html
 */
import "../load-env.js";
import {
  readFileSync,
  existsSync,
  readdirSync,
  appendFileSync,
  mkdirSync,
} from "node:fs";
import { join, basename, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  parseChoiceAmenitiesFromHtml,
  choicePropertyIdFromUrl,
} from "../lib/choice-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

/** Central Airtable field map for this write path. */
export const MAP_CHOICE_AMENITIES_HTML_APPLY = {
  amenities: CENSUS_AMENITIES_TEXT_FIELD, // "Amenities"
  propertyId: CENSUS_PROPERTY_ID_FIELD, // "Property ID"
  website: "Website",
  name: CENSUS_FIELDS.name,
  parentCompany: CENSUS_FIELDS.parentCompany,
};

const APPLY = process.argv.includes("--apply");
const HTML_DIR = "reports/choice-amenity-html";
const LOG = join("reports", "choice-amenities-html-applies.csv");
const DEV_LOG = process.env.NODE_ENV !== "production";

const fileArg = process.argv.find((a) => a.startsWith("--file="))?.split("=")[1];
const fileIdx = process.argv.indexOf("--file");
const singleFile =
  fileArg ||
  (fileIdx >= 0 && process.argv[fileIdx + 1] && !process.argv[fileIdx + 1].startsWith("--")
    ? process.argv[fileIdx + 1]
    : "");

function logDev(module, payload) {
  if (!DEV_LOG) return;
  console.log(`[dev:choice-amenities-html] ${module}`, JSON.stringify(payload));
}

/**
 * @param {{ amenities: string[], amenitiesText: string, hasAmenityMarkers?: boolean, parseErrors?: string[] }} parsed
 * @param {{ fields?: Record<string, unknown>, id?: string } | null} rec
 * @param {string} filePath
 */
export function validateChoiceAmenityHtmlApply(parsed, rec, filePath) {
  /** @type {string[]} */
  const failed = [];

  if (!filePath) failed.push("missing_html_file");
  if (!parsed?.hasAmenityMarkers) failed.push("missing_amenity_markers");
  if (!Array.isArray(parsed?.amenities) || parsed.amenities.length < 1) {
    failed.push("no_amenities_parsed");
  }
  if (!parsed?.amenitiesText || !String(parsed.amenitiesText).trim()) {
    failed.push("empty_amenities_text");
  }
  if (!rec?.id) failed.push("no_census_match");
  if (rec && !isBlankCensusValue(rec.fields?.[MAP_CHOICE_AMENITIES_HTML_APPLY.amenities])) {
    failed.push("amenities_already_populated");
  }
  if (parsed?.amenities?.some((a) => typeof a !== "string" || !a.trim())) {
    failed.push("invalid_amenity_label");
  }

  const pass = failed.length === 0;
  const payload = pass
    ? { [MAP_CHOICE_AMENITIES_HTML_APPLY.amenities]: parsed.amenitiesText }
    : null;

  return {
    pass,
    failed,
    fieldMapping: {
      [MAP_CHOICE_AMENITIES_HTML_APPLY.amenities]: "parsed.amenitiesText (HTML-derived only)",
    },
    sanitizedPayloadPreview: payload,
  };
}

function listHtmlFiles() {
  if (singleFile) return [singleFile];
  if (!existsSync(HTML_DIR)) return [];
  return readdirSync(HTML_DIR, { withFileTypes: true })
    .filter(
      (d) =>
        d.isFile() &&
        /\.html?$/i.test(d.name) &&
        !d.name.startsWith("_") &&
        !d.name.startsWith(".")
    )
    .map((d) => join(HTML_DIR, d.name));
}

function findCensusForHtml(census, filePath, parsedPid) {
  const stem = basename(filePath).replace(/\.html?$/i, "").toUpperCase();
  const pid = (parsedPid || stem).toUpperCase();
  return (
    census.find(
      (r) =>
        String(r.fields[MAP_CHOICE_AMENITIES_HTML_APPLY.propertyId] || "").toUpperCase() === pid
    ) ||
    census.find(
      (r) =>
        choicePropertyIdFromUrl(r.fields[MAP_CHOICE_AMENITIES_HTML_APPLY.website]).toUpperCase() ===
        pid
    ) ||
    census.find((r) => r.id === stem)
  );
}

function summarizeCli({ mode, htmlCount, applied, dryRunnable, skipped, errors }) {
  console.log("\n── Summary ──");
  console.log(`Mode: ${mode}`);
  console.log(`HTML files: ${htmlCount}`);
  console.log(`Would apply (blank Amenities + valid parse): ${dryRunnable}`);
  if (mode === "apply") console.log(`Applied: ${applied}`);
  console.log(`Skipped: ${skipped}`);
  console.log(`Errors: ${errors}`);
}

async function main() {
  console.log("STATE: loading");
  console.log(
    APPLY
      ? "Apply mode — will write blank Amenities only after validation."
      : "Dry-run (default) — pass --apply to write. Fill-blank Amenities only."
  );
  console.log(`Field map: Amenities ← ${MAP_CHOICE_AMENITIES_HTML_APPLY.amenities}`);

  const htmlFiles = listHtmlFiles();

  if (!htmlFiles.length) {
    console.log("STATE: empty");
    console.log(`No HTML files in ${HTML_DIR}/`);
    console.log("Steward path:");
    console.log("  1. Open reports/choice-amenity-pilot-opener.html");
    console.log("  2. Save each page as reports/choice-amenity-html/{propertyId}.html");
    console.log("  3. Re-run this script (dry-run), then --apply");
    console.log(
      "Worklists: reports/choice-amenities-pilot-worklist.csv | choice-amenities-steward-worklist.csv"
    );
    return;
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.log("STATE: error");
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT in env.");
    process.exitCode = 1;
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  console.log("Loading Choice Hotel Census rows…");
  let census;
  try {
    census = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: [
          MAP_CHOICE_AMENITIES_HTML_APPLY.name,
          MAP_CHOICE_AMENITIES_HTML_APPLY.website,
          MAP_CHOICE_AMENITIES_HTML_APPLY.propertyId,
          MAP_CHOICE_AMENITIES_HTML_APPLY.amenities,
          MAP_CHOICE_AMENITIES_HTML_APPLY.parentCompany,
        ],
        filterByFormula: `FIND("Choice", {${MAP_CHOICE_AMENITIES_HTML_APPLY.parentCompany}})`,
      })
      .all();
  } catch (err) {
    console.log("STATE: error");
    console.error("Airtable load failed:", err?.message || err);
    logDev("airtable_load", { error: String(err?.message || err) });
    process.exitCode = 1;
    return;
  }

  mkdirSync("reports", { recursive: true });
  mkdirSync(HTML_DIR, { recursive: true });

  let applied = 0;
  let dryRunnable = 0;
  let skipped = 0;
  let errors = 0;

  for (const filePath of htmlFiles) {
    if (!existsSync(filePath)) {
      console.log(`\nSTATE: error — file missing: ${filePath}`);
      errors++;
      continue;
    }

    const html = readFileSync(filePath, "utf8");
    if (/access denied/i.test(html) && html.length < 5000) {
      console.log(`\nSTATE: error — blocked Akamai/403 shell: ${filePath}`);
      console.log("  Do not invent amenities. Re-save the live property page from a normal browser.");
      errors++;
      skipped++;
      continue;
    }

    const parsed = parseChoiceAmenitiesFromHtml(html);
    const urlHint = html.match(/https?:\/\/(?:www\.)?choicehotels\.com[^"'\s<>]+/i)?.[0] || "";
    const pid = choicePropertyIdFromUrl(urlHint);
    const rec = findCensusForHtml(census, filePath, pid);
    const name = rec?.fields?.[MAP_CHOICE_AMENITIES_HTML_APPLY.name] || "(unmatched)";

    console.log(`\n${name} (${filePath})`);
    console.log(
      `  markers=${Boolean(parsed.hasAmenityMarkers)} amenities=${parsed.amenities.length}` +
        (parsed.amenities.length
          ? ` → ${parsed.amenities.slice(0, 8).join("; ")}${parsed.amenities.length > 8 ? "…" : ""}`
          : "")
    );

    if (!parsed.hasAmenityMarkers) {
      console.log("  STATE: error — HTML missing amenity markers (amenityFeature / amenit* DOM).");
      console.log("  Save Webpage, Complete after amenities are visible; do not invent a list.");
      logDev("missing_markers", { filePath, parseErrors: parsed.parseErrors });
      errors++;
      skipped++;
      continue;
    }

    const validation = validateChoiceAmenityHtmlApply(parsed, rec, filePath);
    if (!validation.pass) {
      const isSoftSkip = validation.failed.every((f) =>
        ["amenities_already_populated", "no_amenities_parsed", "empty_amenities_text"].includes(f)
      );
      console.log(
        `  STATE: ${isSoftSkip ? "skip" : "error"} — validation failed: ${validation.failed.join(", ")}`
      );
      logDev("validation_fail", {
        filePath,
        failed: validation.failed,
        parseErrors: parsed.parseErrors,
      });
      if (validation.failed.includes("no_census_match")) errors++;
      else if (!isSoftSkip) errors++;
      skipped++;
      continue;
    }

    console.log("  validation: pass");
    console.log("  field mapping:", JSON.stringify(validation.fieldMapping));
    if (DEV_LOG && validation.sanitizedPayloadPreview) {
      const preview = String(
        validation.sanitizedPayloadPreview[MAP_CHOICE_AMENITIES_HTML_APPLY.amenities] || ""
      );
      console.log("  payload preview:", preview.slice(0, 160) + (preview.length > 160 ? "…" : ""));
    }

    if (!APPLY) {
      console.log("  STATE: success (dry-run) — would fill blank Amenities");
      dryRunnable++;
      continue;
    }

    try {
      await base(HOTEL_CENSUS_TABLE).update(rec.id, validation.sanitizedPayloadPreview, {
        typecast: true,
      });
      applied++;
      dryRunnable++;
      console.log("  STATE: success — Amenities written (fill-blank)");
      if (!existsSync(LOG)) {
        appendFileSync(LOG, "appliedAt,censusRecordId,censusName,htmlFile,amenityCount\n");
      }
      appendFileSync(
        LOG,
        `${new Date().toISOString()},${rec.id},"${String(name).replace(/"/g, '""')}",${filePath},${parsed.amenities.length}\n`
      );
    } catch (err) {
      console.log("  STATE: error — Airtable write failed:", err?.message || err);
      logDev("airtable_write", {
        filePath,
        recordId: rec.id,
        error: String(err?.message || err),
      });
      errors++;
    }
  }

  summarizeCli({
    mode: APPLY ? "apply" : "dry-run",
    htmlCount: htmlFiles.length,
    applied,
    dryRunnable,
    skipped,
    errors,
  });

  if (errors && !applied && !dryRunnable) {
    console.log("STATE: error");
    process.exitCode = 1;
  } else if (!dryRunnable && !applied) {
    console.log("STATE: empty — no rows eligible to apply");
  } else {
    console.log(
      APPLY ? "STATE: success" : "STATE: success (dry-run only — pass --apply to write)"
    );
  }
}

const isDirectRun =
  process.argv[1] && resolve(fileURLToPath(import.meta.url)) === resolve(process.argv[1]);

if (isDirectRun) {
  await main();
}
