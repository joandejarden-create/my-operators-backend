#!/usr/bin/env node
/**
 * Enrich Brand Basics → Brand Positioning from the 233-brand master mapping,
 * then fill remaining live inventory with researched / derived values when safe.
 *
 * Default: --dry-run. Pass --apply for live writes (founder-authorized).
 * Only updates Brand Positioning. Read-back every write.
 */
import "../load-env.js";
import Airtable from "airtable";
import { readFileSync, writeFileSync, mkdirSync, existsSync, copyFileSync } from "fs";
import { join } from "path";
import {
  BRAND_POSITIONING_FIELD,
  BRAND_POSITIONING_FIELD_ID,
  BRAND_BASICS_TABLE,
  BRAND_BASICS_TABLE_ID,
  BRAND_BASICS_BASE_ID,
  BRAND_POSITIONING_FIELD_DESCRIPTION,
  validateBrandPositioning,
  toPositioningTitleCase,
  rewriteSlashPositioning,
  normalizeBrandNameKey,
  POSITIONING_SLASH_RE,
} from "../lib/brand-explorer/be-brand-positioning-canonical-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const MAPPING_PATH = join(ROOT, "fixtures/brand-positioning-master-mapping-v1.json");

/** Extra alias keys → master brand name */
const ALIASES = {
  "curio collection": "Curio Collection by Hilton",
  "curio": "Curio Collection by Hilton",
  "ascend": "Ascend Hotel Collection",
  "autograph collection hotels": "Autograph Collection",
  "ac hotels": "AC Hotels by Marriott",
  "moxy": "Moxy Hotels",
  "canopy": "Canopy by Hilton",
  "even": "EVEN Hotels",
  "voco": "voco Hotels",
  "motto": "Motto by Hilton",
  "avid": "avid hotels",
  "tapestry collection": "Tapestry Collection by Hilton",
  "tempo": "Tempo by Hilton",
  "aloft": "Aloft Hotels",
  "kimpton": "Kimpton Hotels",
  "bunkhouse": "Bunkhouse Hotels",
  "courtyard": "Courtyard by Marriott",
  "westin hotels": "Westin",
  "the westin": "Westin",
  "jw marriott hotels": "JW Marriott",
  "w hotels worldwide": "W Hotels",
  "le meridien": "Le Méridien",
  "waldorf astoria": "Waldorf Astoria Hotels & Resorts",
  "conrad": "Conrad Hotels & Resorts",
  "lxr": "LXR Hotels & Resorts",
  "ritz carlton": "The Ritz-Carlton",
  "the ritz carlton": "The Ritz-Carlton",
  "st regis": "St. Regis Hotels & Resorts",
  "edition": "EDITION Hotels",
  "luxury collection": "The Luxury Collection",
  "renaissance": "Renaissance Hotels",
  "signia": "Signia by Hilton",
  "intercontinental": "InterContinental Hotels & Resorts",
  "six senses": "Six Senses Hotels Resorts Spas",
  "glo by best western": "GLō by Best Western",
  "glo": "GLō by Best Western",
  "hilton hotels": "Hilton Hotels & Resorts",
  "hilton hotels resorts": "Hilton Hotels & Resorts",
  "doubletree": "DoubleTree by Hilton",
  "hampton": "Hampton by Hilton",
  "hampton inn": "Hampton by Hilton",
  "so": "SO/",
  "so hotels": "SO/",
  "swissotel": "Swissôtel",
  "zoetry": "Zoëtry Wellness & Spa Resorts",
  "movenpick": "Mövenpick Hotels & Resorts",
  "gran melia": "Gran Meliá Hotels & Resorts",
  "me by melia": "ME by Meliá",
  "melia hotels": "Meliá Hotels & Resorts",
  "innside": "INNSiDE by Meliá",
  "parkroyal collection": "PARKROYAL COLLECTION",
};

function flattenMapping(doc) {
  const out = [];
  for (const [batch, rows] of Object.entries(doc.batches || {})) {
    for (const row of rows) {
      out.push({
        batch,
        brand: row.brand,
        positioning: row.positioning,
        key: normalizeBrandNameKey(row.brand),
      });
    }
  }
  return out;
}

function buildLookup(flat) {
  const byKey = new Map();
  for (const row of flat) {
    byKey.set(row.key, row);
    // also register without leading "the "
    if (row.key.startsWith("the ")) byKey.set(row.key.slice(4), row);
  }
  for (const [alias, brand] of Object.entries(ALIASES)) {
    const master = flat.find((r) => r.brand === brand);
    if (master) byKey.set(normalizeBrandNameKey(alias), master);
  }
  return byKey;
}

function chooseFinal(previous, proposed) {
  const prev = String(previous || "").trim();
  const propV = validateBrandPositioning(proposed);
  if (!propV.ok) return { final: prev, action: "skip_invalid_proposed", reason: propV.codes.join(",") };

  let proposedNorm = toPositioningTitleCase(propV.value);
  // Preserve intentional casing for short all-caps / brand tokens already Title-ready
  if (propV.value === propV.value.toUpperCase() && propV.value.length <= 12) {
    proposedNorm = propV.value;
  } else {
    proposedNorm = propV.value; // master list already Title Case
  }

  if (!prev) {
    return { final: proposedNorm, action: "populate", reason: "empty" };
  }
  if (POSITIONING_SLASH_RE.test(prev)) {
    const rewritten = rewriteSlashPositioning(prev) || proposedNorm;
    return { final: rewritten, action: "correct_slash", reason: "previous_had_slash" };
  }
  if (normalizeBrandNameKey(prev) === normalizeBrandNameKey(proposedNorm)) {
    return { final: prev, action: "preserve", reason: "already_matches" };
  }
  // Prefer concise master over long prose (> 60 chars) or sentence-like previous
  if (prev.length > 60 || /[.!?]$/.test(prev) || prev.split(/\s+/).length > 10) {
    return { final: proposedNorm, action: "correct_prose", reason: "previous_was_long_prose" };
  }
  // Prefer master when previous looks like chain-scale only
  if (/^(economy|midscale|upscale|luxury|upper[- ]?midscale|premium)$/i.test(prev) && proposedNorm.split(/\s+/).length > 1) {
    return { final: proposedNorm, action: "correct_generic", reason: "previous_too_generic" };
  }
  // Default: apply master (founder-authorized enrichment)
  return { final: proposedNorm, action: "correct", reason: "master_mapping" };
}

async function updateFieldDescription(token, baseId, tableId, fieldId) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables/${tableId}/fields/${fieldId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ description: BRAND_POSITIONING_FIELD_DESCRIPTION }),
  });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  const apply = process.argv.includes("--apply");
  const dryRun = !apply;
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || BRAND_BASICS_BASE_ID;
  if (!token) throw new Error("AIRTABLE_API_KEY required");

  const mappingDoc = JSON.parse(readFileSync(MAPPING_PATH, "utf8"));
  const flat = flattenMapping(mappingDoc);
  const lookup = buildLookup(flat);
  console.error(`Master mappings: ${flat.length}`);

  // Ensure field description
  if (apply) {
    const desc = await updateFieldDescription(token, baseId, BRAND_BASICS_TABLE_ID, BRAND_POSITIONING_FIELD_ID);
    console.error(`Field description update: ${desc.ok ? "OK" : "SKIP/FAIL " + desc.status}`);
  } else {
    console.error("Dry-run: would PATCH Brand Positioning field description");
  }

  const airtable = new Airtable({ apiKey: token }).base(baseId);
  const live = [];
  await airtable(BRAND_BASICS_TABLE)
    .select({
      fields: ["Brand Name", BRAND_POSITIONING_FIELD, "Parent Company", "Brand Status"],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      live.push(...records);
      next();
    });

  const matrix = [];
  const unmatched = [];
  let newlyPopulated = 0;
  let corrected = 0;
  let preserved = 0;
  let writeFailures = 0;
  let readbackOk = 0;
  let plannedWrites = 0;

  const matchedKeys = new Set();

  for (const rec of live) {
    const name = String(rec.get("Brand Name") || "").trim();
    const parent = String(rec.get("Parent Company") || "").trim();
    const previous = String(rec.get(BRAND_POSITIONING_FIELD) || "").trim();
    const key = normalizeBrandNameKey(name);
    let master = lookup.get(key);
    if (!master && key.startsWith("the ")) master = lookup.get(key.slice(4));
    // fuzzy: starts with / includes
    if (!master) {
      for (const row of flat) {
        if (key.includes(row.key) || row.key.includes(key)) {
          if (Math.abs(key.length - row.key.length) <= 8) {
            master = row;
            break;
          }
        }
      }
    }

    if (!master) {
      // Additional inventory — keep valid existing; flag empty / slash for verification
      let final = previous;
      let action = "inventory_preserve";
      let status = "INVENTORY_NO_MASTER";
      if (!previous) {
        action = "needs_verification";
        status = "NEEDS_VERIFICATION";
      } else if (POSITIONING_SLASH_RE.test(previous)) {
        const rw = rewriteSlashPositioning(previous);
        if (rw) {
          final = rw;
          action = "inventory_correct_slash";
          status = "INVENTORY_SLASH_REWRITE";
        } else {
          action = "needs_verification";
          status = "NEEDS_VERIFICATION_SLASH";
        }
      }
      matrix.push({
        brandId: rec.id,
        brandName: name,
        parentCompany: parent,
        batch: null,
        previousPositioning: previous || null,
        finalPositioning: final || null,
        source: "inventory",
        verificationStatus: status,
        airtableUpdateStatus: "PENDING",
        action,
      });
      continue;
    }

    matchedKeys.add(master.key);
    const choice = chooseFinal(previous, master.positioning);
    matrix.push({
      brandId: rec.id,
      brandName: name,
      parentCompany: parent,
      batch: master.batch,
      previousPositioning: previous || null,
      finalPositioning: choice.final,
      source: "master_mapping",
      verificationStatus: choice.action === "preserve" ? "VERIFIED_MATCH" : "MASTER_APPLIED",
      airtableUpdateStatus: "PENDING",
      action: choice.action,
      reason: choice.reason,
      masterBrand: master.brand,
    });
  }

  for (const row of flat) {
    if (!matchedKeys.has(row.key) && ![...matchedKeys].some((k) => k.includes(row.key) || row.key.includes(k))) {
      // check if any matrix row used this master
      const used = matrix.some((m) => m.masterBrand === row.brand || m.batch === row.batch && normalizeBrandNameKey(m.brandName) === row.key);
      if (!used) {
        unmatched.push({ brand: row.brand, batch: row.batch, positioning: row.positioning });
      }
    }
  }
  // Better unmatched: masters not referenced as masterBrand
  const usedMasters = new Set(matrix.filter((m) => m.masterBrand).map((m) => m.masterBrand));
  const unmatchedClean = flat.filter((r) => !usedMasters.has(r.brand)).map((r) => ({
    brand: r.brand,
    batch: r.batch,
    positioning: r.positioning,
  }));

  // Execute writes
  for (const row of matrix) {
    const prev = row.previousPositioning || "";
    const next = row.finalPositioning || "";
    if (!next || next === prev) {
      if (row.action === "preserve" || row.action === "inventory_preserve") preserved += 1;
      row.airtableUpdateStatus = "UNCHANGED";
      continue;
    }
    if (row.action === "needs_verification") {
      row.airtableUpdateStatus = "SKIPPED_NEEDS_VERIFICATION";
      continue;
    }
    plannedWrites += 1;
    if (dryRun) {
      row.airtableUpdateStatus = "DRY_RUN_WOULD_WRITE";
      if (!prev) newlyPopulated += 1;
      else corrected += 1;
      continue;
    }
    try {
      await airtable(BRAND_BASICS_TABLE).update(row.brandId, {
        [BRAND_POSITIONING_FIELD]: next,
      });
      // read-back
      const rb = await airtable(BRAND_BASICS_TABLE).find(row.brandId);
      const got = String(rb.get(BRAND_POSITIONING_FIELD) || "").trim();
      if (got === next) {
        readbackOk += 1;
        row.airtableUpdateStatus = "UPDATED_READBACK_OK";
        if (!prev) newlyPopulated += 1;
        else corrected += 1;
      } else {
        writeFailures += 1;
        row.airtableUpdateStatus = "READBACK_MISMATCH";
        row.readbackValue = got;
      }
    } catch (err) {
      writeFailures += 1;
      row.airtableUpdateStatus = "WRITE_FAILED";
      row.error = String(err.message || err);
    }
  }

  const needsVerification = matrix.filter((m) =>
    String(m.verificationStatus || "").includes("NEEDS_VERIFICATION")
  ).length;

  const summary = {
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "apply",
    contract: "BE_BRAND_POSITIONING_ENRICHMENT_V1",
    field: BRAND_POSITIONING_FIELD,
    fieldId: BRAND_POSITIONING_FIELD_ID,
    tableId: BRAND_BASICS_TABLE_ID,
    baseId,
    totalSuppliedMappings: flat.length,
    totalLiveBrandBasics: live.length,
    totalMatched: matrix.filter((m) => m.source === "master_mapping").length,
    totalUnmatchedMasters: unmatchedClean.length,
    newlyPopulated,
    corrected,
    preserved,
    additionalInventoryRows: matrix.filter((m) => m.source === "inventory").length,
    needingVerification: needsVerification,
    writeFailures,
    readbackOk,
    plannedWrites,
    researchSpendUsd: 0,
  };

  const dir = join(ROOT, "reports/brand-explorer/brand-positioning-v1");
  mkdirSync(dir, { recursive: true });
  const stamp = dryRun ? "DRY_RUN" : "APPLY";
  writeFileSync(join(dir, `01-enrichment-${stamp}.json`), JSON.stringify({ summary, unmatched: unmatchedClean, matrix }, null, 2) + "\n");
  writeFileSync(
    join(dir, `01-enrichment-${stamp}.md`),
    [
      `# Brand Positioning Enrichment — ${stamp}`,
      ``,
      `Generated: ${summary.generatedAt}`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Supplied mappings | ${summary.totalSuppliedMappings} |`,
      `| Live Brand Basics | ${summary.totalLiveBrandBasics} |`,
      `| Matched | ${summary.totalMatched} |`,
      `| Unmatched masters | ${summary.totalUnmatchedMasters} |`,
      `| Newly populated | ${summary.newlyPopulated} |`,
      `| Corrected | ${summary.corrected} |`,
      `| Preserved | ${summary.preserved} |`,
      `| Inventory (no master) | ${summary.additionalInventoryRows} |`,
      `| Needs verification | ${summary.needingVerification} |`,
      `| Planned writes | ${summary.plannedWrites} |`,
      `| Read-back OK | ${summary.readbackOk} |`,
      `| Write failures | ${summary.writeFailures} |`,
      `| Research $ | ${summary.researchSpendUsd} |`,
      ``,
      unmatchedClean.length
        ? `## Unmatched master brands (no live Brand Basics record)\n\n` +
          unmatchedClean.map((u) => `- ${u.brand} (${u.batch}) → ${u.positioning}`).join("\n")
        : `All master brands matched a live Brand Basics record.`,
      ``,
    ].join("\n")
  );

  // Sync atelier UI to HE
  if (existsSync(HE)) {
    const src = join(ROOT, "public/js/brand-explorer-atelier-from-api.js");
    const dest = join(HE, "public/js/brand-explorer-atelier-from-api.js");
    if (existsSync(src) && existsSync(join(HE, "public/js"))) copyFileSync(src, dest);
  }

  console.log(JSON.stringify(summary, null, 2));
  if (!dryRun && writeFailures) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
