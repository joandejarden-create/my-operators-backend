#!/usr/bin/env node
/**
 * Batch 5 — live Brand Basics duplicate check + Presentation coverage.
 * Reuses existing canonical IDs; never invents rec… IDs.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

/** Exact canonical matches from live inventory (false positives excluded). */
export const BATCH_5_EXPECTED = Object.freeze([
  {
    brandId: "recj5BCSpWqMEEBDg",
    brandName: "JW Marriott",
    liveBrandName: "JW Marriott",
    parentCompany: "Marriott International, Inc.",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recqBiokz5ePxbFTE",
    brandName: "W Hotels",
    liveBrandName: "W Hotels",
    parentCompany: "Marriott International, Inc.",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recN6sas0Km17O2Yn",
    brandName: "Le Méridien",
    liveBrandName: "Le Meridien",
    parentCompany: "Marriott International, Inc.",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recXopdFodrD3IyIR",
    brandName: "Waldorf Astoria Hotels & Resorts",
    liveBrandName: "Waldorf Astoria",
    parentCompany: "Hilton Worldwide",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "rec90n1dSVVyoEOto",
    brandName: "Conrad Hotels & Resorts",
    liveBrandName: "Conrad Hotels & Resorts",
    parentCompany: "Hilton Worldwide",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recUsZOxtqIpeA5ZW",
    brandName: "LXR Hotels & Resorts",
    liveBrandName: "LXR Hotels & Resorts",
    parentCompany: "Hilton Worldwide",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recswdE1mhwDvXtBi",
    brandName: "Park Hyatt",
    liveBrandName: "Park Hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recGCWhkeEvsQxuez",
    brandName: "Grand Hyatt",
    liveBrandName: "Grand Hyatt",
    parentCompany: "Hyatt Hotels Corporation",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recbtJoMub5wcankK",
    brandName: "Andaz",
    liveBrandName: "Andaz",
    parentCompany: "Hyatt Hotels Corporation",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
  {
    brandId: "recgwxli5IsoPKCOY",
    brandName: "Hyatt Place",
    liveBrandName: "Hyatt Place",
    parentCompany: "Hyatt Hotels Corporation",
    expectedStatusClass: "EXISTING_CANONICAL",
  },
]);

function loadStage(brandId) {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/brands", brandId, "stage.json");
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function nameMatch(liveName, expectedLive) {
  if (!liveName) return false;
  const live = String(liveName).toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const exp = String(expectedLive)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  if (live === exp) return true;
  return live.includes(exp) || exp.includes(live);
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable credentials");

  const formula = `OR(${BATCH_5_EXPECTED.map((b) => `RECORD_ID()='${b.brandId}'`).join(",")})`;
  const params = new URLSearchParams({ filterByFormula: formula, pageSize: "20" });
  params.append("fields[]", "Brand Name");
  params.append("fields[]", "Brand Status");
  params.append("fields[]", "Parent Company");
  params.append("fields[]", "External Display Status");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}?${params}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json).slice(0, 400));

  const byId = new Map((json.records || []).map((r) => [r.id, r]));
  const rows = [];
  for (const e of BATCH_5_EXPECTED) {
    const rec = byId.get(e.brandId);
    const liveName = rec?.fields?.["Brand Name"] || null;
    const status = rec?.fields?.["Brand Status"] || null;
    const parent = rec?.fields?.["Parent Company"] || null;
    const eds = rec?.fields?.["External Display Status"] || null;
    const stage = loadStage(e.brandId);
    const confirmed = Boolean(rec) && nameMatch(liveName, e.liveBrandName);
    let presentationCount = 0;
    let presentationError = null;
    if (confirmed) {
      try {
        const live = await fetchPresentationBlocksForBrand(e.brandId, e.liveBrandName);
        presentationCount = (live.blocks || []).length;
      } catch (err) {
        presentationError = String(err?.message || err).slice(0, 200);
      }
    }
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED"].includes(
      stage?.factoryStatus
    );
    rows.push({
      brandId: e.brandId,
      expectedName: e.brandName,
      liveName,
      status,
      parent,
      externalDisplayStatus: eds,
      classification: confirmed ? "EXISTING_CANONICAL" : "MISSING_OR_MISMATCH",
      newBrandBasicsCreateRequired: false,
      confirmed,
      presentationBlockCount: presentationCount,
      presentationError,
      existingFactoryStatus: stage?.factoryStatus || null,
      alreadyComplete,
      process: confirmed && !alreadyComplete,
      note: !confirmed
        ? "STOP — identity not confirmed; do not invent ID"
        : alreadyComplete
          ? "Skip — already READY/APPROVED/PUBLISHED"
          : presentationCount === 0
            ? "EXISTING Brand Basics; Presentation empty — stage depth candidate + create-plan only (no Basics create)"
            : "Reuse existing Brand Basics; depth-stage Presentation updates (no Basics create)",
    });
  }

  const out = {
    ok: rows.every((r) => r.confirmed),
    batch: "BATCH_5",
    generatedAt: new Date().toISOString(),
    inventoryNote:
      "All ten Batch 5 brands already exist in Brand Setup - Brand Basics (Draft or Under Review). No new Brand Basics records will be created. Duplicate creation is forbidden.",
    newBrandBasicsCreates: 0,
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "40-batch5-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
  if (!out.ok) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
