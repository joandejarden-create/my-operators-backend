#!/usr/bin/env node
/**
 * Batch 6 — live Brand Basics duplicate check + Presentation coverage.
 * Reuses existing canonical IDs; never invents rec… IDs.
 */
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const TARGETS = [
  {
    key: "ritz-carlton",
    brandName: "The Ritz-Carlton",
    parentCompany: "Marriott International, Inc.",
    aliases: ["The Ritz-Carlton", "Ritz-Carlton", "Ritz Carlton", "The Ritz Carlton"],
  },
  {
    key: "st-regis",
    brandName: "St. Regis Hotels & Resorts",
    parentCompany: "Marriott International, Inc.",
    aliases: ["St. Regis", "St Regis", "St. Regis Hotels & Resorts", "The St. Regis"],
  },
  {
    key: "edition",
    brandName: "EDITION Hotels",
    parentCompany: "Marriott International, Inc.",
    aliases: ["EDITION", "Edition", "EDITION Hotels", "Edition Hotels", "The EDITION"],
  },
  {
    key: "luxury-collection",
    brandName: "The Luxury Collection",
    parentCompany: "Marriott International, Inc.",
    aliases: ["The Luxury Collection", "Luxury Collection"],
  },
  {
    key: "renaissance",
    brandName: "Renaissance Hotels",
    parentCompany: "Marriott International, Inc.",
    aliases: ["Renaissance Hotels", "Renaissance", "Renaissance Hotel"],
  },
  {
    key: "signia",
    brandName: "Signia by Hilton",
    parentCompany: "Hilton Worldwide",
    aliases: ["Signia by Hilton", "Signia", "Signia Hilton"],
  },
  {
    key: "alila",
    brandName: "Alila",
    parentCompany: "Hyatt Hotels Corporation",
    aliases: ["Alila", "Alila Hotels", "Alila Hotels and Resorts"],
  },
  {
    key: "hyatt-house",
    brandName: "Hyatt House",
    parentCompany: "Hyatt Hotels Corporation",
    aliases: ["Hyatt House"],
  },
  {
    key: "intercontinental",
    brandName: "InterContinental Hotels & Resorts",
    parentCompany: "InterContinental Hotels Group",
    aliases: [
      "InterContinental",
      "InterContinental Hotels & Resorts",
      "InterContinental Hotels and Resorts",
      "InterContinental Hotels",
    ],
  },
  {
    key: "six-senses",
    brandName: "Six Senses Hotels Resorts Spas",
    parentCompany: "InterContinental Hotels Group",
    aliases: ["Six Senses", "Six Senses Hotels Resorts Spas", "Six Senses Hotels & Spas"],
  },
];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadStage(brandId) {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/brands", brandId, "stage.json");
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function scoreMatch(liveName, aliases) {
  const live = norm(liveName);
  let best = 0;
  for (const a of aliases) {
    const n = norm(a);
    if (live === n) return 100;
    if (live.includes(n) || n.includes(live)) best = Math.max(best, 80);
    const liveTokens = live.split(" ").filter((t) => t.length > 2);
    const aliasTokens = n.split(" ").filter((t) => t.length > 2);
    const overlap = aliasTokens.filter((t) => liveTokens.includes(t)).length;
    if (overlap >= Math.min(2, aliasTokens.length)) best = Math.max(best, 60 + overlap * 5);
  }
  return best;
}

async function fetchAllBasics(token, baseId) {
  const all = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    params.append("fields[]", "Brand Name");
    params.append("fields[]", "Brand Status");
    params.append("fields[]", "Parent Company");
    params.append("fields[]", "External Display Status");
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json).slice(0, 400));
    all.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return all;
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable credentials");

  const records = await fetchAllBasics(token, baseId);
  const rows = [];

  for (const t of TARGETS) {
    const scored = records
      .map((r) => ({
        id: r.id,
        name: r.fields?.["Brand Name"] || null,
        status: r.fields?.["Brand Status"] || null,
        parent: r.fields?.["Parent Company"] || null,
        eds: r.fields?.["External Display Status"] || null,
        score: scoreMatch(r.fields?.["Brand Name"], t.aliases),
      }))
      .filter((r) => r.score >= 60)
      .sort((a, b) => b.score - a.score);

    // Prefer exact / near-exact over weak includes (e.g. avoid "Hyatt" matching Hyatt House weakly via parent)
    const best = scored[0] || null;
    const confirmed = Boolean(best && best.score >= 80);
    let presentationBlockCount = 0;
    let presentationError = null;
    if (confirmed) {
      try {
        const live = await fetchPresentationBlocksForBrand(best.id, best.name);
        presentationBlockCount = (live.blocks || []).length;
      } catch (err) {
        presentationError = String(err?.message || err).slice(0, 200);
      }
    }
    const stage = confirmed ? loadStage(best.id) : null;
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED"].includes(
      stage?.factoryStatus
    );
    const depthDone =
      stage?.candidate?.contract === "BE_FACTORY_2_0_DEPTH_CANDIDATE_V1" ||
      String(stage?.batchTag || "").includes("batch-");

    rows.push({
      key: t.key,
      expectedName: t.brandName,
      expectedParent: t.parentCompany,
      brandId: confirmed ? best.id : null,
      liveName: confirmed ? best.name : null,
      status: confirmed ? best.status : null,
      parent: confirmed ? best.parent : null,
      matchScore: best?.score || 0,
      candidates: scored.slice(0, 5),
      classification: confirmed
        ? "EXISTING_CANONICAL"
        : scored.length
          ? "AMBIGUOUS_OR_WEAK_MATCH"
          : "MISSING_FROM_INVENTORY",
      newBrandBasicsCreateRequired: !confirmed,
      confirmed,
      presentationBlockCount,
      presentationError,
      existingFactoryStatus: stage?.factoryStatus || null,
      alreadyComplete,
      depthDone,
      process: confirmed && !alreadyComplete,
      note: !confirmed
        ? scored.length
          ? "Weak/ambiguous match — do not invent ID; document for founder"
          : "MISSING — stage research with provisional slug; Basics create requires separate founder approval"
        : alreadyComplete
          ? "Skip — already READY/APPROVED/PUBLISHED"
          : presentationBlockCount === 0
            ? "EXISTING Brand Basics; Presentation empty — depth stage + create-plan"
            : "Reuse existing Brand Basics; depth-stage Presentation updates",
    });
  }

  const out = {
    ok: rows.every((r) => r.confirmed),
    batch: "BATCH_6",
    generatedAt: new Date().toISOString(),
    inventoryTotal: records.length,
    inventoryNote:
      "Batch 6 duplicate check against live Brand Setup - Brand Basics. Reuse EXISTING_CANONICAL IDs only. No invented rec… IDs.",
    newBrandBasicsCreatesProposed: rows.filter((r) => r.newBrandBasicsCreateRequired).length,
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
    missingOrAmbiguous: rows.filter((r) => !r.confirmed).map((r) => r.expectedName),
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "50-batch6-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
  // Exit 0 even if some missing — Batch 6 may stage provisional research for missing brands
  // without inventing IDs; process script gates on confirmed IDs.
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
