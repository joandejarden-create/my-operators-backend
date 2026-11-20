#!/usr/bin/env node
/**
 * Batch 7 — live Brand Basics duplicate check (economy / midscale).
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
    key: "garner",
    brandName: "Garner Hotels",
    parentCompany: "InterContinental Hotels Group",
    aliases: ["Garner", "Garner Hotels", "Garner by IHG"],
  },
  {
    key: "sleep-inn",
    brandName: "Sleep Inn",
    parentCompany: "Choice Hotels International",
    aliases: ["Sleep Inn", "Sleep Inn Hotels"],
  },
  {
    key: "econo-lodge",
    brandName: "Econo Lodge",
    parentCompany: "Choice Hotels International",
    aliases: ["Econo Lodge", "EconoLodge"],
  },
  {
    key: "rodeway-inn",
    brandName: "Rodeway Inn",
    parentCompany: "Choice Hotels International",
    aliases: ["Rodeway Inn", "Rodeway"],
  },
  {
    key: "mainstay-suites",
    brandName: "MainStay Suites",
    parentCompany: "Choice Hotels International",
    aliases: ["MainStay Suites", "MainStay", "Main Stay Suites"],
  },
  {
    key: "days-inn",
    brandName: "Days Inn by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Days Inn", "Days Inn by Wyndham", "DaysInn"],
  },
  {
    key: "super-8",
    brandName: "Super 8 by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Super 8", "Super 8 by Wyndham", "Super8"],
  },
  {
    key: "microtel",
    brandName: "Microtel by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Microtel", "Microtel by Wyndham", "Microtel Inn & Suites"],
  },
  {
    key: "baymont",
    brandName: "Baymont by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Baymont", "Baymont by Wyndham", "Baymont Inn & Suites"],
  },
  {
    key: "travelodge",
    brandName: "Travelodge by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Travelodge", "Travelodge by Wyndham"],
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
    if (live.startsWith(n) || n.startsWith(live)) best = Math.max(best, 95);
    if (live.includes(n) || n.includes(live)) best = Math.max(best, 85);
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
        score: scoreMatch(r.fields?.["Brand Name"], t.aliases),
      }))
      .filter((r) => r.score >= 80)
      .sort((a, b) => b.score - a.score);

    const best = scored[0] || null;
    const confirmed = Boolean(best && best.score >= 85);
    let presentationBlockCount = 0;
    if (confirmed) {
      try {
        const live = await fetchPresentationBlocksForBrand(best.id, best.name);
        presentationBlockCount = (live.blocks || []).length;
      } catch {
        /* keep 0 */
      }
    }
    const stage = confirmed ? loadStage(best.id) : null;
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED"].includes(
      stage?.factoryStatus
    );

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
      existingFactoryStatus: stage?.factoryStatus || null,
      alreadyComplete,
      process: confirmed && !alreadyComplete,
      note: !confirmed
        ? "MISSING or weak match — do not invent ID; stage research only after founder create approval"
        : alreadyComplete
          ? "Skip — already READY/APPROVED/PUBLISHED"
          : presentationBlockCount === 0
            ? "EXISTING Brand Basics; Presentation empty — depth stage + create-plan"
            : "Reuse existing Brand Basics; depth-stage Presentation updates",
    });
  }

  const out = {
    ok: rows.every((r) => r.confirmed),
    batch: "BATCH_7",
    generatedAt: new Date().toISOString(),
    inventoryTotal: records.length,
    inventoryNote:
      "Batch 7 economy/midscale duplicate check. Reuse EXISTING_CANONICAL only. No invented rec… IDs.",
    newBrandBasicsCreatesProposed: rows.filter((r) => r.newBrandBasicsCreateRequired).length,
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
    missingOrAmbiguous: rows.filter((r) => !r.confirmed).map((r) => r.expectedName),
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "60-batch7-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
