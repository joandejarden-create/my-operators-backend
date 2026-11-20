#!/usr/bin/env node
/**
 * Batch 8 — live Brand Basics duplicate check
 * (Wyndham midscale/economy + BWH + Sonesta economy/midscale/extended-stay).
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
    key: "la-quinta",
    brandName: "La Quinta by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["La Quinta by Wyndham", "La Quinta", "LaQuinta", "LQ"],
  },
  {
    key: "americinn",
    brandName: "AmericInn by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["AmericInn by Wyndham", "AmericInn", "Americ Inn", "AmericInn Lodge & Suites"],
  },
  {
    key: "howard-johnson",
    brandName: "Howard Johnson by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Howard Johnson by Wyndham", "Howard Johnson", "HoJo", "Howard Johnson Inn"],
  },
  {
    key: "wingate",
    brandName: "Wingate by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Wingate by Wyndham", "Wingate", "Wingate by Wyndham Hotels"],
  },
  {
    key: "ramada",
    brandName: "Ramada by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Ramada by Wyndham", "Ramada", "Ramada Inn", "Ramada Plaza"],
  },
  {
    key: "hawthorn",
    brandName: "Hawthorn Extended Stay by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: [
      "Hawthorn Extended Stay by Wyndham",
      "Hawthorn Suites",
      "Hawthorn Extended Stay",
      "Hawthorn",
    ],
  },
  {
    key: "wyndham-garden",
    brandName: "Wyndham Garden",
    parentCompany: "Wyndham Hotels & Resorts",
    aliases: ["Wyndham Garden", "Wyndham Garden Hotels"],
  },
  {
    key: "best-western",
    brandName: "Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["Best Western", "Best Western Hotels", "BestWestern"],
  },
  {
    key: "best-western-plus",
    brandName: "Best Western Plus",
    parentCompany: "BWH Hotels",
    aliases: ["Best Western Plus", "Best Western Plus Hotels", "BW Plus"],
  },
  {
    key: "surestay",
    brandName: "SureStay by Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["SureStay by Best Western", "SureStay", "Sure Stay"],
  },
  {
    key: "surestay-plus",
    brandName: "SureStay Plus by Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["SureStay Plus by Best Western", "SureStay Plus", "Sure Stay Plus"],
  },
  {
    key: "surestay-studio",
    brandName: "SureStay Studio by Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["SureStay Studio by Best Western", "SureStay Studio", "Sure Stay Studio"],
  },
  {
    key: "glo",
    brandName: "GLō by Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["GLō by Best Western", "GLo by Best Western", "GLō", "GLo", "Glo by Best Western"],
  },
  {
    key: "executive-residency",
    brandName: "Executive Residency by Best Western",
    parentCompany: "BWH Hotels",
    aliases: [
      "Executive Residency by Best Western",
      "Executive Residency",
      "Executive Residency Suites",
    ],
  },
  {
    key: "aiden",
    brandName: "Aiden by Best Western",
    parentCompany: "BWH Hotels",
    aliases: ["Aiden by Best Western", "Aiden", "Aiden Hotels"],
  },
  {
    key: "sonesta-select",
    brandName: "Sonesta Select",
    parentCompany: "Sonesta International Hotels Corporation",
    aliases: ["Sonesta Select", "Sonesta Select Hotels", "Select by Sonesta"],
  },
  {
    key: "sonesta-es-suites",
    brandName: "Sonesta ES Suites",
    parentCompany: "Sonesta International Hotels Corporation",
    aliases: ["Sonesta ES Suites", "Sonesta ES", "ES Suites", "Sonesta Extended Stay"],
  },
  {
    key: "red-lion",
    brandName: "Red Lion Hotels, Inns & Suites",
    parentCompany: "Sonesta International Hotels Corporation",
    aliases: [
      "Red Lion Hotels, Inns & Suites",
      "Red Lion Hotels",
      "Red Lion Inn",
      "Red Lion Inns & Suites",
      "Red Lion",
    ],
  },
  {
    key: "signature-inn",
    brandName: "Signature Inn",
    parentCompany: "Sonesta International Hotels Corporation",
    aliases: ["Signature Inn", "Signature Inns", "Signature Inn by Sonesta"],
  },
  {
    key: "americas-best-value-inn",
    brandName: "Americas Best Value Inn",
    parentCompany: "Sonesta International Hotels Corporation",
    aliases: [
      "Americas Best Value Inn",
      "America's Best Value Inn",
      "ABVI",
      "Americas Best Value Inn & Suites",
    ],
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
  // Soften false positives: "Best Western" must not steal "Best Western Plus"
  return best;
}

function disambiguate(targetKey, scored) {
  if (!scored.length) return scored;
  // Prefer exact ladder rung for Best Western family
  if (targetKey === "best-western") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return (
        n === "best western" ||
        n === "best western hotels" ||
        (n.startsWith("best western") &&
          !n.includes("plus") &&
          !n.includes("premier") &&
          !n.includes("surestay") &&
          !n.includes("aiden") &&
          !n.includes("glo") &&
          !n.includes("executive"))
      );
    });
  }
  if (targetKey === "best-western-plus") {
    return scored.filter((r) => norm(r.name).includes("plus") && !norm(r.name).includes("premier"));
  }
  if (targetKey === "surestay" && !targetKey.includes("plus")) {
    return scored.filter((r) => {
      const n = norm(r.name);
      return n.includes("surestay") && !n.includes("plus") && !n.includes("studio");
    });
  }
  if (targetKey === "surestay-plus") {
    return scored.filter((r) => {
      const n = norm(r.name);
      return n.includes("surestay") && n.includes("plus") && !n.includes("studio");
    });
  }
  if (targetKey === "surestay-studio") {
    return scored.filter((r) => norm(r.name).includes("studio"));
  }
  return scored;
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
    let scored = records
      .map((r) => ({
        id: r.id,
        name: r.fields?.["Brand Name"] || null,
        status: r.fields?.["Brand Status"] || null,
        parent: r.fields?.["Parent Company"] || null,
        score: scoreMatch(r.fields?.["Brand Name"], t.aliases),
      }))
      .filter((r) => r.score >= 80)
      .sort((a, b) => b.score - a.score);

    scored = disambiguate(t.key, scored).sort((a, b) => b.score - a.score);

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
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED", "CERTIFIED_READY_FOR_REVIEW"].includes(
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
    batch: "BATCH_8",
    generatedAt: new Date().toISOString(),
    inventoryTotal: records.length,
    inventoryNote:
      "Batch 8 economy/midscale/extended-stay duplicate check (Wyndham / BWH / Sonesta). Reuse EXISTING_CANONICAL only. No invented rec… IDs.",
    newBrandBasicsCreatesProposed: rows.filter((r) => r.newBrandBasicsCreateRequired).length,
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
    missingOrAmbiguous: rows.filter((r) => !r.confirmed).map((r) => r.expectedName),
  };

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "80-batch8-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
