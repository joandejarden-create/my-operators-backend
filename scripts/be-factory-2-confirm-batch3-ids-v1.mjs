#!/usr/bin/env node
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const EXPECTED = [
  { brandId: "recwONQTqGU1jHCsM", brandName: "voco Hotels" },
  { brandId: "reclt44apoi8co0e6", brandName: "Motto by Hilton" },
  { brandId: "recoEarnE8T6sDjZq", brandName: "avid hotels" },
  { brandId: "recCvV0PuZOi8c3hC", brandName: "Tribute Portfolio" },
  { brandId: "recegXrqaPiSLGCIe", brandName: "Hotel Indigo" },
];

function loadStage(brandId) {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/brands", brandId, "stage.json");
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable credentials");

  const formula = `OR(${EXPECTED.map((b) => `RECORD_ID()='${b.brandId}'`).join(",")})`;
  const params = new URLSearchParams({ filterByFormula: formula, pageSize: "10" });
  params.append("fields[]", "Brand Name");
  params.append("fields[]", "Brand Status");
  params.append("fields[]", "Parent Company");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}?${params}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json).slice(0, 300));

  const byId = new Map((json.records || []).map((r) => [r.id, r]));
  const rows = EXPECTED.map((e) => {
    const rec = byId.get(e.brandId);
    const liveName = rec?.fields?.["Brand Name"] || null;
    const status = rec?.fields?.["Brand Status"] || null;
    const parent = rec?.fields?.["Parent Company"] || null;
    const stage = loadStage(e.brandId);
    const first = e.brandName.split(" ")[0].toLowerCase();
    const nameMatch = liveName && String(liveName).toLowerCase().includes(first);
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED"].includes(
      stage?.factoryStatus
    );
    return {
      brandId: e.brandId,
      expectedName: e.brandName,
      liveName,
      status,
      parent,
      confirmed: Boolean(rec) && Boolean(nameMatch),
      existingFactoryStatus: stage?.factoryStatus || null,
      alreadyComplete,
      process: Boolean(rec) && Boolean(nameMatch) && !alreadyComplete,
    };
  });

  const out = {
    ok: rows.every((r) => r.confirmed),
    batch: "BATCH_3",
    generatedAt: new Date().toISOString(),
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
  };
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "20-batch3-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
  if (!out.ok) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
