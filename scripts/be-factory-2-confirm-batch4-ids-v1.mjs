#!/usr/bin/env node
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync, existsSync, readFileSync } from "fs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const EXPECTED = [
  { brandId: "reccXxMHEh7NNRhIE", brandName: "Tapestry Collection by Hilton" },
  { brandId: "recqiHq3GHKMj8Meo", brandName: "Tempo by Hilton" },
  { brandId: "rec02zPClpWUTCyXM", brandName: "Design Hotels" },
  { brandId: "recJ1GZQpttX7qHgw", brandName: "Aloft Hotels" },
  { brandId: "recCKuXCmGvxHPfb3", brandName: "Kimpton Hotels" },
  { brandId: "recGv268Wda31PlSZ", brandName: "Bunkhouse Hotels" },
  { brandId: "recNy2efMm4N1JtgC", brandName: "Hyatt Centric" },
  { brandId: "rec6hye5H8zJmAGv3", brandName: "Courtyard by Marriott" },
  { brandId: "recIPuBC50fv13zRR", brandName: "Westin" },
  { brandId: "recmGmiIqDtAsm01f", brandName: "Holiday Inn Express" },
];

function loadStage(brandId) {
  const p = join(ROOT, "data/brand-explorer/factory-2-0/brands", brandId, "stage.json");
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function nameMatch(liveName, expected) {
  if (!liveName) return false;
  const live = String(liveName).toLowerCase();
  const exp = String(expected).toLowerCase();
  if (live === exp) return true;
  const tokens = exp.split(/\s+/).filter((t) => t.length > 3);
  return tokens.some((t) => live.includes(t));
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing Airtable credentials");

  const formula = `OR(${EXPECTED.map((b) => `RECORD_ID()='${b.brandId}'`).join(",")})`;
  const params = new URLSearchParams({ filterByFormula: formula, pageSize: "20" });
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
    const confirmed = Boolean(rec) && nameMatch(liveName, e.brandName);
    const depthDone =
      stage?.candidate?.contract === "BE_FACTORY_2_0_DEPTH_CANDIDATE_V1" ||
      String(stage?.batchTag || "").includes("batch-");
    const alreadyComplete = ["READY_FOR_REVIEW", "APPROVED", "PUBLISHED"].includes(
      stage?.factoryStatus
    );
    // Reprocess CURRENT-baseline-only stages with depth packs; skip true READY/APPROVED.
    const process = confirmed && !alreadyComplete;
    return {
      brandId: e.brandId,
      expectedName: e.brandName,
      liveName,
      status,
      parent,
      confirmed,
      existingFactoryStatus: stage?.factoryStatus || null,
      existingBatchTag: stage?.batchTag || null,
      depthDone,
      alreadyComplete,
      process,
      note: alreadyComplete
        ? "Skip — already READY/APPROVED/PUBLISHED"
        : depthDone && !alreadyComplete
          ? "Re-run depth to refresh / ensure Batch 4 pack"
          : "Process with Batch 4 depth pack",
    };
  });

  const out = {
    ok: rows.every((r) => r.confirmed),
    batch: "BATCH_4",
    generatedAt: new Date().toISOString(),
    rows,
    toProcess: rows.filter((r) => r.process).map((r) => r.brandId),
  };
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "30-batch4-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
  if (!out.ok) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
