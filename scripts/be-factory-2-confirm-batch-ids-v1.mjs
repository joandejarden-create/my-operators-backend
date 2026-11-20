#!/usr/bin/env node
import { config as loadEnv } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { writeFileSync, mkdirSync } from "fs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
loadEnv({ path: join(ROOT, ".env") });

const EXPECTED = [
  { brandId: "rec9aZp7GHtzUEg0c", brandName: "AC Hotels by Marriott" },
  { brandId: "recahVIW4aCx0Ao84", brandName: "Moxy Hotels" },
  { brandId: "recDwzv86TWnz2gGB", brandName: "Vignette Collection" },
  { brandId: "recsggfbKlJbjeRP9", brandName: "Canopy by Hilton" },
  { brandId: "recvvmiyReHhiKdoK", brandName: "EVEN Hotels" },
];

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
    const nameMatch =
      liveName &&
      String(liveName).toLowerCase().includes(e.brandName.split(" ")[0].toLowerCase());
    return {
      brandId: e.brandId,
      expectedName: e.brandName,
      liveName,
      status,
      parent,
      confirmed: Boolean(rec) && Boolean(nameMatch),
    };
  });

  const out = {
    ok: rows.every((r) => r.confirmed),
    generatedAt: new Date().toISOString(),
    rows,
  };
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "10-batch-five-identity-confirm.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
  if (!out.ok) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
