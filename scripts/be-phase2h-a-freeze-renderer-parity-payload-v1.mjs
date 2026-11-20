#!/usr/bin/env node
/**
 * Phase 2H-A Gate 1 — freeze Curio Presentation payload for renderer parity.
 * Does NOT rewrite Presentation bodies. Writes a deterministic brand object + block hash.
 */
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-a-renderer-parity");
const preview = join(root, "public/brand-explorer-phase2e-preview");
mkdirSync(outDir, { recursive: true });

const CURIO_ID = "receQkxgjlezsc1xg";
const PORT = process.env.PORT || "8082";

function sha256(s) {
  return createHash("sha256").update(String(s)).digest("hex");
}

function blocksHash(blocks) {
  const norm = (blocks || [])
    .map((b) => ({
      slotKey: String(b.slotKey || "").trim(),
      title: String(b.title || "").trim(),
      body: String(b.body || "").trim(),
      imageUrl: b.imageUrl || null,
      sort: typeof b.sort === "number" ? b.sort : null,
    }))
    .sort((a, b) => String(a.slotKey).localeCompare(String(b.slotKey)) || String(a.sort).localeCompare(String(b.sort)));
  return sha256(JSON.stringify(norm));
}

function brandFromBaseline() {
  const snap = JSON.parse(readFileSync(join(preview, "curio-frozen-baseline-snapshot.json"), "utf8"));
  const id = snap.identity || {};
  const rows = snap.presentation?.blocks || snap.presentation?.rows || [];
  const blocks = rows
    .map((r, i) => ({
      recordId: r.recordId || `baseline-${i}`,
      slotKey: String(r.slotKey || r.fields?.["Slot Key"] || "").trim(),
      title: String(r.title || r.fields?.Title || "").trim(),
      body: String(r.body || r.fields?.Body || "").trim(),
      sort: typeof r.sort === "number" ? r.sort : i + 1,
      imageUrl: r.imageUrl || null,
      imageFilename: r.imageFilename || null,
      summaryUrl: r.summaryUrl || "",
    }))
    .filter((b) => b.slotKey);
  return {
    id: CURIO_ID,
    name: id.brandName || "Curio Collection by Hilton",
    parentCompany: id.parentCompany || "Hilton Worldwide",
    brandWebsite: id.website || "",
    brandStatus: id.brandStatus || "Active",
    brandExplorer: { version: 1, blocks, source: "curio-frozen-baseline-snapshot" },
    gate1: {
      payloadSource: "frozen-baseline",
      note: "Saved Presentation only — no content regeneration",
    },
  };
}

async function tryLiveBrand() {
  const url = `http://127.0.0.1:${PORT}/api/brand-library/brand?brandId=${encodeURIComponent(CURIO_ID)}&beInternalPreview=1`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`live HTTP ${res.status}`);
  const data = await res.json();
  if (!data?.success || !data.brand) throw new Error("live brand missing");
  const brand = data.brand;
  brand.gate1 = {
    payloadSource: "live-api-saved-presentation",
    fetchedAt: new Date().toISOString(),
    note: "Live Brand Library Presentation rows — no content regeneration",
  };
  return brand;
}

let brand;
let source;
try {
  brand = await tryLiveBrand();
  source = "live";
} catch (e) {
  console.warn("[gate1] live fetch failed, using frozen baseline:", e.message);
  brand = brandFromBaseline();
  source = "baseline";
}

const blocks = brand.brandExplorer?.blocks || [];
const hash = blocksHash(blocks);
brand.gate1 = {
  ...(brand.gate1 || {}),
  presentationBlocksHashSha256: hash,
  presentationBlockCount: blocks.length,
  brandBasicsRecordId: CURIO_ID,
  frozenAt: new Date().toISOString(),
};

const payloadPath = join(preview, "curio-gate1-renderer-parity-payload.json");
writeFileSync(payloadPath, JSON.stringify(brand, null, 2));
writeFileSync(
  join(outDir, "00-payload-manifest.json"),
  JSON.stringify(
    {
      phase: "2H-A",
      gate: 1,
      source,
      brandId: CURIO_ID,
      presentationBlocksHashSha256: hash,
      presentationBlockCount: blocks.length,
      payloadUrl: "/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json",
      productUrl:
        "/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
      compareUrl:
        "/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&brand=receQkxgjlezsc1xg",
      founderApprovalStatus: "NOT_FOUNDER_APPROVED",
      contentMutation: false,
    },
    null,
    2,
  ),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      source,
      hash,
      blocks: blocks.length,
      payloadPath: "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json",
    },
    null,
    2,
  ),
);
