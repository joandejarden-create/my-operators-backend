#!/usr/bin/env node
/**
 * Backfill NEW_* staged Factory candidates into Comparison Studio registry.
 * Founder requirement: every genuinely processed brand must be selectable in
 * Brand Explorer Comparison Studio, including brands awaiting Brand Basics create.
 *
 * Does NOT write Airtable Presentation.
 */
import { existsSync, readdirSync, readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const STAGING = join(ROOT, "data/brand-explorer/factory-2-0/brands");
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const REGISTRY = join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js");

function slugify(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function escapeJs(s) {
  return String(s || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function main() {
  let src = readFileSync(REGISTRY, "utf8");
  const markers = ["\n  ];\n\n  function nz(s)", "\n  ];\r\n\r\n  function nz(s)"];
  const marker = markers.find((m) => src.includes(m));
  if (!marker) {
    console.error(JSON.stringify({ ok: false, error: "registry_marker_missing" }));
    process.exit(1);
  }

  const previewFiles = existsSync(PREVIEW) ? readdirSync(PREVIEW) : [];
  const added = [];
  const skipped = [];

  for (const brandId of readdirSync(STAGING).filter((d) => d.startsWith("NEW_"))) {
    if (src.includes(`id: '${brandId}'`)) {
      skipped.push({ brandId, reason: "already_registered" });
      continue;
    }
    const stage = JSON.parse(readFileSync(join(STAGING, brandId, "stage.json"), "utf8"));
    const brandName = stage.brandName || brandId;
    let slug = stage.slug || stage.brandSlug || null;
    const frozenHint = `${slugify(brandName)}-factory2-frozen-approval-payload-v1.json`;
    let frozen =
      previewFiles.find((f) => f === frozenHint) ||
      previewFiles.find(
        (f) =>
          f.includes("factory2-frozen-approval-payload") &&
          f.toLowerCase().includes(slugify(brandName).slice(0, 12))
      ) ||
      previewFiles.find(
        (f) =>
          f.includes("factory2-frozen-approval-payload") &&
          f.toLowerCase().startsWith(brandId.replace(/^NEW_/, "").toLowerCase())
      );

    if (!slug && frozen) {
      slug = frozen.replace(/-factory2-frozen-approval-payload-v1\.json$/, "");
    }
    if (!slug) slug = slugify(brandName) || brandId.replace(/^NEW_/, "");

    if (!frozen) {
      frozen = `${slug}-factory2-frozen-approval-payload-v1.json`;
      if (!existsSync(join(PREVIEW, frozen))) {
        skipped.push({ brandId, reason: "frozen_payload_missing", slug });
        continue;
      }
    }

    const hash = stage.assembledHash || stage.candidateHash || "";
    const entry = `,
    {
      id: '${brandId}',
      slug: '${escapeJs(slug)}',
      name: '${escapeJs(brandName)}',
      overlayUrl: '/brand-explorer-phase2e-preview/${escapeJs(frozen)}',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-depth',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '${hash}',
      parentCompany: '${escapeJs(stage.parentCompany || "")}',
      batchTag: '${escapeJs(stage.batchTag || "")}',
      factoryStatus: '${escapeJs(stage.factoryStatus || "NEEDS_REMEDIATION")}',
      awaitingBrandBasicsCreate: true
    }`;
    src = src.replace(marker, entry + marker);
    added.push({ brandId, slug, brandName, frozen, hash: hash.slice(0, 12) });
  }

  if (added.length) writeFileSync(REGISTRY, src);
  console.log(JSON.stringify({ ok: true, added: added.length, skipped: skipped.length, addedSample: added.slice(0, 5), skippedSample: skipped.slice(0, 5) }, null, 2));
}

main();
