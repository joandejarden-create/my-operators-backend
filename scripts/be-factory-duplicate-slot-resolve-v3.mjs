#!/usr/bin/env node
/**
 * Resolve duplicate Presentation image slots from V3 audit conflict report.
 * Non-destructive — writes resolution plan only (no Airtable deletes).
 */
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import {
  BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1,
  resolveDuplicateSlotRows,
} from "../lib/brand-explorer/factory-2/be-factory-duplicate-slot-resolution-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });

const conflictsPath = join(OUT, "04-duplicate-slot-conflicts.json");
const matrixPath = join(OUT, "03-current-slot-matrix.json");
if (!existsSync(conflictsPath)) {
  console.error("Missing 04-duplicate-slot-conflicts.json — run current audit first");
  process.exit(1);
}

const conflicts = JSON.parse(readFileSync(conflictsPath, "utf8"));
const matrix = existsSync(matrixPath)
  ? JSON.parse(readFileSync(matrixPath, "utf8"))
  : { slots: [] };
const byRec = new Map((matrix.slots || []).map((s) => [s.recordId, s]));

const resolutions = [];
for (const c of conflicts.conflicts || []) {
  const rows = (c.recordIds || []).map((id) => {
    const s = byRec.get(id) || {};
    return {
      recordId: id,
      hasAttachment: !!s.attachmentId || s.v3Class !== "IMAGE_MISSING",
      technicallyValid: s.v3Class === "IMAGE_TECHNICALLY_VALID",
      exactBrandOk: s.exactBrandOk === true,
      browserOk: s.browserOk === true,
      rightsOk: s.rightsOk === true,
    };
  });
  const resolved = resolveDuplicateSlotRows(rows);
  resolutions.push({
    brandId: c.brandId,
    brandName: c.brandName,
    slotKey: c.slotKey,
    rawCount: c.count,
    ...resolved,
  });
}

const report = {
  contract: BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1,
  at: new Date().toISOString(),
  conflictCount: resolutions.length,
  reviewRequired: resolutions.filter((r) => r.reviewRequired).length,
  resolutions,
};
const outPath = join(OUT, "05-duplicate-slot-resolutions.json");
writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      ok: true,
      report: outPath,
      conflictCount: report.conflictCount,
      reviewRequired: report.reviewRequired,
    },
    null,
    2
  )
);
