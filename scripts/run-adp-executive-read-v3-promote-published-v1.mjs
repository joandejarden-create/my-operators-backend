#!/usr/bin/env node
/**
 * Promote nested compositionV3 → customer-default top-level compositionVersion + sections
 * on published snapshots (additive; preserves writeup/ux).
 *
 *   node scripts/run-adp-executive-read-v3-promote-published-v1.mjs
 *   node scripts/run-adp-executive-read-v3-promote-published-v1.mjs --apply
 */

import { readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { ADP_EXECUTIVE_READ_COMPOSITION_V3 } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";

const APPLY = process.argv.includes("--apply");

function unwrap(report) {
  return report?.payload && typeof report.payload === "object" ? report.payload : report;
}

function promote(er) {
  if (!er || typeof er !== "object") return { changed: false, er };
  if (er.compositionVersion === ADP_EXECUTIVE_READ_COMPOSITION_V3 && er.sections) {
    return { changed: false, er, already: true };
  }
  const v3 = er.compositionV3;
  if (!(v3?.ok && v3?.sections)) return { changed: false, er, missing: true };
  return {
    changed: true,
    er: {
      ...er,
      compositionVersion: v3.compositionVersion || ADP_EXECUTIVE_READ_COMPOSITION_V3,
      compositionContract: v3.compositionContract || ADP_EXECUTIVE_READ_COMPOSITION_V3,
      sections: v3.sections,
      numericAnchors: v3.numericAnchors,
      primaryIssueId: v3.primaryIssueId,
      compositionHash: v3.compositionHash,
      sourceSnapshotHash: v3.sourceSnapshotHash,
      evidenceTrace: v3.evidenceTrace,
      qualityGates: v3.qualityGates,
      insightArchetype: v3.insightArchetype,
    },
  };
}

function main() {
  const rows = [];
  for (const propertyId of listPublishedPropertyIds()) {
    const manifest = loadPublishedManifest(propertyId);
    const reportFile = manifest?.reportFile;
    if (!reportFile) {
      rows.push({ propertyId, ok: false, reason: "NO_REPORT_FILE" });
      continue;
    }
    const path = join(process.cwd(), "data/ai-demand-positioning/published", propertyId, reportFile);
    const report = JSON.parse(readFileSync(path, "utf8"));
    const payload = unwrap(report);
    const result = promote(payload.executiveRead);
    if (result.changed && APPLY) {
      payload.executiveRead = result.er;
      if (report.payload) report.payload = payload;
      else Object.assign(report, payload);
      report.compositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;
      writeFileSync(path, JSON.stringify(report, null, 2) + "\n");
      if (manifest) {
        const mPath = join(process.cwd(), "data/ai-demand-positioning/published", propertyId, "manifest.json");
        const m = JSON.parse(readFileSync(mPath, "utf8"));
        m.executiveReadCompositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;
        m.executiveReadCompositionHash = result.er.compositionHash || null;
        writeFileSync(mPath, JSON.stringify(m, null, 2) + "\n");
      }
    }
    rows.push({
      propertyId,
      ok: true,
      changed: Boolean(result.changed),
      already: Boolean(result.already),
      missing: Boolean(result.missing),
      applied: APPLY && Boolean(result.changed),
    });
  }
  console.log(
    JSON.stringify(
      {
        ok: true,
        apply: APPLY,
        promoted: rows.filter((r) => r.changed).length,
        already: rows.filter((r) => r.already).length,
        missing: rows.filter((r) => r.missing).length,
        rows,
      },
      null,
      2
    )
  );
}

main();
