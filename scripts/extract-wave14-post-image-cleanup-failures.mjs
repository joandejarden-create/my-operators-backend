#!/usr/bin/env node
/**
 * Wave 14 Stage 6 — failure extraction (read-only).
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE14_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave14-factory-plan.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORTS = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJson(name) {
  const p = path.join(REPORTS, name);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function main() {
  const tab = readJson("brand-explorer-tab-factory-audit.json");
  const stage5 = readJson("brand-explorer-wave14-image-materialization.json");
  const failures = [];

  for (const slug of WAVE14_SLUGS) {
    const b = (tab?.brandResults || []).find((x) => x.brandSlug === slug);
    const spp = b?.sectionPatternParity?.sections?.recent_momentum || b?.sectionPatternParity?.recent_momentum;
    if (spp && spp.pass === false) {
      for (const f of spp.failures || [spp.failureReason || "recent_momentum_pattern"]) {
        failures.push({
          brand: slug,
          section: "Recent Momentum",
          recordId: null,
          field: "footprint.momentum",
          failureType: "recent_momentum_pattern",
          currentValue: String(f).slice(0, 200),
          proposedFix:
            "Rewrite momentum cards to dateLine\\n\\nsummary (≥35 words)\\n\\nhttps sourceUrl with structured dates (Directory|Mon YYYY) and geography labels",
          sourceSupport: "Wave 14 source packs + hotel-development.marriott.com / property overviews",
          stewardRequired: false,
        });
      }
    }
    for (const f of b?.findings || []) {
      if (f.status === "pass") continue;
      const isFlexOpenings =
        slug === "four-points-flex-by-sheraton" && /footprint\.openings|openings/i.test(`${f.reason}${f.fieldId}${f.slotKey}`);
      failures.push({
        brand: slug,
        section: f.tabName || f.tab || "Footprint & Growth",
        recordId: f.sourceRecordId || f.recordId || null,
        field: f.fieldName || f.slotKey || f.fieldId || null,
        failureType: isFlexOpenings ? "accepted_flex_openings_hold" : f.recommendedAction || f.status,
        currentValue: String(f.reason || f.detail || "").slice(0, 240),
        proposedFix: isFlexOpenings
          ? "Keep openings Do Not Display / cleanly unavailable — no Four Points by Sheraton substitutes"
          : String(f.proposedFix || f.reason || "Targeted Presentation patch").slice(0, 240),
        sourceSupport: isFlexOpenings ? "Stage 5 Flex hold documentation" : "Wave 14 curated content",
        stewardRequired: isFlexOpenings,
        acceptedHold: isFlexOpenings,
      });
    }
  }

  // Momentum evidence quality — from Stage 5/Stage 6 preflight narrative + known pattern
  const momentumEvidenceKnown = {
    "marriott-hotels": 7,
    sheraton: 7,
    westin: 7,
    "residence-inn-by-marriott": 7,
    "springhill-suites-by-marriott": 4,
    "towneplace-suites-by-marriott": 4,
    "aloft-hotels": 7,
    "four-points-flex-by-sheraton": 7,
    studiores: 6,
  };
  for (const [slug, n] of Object.entries(momentumEvidenceKnown)) {
    failures.push({
      brand: slug,
      section: "Recent Momentum",
      recordId: null,
      field: "footprint.momentum Body",
      failureType: "recent_momentum_evidence_quality",
      currentValue: `evidence_fail_count≈${n} (missing_or_invalid_date, missing_source_url, body_too_thin, intl label gaps)`,
      proposedFix:
        "Patch Body to structured momentum format; use Directory/Apr 2025 dates; embed https URLs; ensure ≥2 cards; label International Reference where non-CALA",
      sourceSupport: "Source-pack announcementUrl + property overview URLs",
      stewardRequired: false,
    });
  }

  // Stage 5 accepted holds
  failures.push({
    brand: "four-points-flex-by-sheraton",
    section: "Gallery / Openings",
    recordId: null,
    field: "materials.gallery / footprint.openings",
    failureType: "accepted_flex_source_limitation",
    currentValue: "gallery 4/6 · openings 0/3 held",
    proposedFix: "Document only — do not fill with Four Points by Sheraton imagery",
    sourceSupport: "reports/brand-explorer-wave14-image-materialization-four-points-flex-by-sheraton.md",
    stewardRequired: true,
    acceptedHold: true,
  });
  failures.push({
    brand: "springhill-suites-by-marriott",
    section: "Openings / Examples",
    recordId: null,
    field: "footprint.openings",
    failureType: "accepted_international_reference_openings",
    currentValue: "International Reference openings until steward-matched property URLs",
    proposedFix: "Ensure International Reference labels remain; no sibling-brand substitutes",
    sourceSupport: "Stage 4/5 acceptance notes",
    stewardRequired: true,
    acceptedHold: true,
  });
  failures.push({
    brand: "towneplace-suites-by-marriott",
    section: "Openings / Examples",
    recordId: null,
    field: "footprint.openings",
    failureType: "accepted_international_reference_openings",
    currentValue: "International Reference openings until steward-matched property URLs",
    proposedFix: "Ensure International Reference labels remain; no Residence Inn / StudioRes substitutes",
    sourceSupport: "Stage 4/5 acceptance notes",
    stewardRequired: true,
    acceptedHold: true,
  });

  const report = {
    version: "wave14-post-image-cleanup-failures-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    stage5Ready: stage5?.readyState || null,
    scope: [...WAVE14_SLUGS],
    summary: {
      total: failures.length,
      actionable: failures.filter((f) => !f.acceptedHold).length,
      acceptedHolds: failures.filter((f) => f.acceptedHold).length,
      primaryTheme: "recent_momentum_structured_body_dates_urls",
    },
    failures,
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-wave14-post-image-cleanup-failures.json");
  const mdPath = path.join(REPORTS, "brand-explorer-wave14-post-image-cleanup-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const lines = [
    `# Wave 14 Stage 6 — Post-Image Cleanup Failures`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `## Summary`,
    ``,
    `- Total rows: **${report.summary.total}**`,
    `- Actionable: **${report.summary.actionable}**`,
    `- Accepted holds: **${report.summary.acceptedHolds}**`,
    `- Primary theme: **${report.summary.primaryTheme}**`,
    ``,
    `## Failure table`,
    ``,
    `| Brand | Section | Record ID | Field | Failure Type | Current Value | Proposed Fix | Source Support | Steward? |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const f of failures) {
    lines.push(
      `| ${f.brand} | ${f.section} | ${f.recordId || "—"} | ${f.field || "—"} | ${f.failureType} | ${String(f.currentValue || "").replace(/\|/g, "/").slice(0, 80)} | ${String(f.proposedFix || "").replace(/\|/g, "/").slice(0, 100)} | ${String(f.sourceSupport || "").replace(/\|/g, "/").slice(0, 60)} | ${f.stewardRequired ? "yes" : "no"} |`
    );
  }
  lines.push("");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(JSON.stringify(report.summary));
}

main();
