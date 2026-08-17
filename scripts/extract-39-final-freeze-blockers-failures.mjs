/**
 * Extract exact freeze blockers for the final 39-brand cleanup (read-only).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const AUDIT = path.join(ROOT, "reports", "brand-explorer-24-tab-section-quality-audit.json");
const OUT_JSON = path.join(ROOT, "reports", "brand-explorer-39-final-freeze-blockers-failures.json");
const OUT_MD = path.join(ROOT, "reports", "brand-explorer-39-final-freeze-blockers-failures.md");

const IMAGE = [
  "voco-hotels",
  "avid-hotels",
  "holiday-inn-express",
  "vignette-collection",
];
const ADR = [
  "small-luxury-hotels-of-the-world",
  "suburban-studios",
  "trademark-collection-by-wyndham",
  "woodspring-suites",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function main() {
  const j = JSON.parse(fs.readFileSync(AUDIT, "utf8"));
  const failures = [];
  for (const slug of [...IMAGE, ...ADR]) {
    const b = (j.brandResults || []).find((x) => x.slug === slug);
    const lane = IMAGE.includes(slug) ? "image_uniqueness" : "adr_forbidden_language";
    if (!b) {
      failures.push({
        brand: slug,
        lane,
        tab: null,
        section: null,
        recordId: null,
        fieldOrImageSlot: null,
        failureType: "missing_from_audit",
        severity: "blocker",
        currentValueOrImage: null,
        proposedFix: "re-run quality audit",
        writeRequired: false,
      });
      continue;
    }
    for (const f of [...(b.tabFindings || []), ...(b.imageFindings || [])]) {
      const blob = JSON.stringify(f);
      if (lane === "adr_forbidden_language") {
        if (!(f.severity === "blocker" || /adr|forbidden/i.test(blob))) continue;
        if (/Guest Psychographics|missing.*expected slots/i.test(nz(f.finding)) && !/adr|forbidden/i.test(blob)) {
          continue;
        }
      } else {
        if (!(f.severity === "blocker" || f.severity === "major")) continue;
        if (/valueOwners\.overview|missing.*expected slots/i.test(nz(f.finding)) && !/gallery|scenario|near_duplicate|distinct/i.test(nz(f.finding))) {
          continue;
        }
        if (!/near_duplicate|distinct|duplicate|gallery|scenario|image/i.test(blob)) continue;
      }
      failures.push({
        brand: slug,
        lane,
        tab: f.tab || null,
        section: f.section || null,
        recordId: f.recordId || null,
        fieldOrImageSlot: f.slotKey || f.slots || f.card || null,
        failureType: f.finding || f.issueType || f.status,
        severity: f.severity || null,
        currentValueOrImage:
          f.currentCaption || (f.image ? String(f.image).slice(0, 120) : null) || null,
        proposedFix:
          f.proposedFix ||
          (lane === "image_uniqueness"
            ? "replace_duplicate_slot_with_scene7_aware_distinct_asset"
            : "scrub_adr_token_from_owner_facing_body"),
        writeRequired: true,
      });
    }
  }

  const report = {
    version: "brand-explorer-39-final-freeze-blockers-failures-v1",
    generatedAt: new Date().toISOString(),
    sourceAuditGeneratedAt: j.generatedAt,
    recommendationCounts: j.recommendationCounts,
    baselineFreezeDecision: j.baselineFreezeDecision,
    imageTargets: IMAGE,
    adrTargets: ADR,
    failureCount: failures.length,
    failures,
  };
  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Brand Explorer 39 — Final Freeze Blocker Failures",
    "",
    `Generated: ${report.generatedAt}`,
    `Source audit: ${report.sourceAuditGeneratedAt}`,
    `Counts: \`${JSON.stringify(report.recommendationCounts)}\``,
    "",
    "| Brand | Lane | Tab | Section | Record ID | Field / Image Slot | Failure Type | Current Value / Image | Proposed Fix |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const f of failures) {
    const slot = Array.isArray(f.fieldOrImageSlot)
      ? f.fieldOrImageSlot.join(", ")
      : f.fieldOrImageSlot || "—";
    const cur = nz(f.currentValueOrImage).replace(/\|/g, "/").slice(0, 80) || "—";
    const typ = nz(f.failureType).replace(/\|/g, "/").slice(0, 80);
    lines.push(
      `| ${f.brand} | ${f.lane} | ${f.tab || "—"} | ${f.section || "—"} | ${f.recordId || "—"} | ${slot} | ${typ} | ${cur} | ${f.proposedFix} |`
    );
  }
  fs.writeFileSync(OUT_MD, `${lines.join("\n")}\n`, "utf8");
  console.log(`Wrote ${OUT_JSON}`);
  console.log(`Wrote ${OUT_MD}`);
  console.log(`failureCount=${failures.length}`);
}

main();
