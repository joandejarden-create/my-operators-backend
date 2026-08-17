#!/usr/bin/env node
/**
 * Read-only failure extraction for Wave 12 post-release freeze cleanup.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TARGETS = ["bunkhouse-hotels", "moxy-hotels", "voco-hotels"];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function main() {
  const audit = JSON.parse(
    fs.readFileSync(path.join(ROOT, "reports", "brand-explorer-24-tab-section-quality-audit.json"), "utf8")
  );
  const failures = [];

  function add(brand, f, currentValue, why, fix, writeRequired) {
    failures.push({
      brand,
      tab: f.tab || f.section || "",
      section: f.section || f.card || "",
      recordId: f.recordId || null,
      slotKey: f.slotKey || null,
      field: f.field || (String(f.finding || "").includes("repeated_visual_role") ? "Title" : "Body"),
      failureType: f.status || f.issueType || f.finding || "",
      currentValue: nz(currentValue).slice(0, 280),
      whyItFails: why,
      proposedFix: fix,
      writeRequired: writeRequired === true,
      severity: f.severity || null,
    });
  }

  for (const slug of TARGETS) {
    const b = (audit.brandResults || []).find((x) => x.slug === slug);
    if (!b) continue;
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    const { rows } = await listPresentationRowsLight(id.recordId, id.name);
    const byId = new Map(rows.map((r) => [r.recordId, r]));

    for (const f of b.tabFindings || []) {
      if (f.severity !== "major" && !(slug === "voco-hotels" && f.status === "thin")) continue;
      if (f.status === "missing") {
        add(
          slug,
          f,
          "(no row)",
          "Optional section missing (also present on freeze-ready peers)",
          "No write — intentional suppression shared with approve_for_baseline_freeze peers",
          false
        );
        continue;
      }
      const row = f.recordId ? byId.get(f.recordId) : null;
      const cur = row ? nz(row.body) : "(missing row)";
      if (f.status === "wrong_brand" && /World of Hyatt/i.test(f.finding || "")) {
        add(
          slug,
          f,
          cur,
          "Literal World of Hyatt in owner-facing copy; slug lacks hyatt so scanner treats as wrong-brand carryover",
          "Replace with Bunkhouse-specific copy; label Hyatt only as parent-platform context",
          true
        );
      } else if (f.status === "wrong_brand" && /Marriott Bonvoy/i.test(f.finding || "")) {
        add(
          slug,
          f,
          cur,
          "Marriott Bonvoy present; Moxy is Marriott-affiliated but slug lacks marriott — validator false positive",
          "Targeted parent-platform allowlist for moxy-hotels (case B); do not scrub valid Bonvoy context",
          false
        );
      } else if (f.status === "thin") {
        add(
          slug,
          f,
          cur,
          "Body under 12-word thin threshold",
          "Thicken operations.operator_compat.tags with brand-specific chips",
          true
        );
      }
    }

    for (const f of b.imageFindings || []) {
      if (f.severity !== "major") continue;
      const scenarios = [1, 2, 3].map((i) => rows.find((r) => r.slotKey === `overview.scenario.${i}`));
      add(
        slug,
        { ...f, tab: "Where This Brand Creates the Most Value", field: "Title" },
        scenarios.map((r) => r?.title).join(" | "),
        "auditScenarioImageRoles classifies all three scenario titles as exterior_arrival (conversion/reposition keywords)",
        "Retitle scenarios to diversify detected roles (Title only; no image swap)",
        true
      );
    }
  }

  const report = {
    version: "wave12-post-release-freeze-cleanup-failures-v1",
    generatedAt: new Date().toISOString(),
    sourceAudit: "reports/brand-explorer-24-tab-section-quality-audit.json",
    targets: TARGETS,
    summary: {
      bunkhouse: {
        recommendation: "remediation_required",
        wrongBrandMajors: failures.filter((f) => f.brand === "bunkhouse-hotels" && f.failureType === "wrong_brand")
          .length,
        approach: "scrub World of Hyatt identity phrasing in Presentation Body",
        writeRequired: true,
      },
      moxy: {
        recommendation: "remediation_required",
        wrongBrandMajors: failures.filter((f) => f.brand === "moxy-hotels" && f.failureType === "wrong_brand").length,
        approach: "validator parent-platform allowlist (case B false positive)",
        writeRequired: false,
      },
      voco: {
        recommendation: "approve_after_minor_cleanup",
        approach: "retitle scenarios + thicken operator_compat.tags",
        writeRequired: true,
      },
    },
    failureCount: failures.length,
    writeRequiredCount: failures.filter((f) => f.writeRequired).length,
    failures,
  };

  const jsonPath = path.join(ROOT, "reports", "brand-explorer-wave12-post-release-freeze-cleanup-failures.json");
  const mdPath = path.join(ROOT, "reports", "brand-explorer-wave12-post-release-freeze-cleanup-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    `# Wave 12 Post-Release Freeze Cleanup — Failure Extraction`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `## Summary`,
    ``,
    `| Brand | Recommendation | Root cause | Write? |`,
    `| --- | --- | --- | --- |`,
    `| bunkhouse-hotels | remediation_required | World of Hyatt copy hits | Yes — Presentation Body |`,
    `| moxy-hotels | remediation_required | Marriott Bonvoy allowlist false positive | No content — validator allowlist |`,
    `| voco-hotels | approve_after_minor_cleanup | Scenario titles all exterior_arrival + thin tags | Yes — Title + tags Body |`,
    ``,
    `## Failures`,
    ``,
    `| Brand | Tab | Section | Record ID | Field | Type | Why | Fix | Write? |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const f of failures) {
    const esc = (s) => String(s || "").replace(/\|/g, "/").replace(/\n/g, " ");
    lines.push(
      `| ${f.brand} | ${esc(f.tab)} | ${esc(f.section)} | ${f.recordId || ""} | ${f.field} | ${esc(f.failureType)} | ${esc(f.whyItFails)} | ${esc(f.proposedFix)} | ${f.writeRequired} |`
    );
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(`failures=${failures.length} writeRequired=${report.writeRequiredCount}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
