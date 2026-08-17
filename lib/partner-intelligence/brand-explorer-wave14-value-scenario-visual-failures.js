/**
 * Wave 14 — Failure extraction for "Where This Brand Creates the Most Value"
 * (overview.scenario.1–3) on the eight active public brands.
 *
 * Read-only. Does not patch Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
} from "./brand-explorer-image-uniqueness.js";
import { detectVisualCategory } from "./brand-explorer-image-role-match.js";
import { SCENARIO_SLOTS, words } from "./brand-explorer-scenario-owner-value-bar.js";
import {
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  WAVE14_VALUE_SCENARIO_TARGET_SLUGS,
  getWave14ValueScenarioVisualPackage,
} from "./brand-explorer-wave14-value-scenario-visual-packages.js";

export const WAVE14_VALUE_SCENARIO_VISUAL_FAILURES_VERSION =
  "wave14-value-scenario-visual-failures-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

const FAILURE_RES = Object.freeze([
  {
    type: "generic Bonvoy/platform card",
    re: /\bBonvoy Reach\b|\bNetwork Reach With\b|\bNetwork Lift Without\b|Marriott Bonvoy (and Marriott International )?(distribution|strengthen)/i,
  },
  {
    type: "internal diligence language",
    re: /owner-fit diligence|keep .{0,40} out of (the )?(same )?(diligence|underwriting) file|diligence file|brand responsibilities|deliverable after affiliation/i,
  },
  {
    type: "“avoid / do not reuse” language",
    re: /\bdo not reuse\b|\bavoid borrowing\b|\bavoid copying\b|\bdo not borrow\b|\bkeep .{0,30} out of this file\b/i,
  },
  {
    type: "section/process language",
    re: /sequence (design|systems|standards|PIP|training)|operating (model )?discipline after affiliation|operating efficiency after affiliation/i,
  },
  {
    type: "card explains operations instead of investment value",
    re: /sequence .{0,40} so affiliation|systems and training|housekeeping.{0,40}match|ramp-up/i,
  },
  {
    type: "copy not specific to brand",
    re: /^Marriott Bonvoy strengthens distribution only when/i,
  },
]);

const CROSS_BRAND_THIRD_CARD_RE =
  /\bBonvoy Reach\b|\bNetwork Reach With\b|\bNetwork Lift Without\b|Operating (Model )?Discipline After Affiliation|Operating Efficiency After Affiliation|Sibling Clarity/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function findSlot(rows, slotKey) {
  const matches = (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
  return matches[0] || null;
}

function classifyFailures({ title, body, imageUrl, imageFilename, slotIndex, identities, categories }) {
  const failures = [];
  const text = `${title}\n${body}`;

  for (const { type, re } of FAILURE_RES) {
    if (re.test(text)) failures.push(type);
  }
  if (words(body) > 95) failures.push("copy too long");
  if (words(body) > 0 && words(body) < 40) failures.push("weak owner-value example");
  if (slotIndex === 2 && CROSS_BRAND_THIRD_CARD_RE.test(title)) {
    failures.push("repeated cross-brand scenario pattern");
  }

  const id = identities[slotIndex];
  if (!imageUrl) {
    failures.push("image does not match scenario");
  } else {
    const peers = identities.filter(
      (other, j) => j !== slotIndex && other && other.duplicateGroupId === id?.duplicateGroupId
    );
    if (peers.length) failures.push("repeated image within brand");

    const nearPeers = identities.filter(
      (other, j) =>
        j !== slotIndex &&
        other &&
        id?.nearDuplicateGroupId &&
        other.nearDuplicateGroupId === id.nearDuplicateGroupId
    );
    if (nearPeers.length && !failures.includes("repeated image within brand")) {
      failures.push("near-duplicate image within brand");
    }

    // Same visual category twice (common founder screenshot issue)
    const cat = categories[slotIndex];
    if (cat && cat !== "unknown") {
      const sameCat = categories.filter((c, j) => j !== slotIndex && c === cat).length;
      if (sameCat > 0 && /property_setting|unknown/.test(cat)) {
        failures.push("near-duplicate image within brand");
      }
    }
  }

  return [...new Set(failures)];
}

export async function extractWave14ValueScenarioVisualFailures({
  brands = WAVE14_VALUE_SCENARIO_TARGET_SLUGS,
} = {}) {
  const brandRows = [];
  const tableRows = [];

  for (const slug of brands) {
    if (slug === WAVE14_HELD_PROMOTION_SLUG) continue;
    const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    const pkg = getWave14ValueScenarioVisualPackage(slug);
    if (!identity?.recordId) {
      brandRows.push({
        brandSlug: slug,
        blocked: true,
        blockers: ["unknown_identity"],
        scenarios: [],
      });
      continue;
    }

    const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
    const live = SCENARIO_SLOTS.map((sk) => findSlot(fetch.rows || [], sk));
    const identities = live.map((r, i) =>
      r?.imageUrl
        ? buildImageIdentity(r.imageUrl, {
            title: r.title || "",
            slotKey: SCENARIO_SLOTS[i],
            filename: r.imageFilename || r.filename || "",
          })
        : null
    );
    const categories = live.map((r) =>
      r?.imageUrl
        ? detectVisualCategory({
            imageUrl: r.imageUrl,
            title: r.title || "",
            filename: r.imageFilename || r.filename || "",
          }).category
        : null
    );

    const uniq = evaluateImageUniqueness({
      brandSlug: slug,
      brand: { name: identity.name, slug },
      presentationRows: live.filter(Boolean).map((r, i) => ({
        ...r,
        slotKey: SCENARIO_SLOTS[i],
      })),
    });

    const scenarios = live.map((r, i) => {
      const title = nz(r?.title);
      const body = nz(r?.body);
      const imageUrl = r?.imageUrl || null;
      const visualRole = categories[i] || "unknown";
      const failureTypes = classifyFailures({
        title,
        body,
        imageUrl,
        imageFilename: r?.imageFilename,
        slotIndex: i,
        identities,
        categories,
      });
      const proposed = pkg?.overviewScenarios?.[i] || null;
      const row = {
        brandSlug: slug,
        brandName: identity.name,
        scenarioNumber: i + 1,
        currentTitle: title,
        currentBody: body,
        currentImageUrl: imageUrl,
        currentImageFilename: r?.imageFilename || null,
        visualRole,
        duplicateGroupId: identities[i]?.duplicateGroupId || null,
        failureTypes,
        proposedReplacement: proposed
          ? {
              title: proposed.title,
              body: proposed.body,
              imageRole: proposed.imageRole,
              imageCaption: proposed.imageCaption,
            }
          : null,
      };
      tableRows.push(row);
      return row;
    });

    brandRows.push({
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      scenarioUniquenessPass: uniq.pass,
      scenarioDistinctCount: uniq.scenarioDistinctCount,
      uniquenessFindings: (uniq.findings || []).filter((f) => /scenario|duplicate/.test(f.id)),
      scenarios,
      failureCount: scenarios.reduce((n, s) => n + s.failureTypes.length, 0),
    });
  }

  const report = {
    version: WAVE14_VALUE_SCENARIO_VISUAL_FAILURES_VERSION,
    generatedAt: new Date().toISOString(),
    scope: {
      activeWave14: [...WAVE14_PARTIAL_PROMOTION_SLUGS],
      heldReadOnly: WAVE14_HELD_PROMOTION_SLUG,
      audited: [...brands].filter((s) => s !== WAVE14_HELD_PROMOTION_SLUG),
    },
    summary: {
      brands: brandRows.length,
      rowsWithFailures: tableRows.filter((r) => r.failureTypes.length).length,
      totalFailureTags: tableRows.reduce((n, r) => n + r.failureTypes.length, 0),
    },
    brands: brandRows,
    table: tableRows,
    readyStatement: "wave14_value_scenario_visual_failures_extracted_awaiting_remediation",
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-value-scenario-visual-failures.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-value-scenario-visual-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, renderFailuresMd(report), "utf8");
  return { ...report, paths: { jsonPath, mdPath } };
}

function renderFailuresMd(report) {
  const lines = [
    `# Wave 14 — Value Scenario Visual Failures`,
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Ready: \`${report.readyStatement}\``,
    "",
    `Brands audited: ${report.summary.brands} · Rows with failures: ${report.summary.rowsWithFailures}`,
    "",
    `Held (not patched): \`${report.scope.heldReadOnly}\``,
    "",
    `| Brand | # | Current Title | Visual Role | Failure Types | Proposed Title |`,
    `| --- | --- | --- | --- | --- | --- |`,
  ];
  for (const r of report.table || []) {
    lines.push(
      `| ${r.brandName} | ${r.scenarioNumber} | ${r.currentTitle || "_(blank)_"} | ${r.visualRole} | ${(r.failureTypes || []).join("; ") || "—"} | ${r.proposedReplacement?.title || "—"} |`
    );
  }
  lines.push("", "## Detail", "");
  for (const b of report.brands || []) {
    lines.push(`### ${b.brandName} (\`${b.brandSlug}\`)`, "");
    for (const s of b.scenarios || []) {
      lines.push(`**Scenario ${s.scenarioNumber}: ${s.currentTitle || "(blank)"}**`);
      lines.push("");
      lines.push(`- Failures: ${(s.failureTypes || []).join(", ") || "none"}`);
      lines.push(`- Image: \`${(s.currentImageUrl || "").slice(0, 100)}\``);
      lines.push(`- Body: ${((s.currentBody || "").slice(0, 220) || "_(empty)_").replace(/\n/g, " ")}`);
      if (s.proposedReplacement) {
        lines.push(`- Proposed: **${s.proposedReplacement.title}** (${s.proposedReplacement.imageRole})`);
      }
      lines.push("");
    }
  }
  return `${lines.join("\n")}\n`;
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]).includes("brand-explorer-wave14-value-scenario-visual-failures");

if (isMain) {
  extractWave14ValueScenarioVisualFailures()
    .then((r) => {
      console.log(JSON.stringify({ ready: r.readyStatement, paths: r.paths, summary: r.summary }, null, 2));
    })
    .catch((err) => {
      console.error(err);
      process.exitCode = 1;
    });
}
