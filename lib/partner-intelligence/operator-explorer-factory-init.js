/**
 * Operator Explorer factory-init — scaffold fixture stubs + provenance domain for a queue operator.
 * Default dry-run. Does not write Airtable. Does not modify protected quality baselines.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getOperatorFactoryQueueEntry,
  listOperatorFactoryQueue,
} from "./operator-explorer-factory-queue.js";
import {
  isProtectedOperatorQualityBaseline,
} from "./operator-explorer-quality-baseline.js";
import { CANONICAL_OPERATOR_SOURCE_RULES } from "./operator-explorer-source-provenance-by-tab.js";

export const OPERATOR_FACTORY_INIT_VERSION = "operator-factory-init-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/** Fixture file stems (suffix appended as -{suffix}.json). */
export const FACTORY_INIT_FIXTURE_STEMS = Object.freeze([
  "operator-profile-explorer",
  "operator-operating-explorer",
  "operator-brand-explorer",
  "operator-markets-explorer",
  "operator-engagement-explorer",
  "operator-infrastructure-explorer",
  "operator-leadership-explorer",
  "operator-best-fit",
  "operator-recognition-explorer",
]);

function fixtureSuffix(slug) {
  if (slug === "ghl-hoteles") return "ghl-hoteles";
  if (slug === "viento-sur-gestion-hotelera") return "viento-sur";
  if (slug === "aimbridge-latam") return "aimbridge-latam";
  return String(slug || "").replace(/[^a-z0-9-]+/gi, "-").toLowerCase();
}

function stubPayload(entry, stem) {
  const homepage = entry.domain ? `https://www.${entry.domain}/` : "";
  return {
    _meta: {
      operatorMasterId: entry.recordId,
      operatorName: entry.companyName,
      factorySlug: entry.slug,
      factoryInitVersion: OPERATOR_FACTORY_INIT_VERSION,
      sourceUrl: homepage || undefined,
      note:
        "Tab Factory scaffold — fill tab-by-tab against Arbor/HE quality bar. Empty bags are intentional until content pass.",
      status: "scaffold",
    },
    // Empty bags so loader can merge later without inventing copy
    ...(stem.includes("profile") ? { profileFields: {} } : {}),
    ...(stem.includes("operating") ? { platformFields: {} } : {}),
    ...(stem.includes("brand") ? { brandFields: {} } : {}),
    ...(stem.includes("markets") ? { marketsFields: {} } : {}),
    ...(stem.includes("engagement") ? { engagementFields: {} } : {}),
    ...(stem.includes("infrastructure") ? { infrastructureFields: {} } : {}),
    ...(stem.includes("leadership") ? { leadershipFields: {} } : {}),
    ...(stem.includes("best-fit") ? { bestFitFields: {}, commercialFields: {} } : {}),
    ...(stem.includes("recognition") ? { recognitionFields: {} } : {}),
  };
}

/**
 * @param {{
 *   operators?: string[],
 *   apply?: boolean,
 *   approveFactoryInit?: boolean
 * }} [opts]
 */
export function runOperatorExplorerFactoryInit(opts = {}) {
  const apply = opts.apply === true;
  const approve = opts.approveFactoryInit === true;
  if (apply && !approve) {
    throw new Error("Apply requires --approve-operator-explorer-factory-init");
  }

  const operators =
    opts.operators?.length > 0
      ? opts.operators
      : listOperatorFactoryQueue({ status: "queued" }).slice(0, 1).map((o) => o.slug);

  const results = [];
  for (const id of operators) {
    if (isProtectedOperatorQualityBaseline(id)) {
      throw new Error(
        `Refusing factory-init on protected quality baseline "${id}". Use baseline revision workflow instead.`
      );
    }
    const entry = getOperatorFactoryQueueEntry(id);
    if (!entry) {
      throw new Error(
        `Operator "${id}" is not in OPERATOR_FACTORY_QUEUE. Register slug/recordId/domain first.`
      );
    }
    if (!entry.domain) {
      throw new Error(
        `Factory-init blocked for "${entry.slug}": confirm official domain before scaffolding (provenance gate).`
      );
    }

    const suffix = fixtureSuffix(entry.slug);
    const fixturesDir = path.join(ROOT, "fixtures");
    const planned = FACTORY_INIT_FIXTURE_STEMS.map((stem) => {
      const filename = `${stem}-${suffix}.json`;
      const abs = path.join(fixturesDir, filename);
      const exists = fs.existsSync(abs);
      return {
        stem,
        filename,
        relativePath: path.relative(ROOT, abs).replace(/\\/g, "/"),
        exists,
        action: exists ? "skip_existing" : apply ? "create" : "would_create",
        payload: exists ? null : stubPayload(entry, stem),
      };
    });

    const domainRulePresent = Boolean(CANONICAL_OPERATOR_SOURCE_RULES[entry.slug]);
    const created = [];
    if (apply) {
      fs.mkdirSync(fixturesDir, { recursive: true });
      for (const file of planned) {
        if (file.exists) continue;
        fs.writeFileSync(
          path.join(ROOT, file.relativePath),
          JSON.stringify(file.payload, null, 2) + "\n"
        );
        created.push(file.relativePath);
      }
    }

    results.push({
      operatorSlug: entry.slug,
      recordId: entry.recordId,
      companyName: entry.companyName,
      domain: entry.domain,
      explorerUrl: entry.explorerUrl,
      fixtureSuffix: suffix,
      domainRulePresent,
      domainRuleNote: domainRulePresent
        ? "CANONICAL_OPERATOR_SOURCE_RULES already includes this slug"
        : "Add domain rule in operator-explorer-source-provenance-by-tab.js before provenance audit",
      files: planned.map(({ payload, ...rest }) => rest),
      created,
      nextSteps: [
        "Fill Source Pack (company-controlled first)",
        "Tab-by-tab content vs Arbor + Hotel Equities bar",
        "npm run operator-explorer-tab-factory-audit -- --source=fixtures (after fixtures load path supports this slug)",
        "npm run operator-explorer-os -- --dry-run",
        "Founder visual review only after auditPass + pattern + provenance",
      ],
    });
  }

  return {
    version: OPERATOR_FACTORY_INIT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    writeKind: apply ? "fixture_scaffold_json" : "none",
    operators,
    results,
    summary: {
      operators: results.length,
      filesWouldCreate: results.reduce(
        (n, r) => n + r.files.filter((f) => f.action === "would_create").length,
        0
      ),
      filesCreated: results.reduce((n, r) => n + r.created.length, 0),
      filesSkippedExisting: results.reduce(
        (n, r) => n + r.files.filter((f) => f.action === "skip_existing").length,
        0
      ),
    },
  };
}

export function writeOperatorExplorerFactoryInitReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-explorer-factory-init.json");
  const mdPath = path.join(reportsDir, "operator-explorer-factory-init.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Explorer factory-init",
    "",
    `Version: \`${report.version}\` · dryRun: **${!report.applyPerformed}**`,
    `Generated: ${report.generatedAt}`,
    "",
    `Would create: ${report.summary.filesWouldCreate} · Created: ${report.summary.filesCreated} · Skipped existing: ${report.summary.filesSkippedExisting}`,
    "",
  ];
  for (const r of report.results) {
    lines.push(
      `## ${r.companyName} (\`${r.operatorSlug}\`)`,
      "",
      `- recordId: \`${r.recordId}\``,
      `- domain: \`${r.domain}\``,
      `- explorer: ${r.explorerUrl}`,
      `- domain rule present: ${r.domainRulePresent}`,
      "",
      "| File | Action |",
      "| --- | --- |"
    );
    for (const f of r.files) {
      lines.push(`| \`${f.relativePath}\` | ${f.action} |`);
    }
    lines.push("", "Next:", ...r.nextSteps.map((s) => `- ${s}`), "");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
