/**
 * Materialize Operator Explorer factory content packs into fixtures/*.json.
 * Default dry-run. Does not write Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { GHL_HOTELES_FACTORY_CONTENT, FACTORY_CONTENT_VERSION } from "./operator-explorer-factory-content-ghl-hoteles.js";
import { AIMBRIDGE_LATAM_FACTORY_CONTENT } from "./operator-explorer-factory-content-aimbridge-latam.js";
import { BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG } from "./operator-explorer-factory-content-brand-managed.js";
import { PLAYA_FACTORY_CONTENT } from "./operator-explorer-factory-content-playa.js";
import {
  buildAimbridgePrefillExtras,
  buildGhlPrefillExtras,
} from "./operator-explorer-factory-content-prefill-extras.js";

export { FACTORY_CONTENT_VERSION };

const PREFILL_EXTRAS_BUILDERS = Object.freeze({
  "ghl-hoteles": buildGhlPrefillExtras,
  "aimbridge-latam": buildAimbridgePrefillExtras,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export const FACTORY_CONTENT_PACKS = Object.freeze({
  "ghl-hoteles": GHL_HOTELES_FACTORY_CONTENT,
  "aimbridge-latam": AIMBRIDGE_LATAM_FACTORY_CONTENT,
  ...BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG,
  "playa-hotels-resorts": PLAYA_FACTORY_CONTENT,
});

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * @param {object} pack
 * @param {{ recordId?: string|null }} [opts]
 */
export function buildFactoryFixtureFiles(pack, opts = {}) {
  const recordId = opts.recordId || pack.recordId || null;
  const files = [];

  for (const [stem, payload] of Object.entries(pack.fixtures || {})) {
    const body = clone(payload);
    if (body._meta) {
      body._meta.factoryContentVersion = FACTORY_CONTENT_VERSION;
      body._meta.factorySlug = pack.slug;
      if (recordId) body._meta.operatorMasterId = recordId;
    }
    if (body.operatorMasterId !== undefined && recordId) {
      body.operatorMasterId = recordId;
    }
    const filename = `${stem}-${pack.suffix}.json`;
    files.push({
      relativePath: `fixtures/${filename}`,
      payload: body,
    });
  }

  if (Array.isArray(pack.diligence) && pack.diligence.length) {
    files.push({
      relativePath: `fixtures/operator-diligence-qa-${pack.suffix}.json`,
      payload: pack.diligence.map((row) => ({
        ...row,
        operatorMasterId: recordId,
        companyName: pack.companyName,
      })),
    });
  }

  const extrasBuilder = PREFILL_EXTRAS_BUILDERS[pack.slug];
  if (extrasBuilder) {
    files.push({
      relativePath: `fixtures/operator-prefill-extras-${pack.suffix}.json`,
      payload: {
        _meta: {
          operatorMasterId: recordId,
          operatorName: pack.companyName,
          factorySlug: pack.slug,
          note: "Flat prefill extras for Tab Factory field coverage",
        },
        ...extrasBuilder(),
      },
    });
  }

  return files;
}

/**
 * @param {{
 *   operators?: string[],
 *   apply?: boolean,
 *   approveMaterialize?: boolean,
 *   recordIds?: Record<string, string>
 * }} [opts]
 */
export function runOperatorExplorerFactoryContentMaterialize(opts = {}) {
  const apply = opts.apply === true;
  if (apply && !opts.approveMaterialize) {
    throw new Error("Apply requires --approve-operator-factory-content-materialize");
  }

  const operators = opts.operators?.length
    ? opts.operators
    : Object.keys(FACTORY_CONTENT_PACKS);

  const results = [];
  for (const slug of operators) {
    const pack = FACTORY_CONTENT_PACKS[slug];
    if (!pack) throw new Error(`Unknown factory content pack: ${slug}`);
    const recordId = opts.recordIds?.[slug] || pack.recordId;
    const planned = buildFactoryFixtureFiles(pack, { recordId });
    const written = [];
    if (apply) {
      for (const file of planned) {
        const abs = path.join(ROOT, file.relativePath);
        fs.mkdirSync(path.dirname(abs), { recursive: true });
        fs.writeFileSync(abs, JSON.stringify(file.payload, null, 2) + "\n");
        written.push(file.relativePath);
      }
    }
    results.push({
      operatorSlug: pack.slug,
      recordId: recordId || null,
      companyName: pack.companyName,
      domain: pack.domain,
      filesPlanned: planned.map((f) => f.relativePath),
      filesWritten: written,
      intentionalSuppressKeys: Object.keys(pack.intentionalSuppress || {}),
    });
  }

  return {
    version: FACTORY_CONTENT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    writeKind: apply ? "fixture_json" : "none",
    results,
    summary: {
      operators: results.length,
      filesPlanned: results.reduce((n, r) => n + r.filesPlanned.length, 0),
      filesWritten: results.reduce((n, r) => n + r.filesWritten.length, 0),
    },
  };
}

export function writeFactoryContentMaterializeReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-explorer-factory-content-materialize.json");
  const mdPath = path.join(reportsDir, "operator-explorer-factory-content-materialize.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Explorer factory content materialize",
    "",
    `dryRun: **${report.dryRun}** · filesWritten: **${report.summary.filesWritten}**`,
    "",
  ];
  for (const r of report.results) {
    lines.push(`## ${r.companyName} (\`${r.operatorSlug}\`)`, "");
    lines.push(`- recordId: \`${r.recordId || "TBD"}\``);
    for (const f of r.filesPlanned) lines.push(`- ${f}`);
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
