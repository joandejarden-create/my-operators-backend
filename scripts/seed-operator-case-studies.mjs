/**
 * Seed Operator Setup case study child rows from a JSON fixture.
 *
 * Prerequisite: add columns on **Operator Setup - Case Studies** (and legacy
 * **3rd Party Operator - Case Studies** if used): `challenge`, `data_status`
 * (long text). See docs/operator-case-study-airtable-fields.md.
 *
 * Usage:
 *   node scripts/seed-operator-case-studies.mjs
 *   node scripts/seed-operator-case-studies.mjs recTUjuDxL96yWcQA
 *   node scripts/seed-operator-case-studies.mjs recTUjuDxL96yWcQA fixtures/operator-case-studies-antillano-norte.json
 *   node scripts/seed-operator-case-studies.mjs recTUjuDxL96yWcQA --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import { mapCaseStudyDetailToNewBaseChildRow } from "../api/lib/operator-case-study-airtable-map.js";
import { replaceOperatorCaseStudies } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recTUjuDxL96yWcQA";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-case-studies-antillano-norte.json");

function parseArgs(argv) {
  const flags = new Set();
  const pos = [];
  for (const a of argv.slice(2)) {
    if (a.startsWith("--")) flags.add(a);
    else pos.push(a);
  }
  return {
    masterId: (pos[0] || DEFAULT_MASTER).trim(),
    fixturePath: (pos[1] || DEFAULT_FIXTURE).trim(),
    dryRun: flags.has("--dry-run"),
    omitExtendedFields: flags.has("--omit-extended-fields"),
    flags,
  };
}

async function writeCaseStudies(masterId, rows, correlationId) {
  try {
    return await replaceOperatorCaseStudies(masterId, rows, correlationId);
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    if (/Unknown field name/i.test(msg) && rows.some((r) => r.challenge != null || r.data_status != null)) {
      console.warn(
        "Retrying without challenge / data_status — add those columns in Airtable, then re-run without --omit-extended-fields."
      );
      const slim = rows.map((r) =>
        mapCaseStudyDetailToNewBaseChildRow(
          {
            property_name: r.property_name,
            hotel_type: r.hotel_type,
            region: r.region,
            branded_independent: r.branded_independent,
            situation: r.situation,
            services: r.services,
            outcome: r.outcome,
            owner_relevance: r.owner_relevance,
            image_url: r.image_url,
          },
          (r.display_order || 1) - 1,
          { omitExtendedFields: true }
        )
      );
      return replaceOperatorCaseStudies(masterId, slim, correlationId);
    }
    throw e;
  }
}

function main() {
  const { masterId, fixturePath, dryRun, omitExtendedFields, flags } = parseArgs(process.argv);
  const raw = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const list = Array.isArray(raw.caseStudiesDetail) ? raw.caseStudiesDetail : [];
  if (!list.length) {
    console.error("No caseStudiesDetail rows in fixture:", fixturePath);
    process.exit(1);
  }

  const rows = list
    .map((item, idx) =>
      mapCaseStudyDetailToNewBaseChildRow(item, idx, {
        omitExtendedFields: omitExtendedFields,
      })
    )
    .filter((r) =>
      Object.entries(r).some(([k, v]) => k !== "display_order" && v)
    );

  console.log(
    JSON.stringify(
      {
        masterId,
        fixturePath,
        dryRun,
        omitExtendedFields,
        rowCount: rows.length,
        preview: rows.map((r) => ({
          property_name: r.property_name,
          challenge: (r.challenge || "").slice(0, 80) + "…",
          data_status: r.data_status,
        })),
      },
      null,
      2
    )
  );

  if (dryRun) {
    console.log("\nDry run — no Airtable writes.");
    return;
  }

  const correlationId = randomUUID();
  writeCaseStudies(masterId, rows, correlationId)
    .then((res) => {
      console.log("\nCase studies replaced:", res);
      console.log(
        "Reload:",
        `http://localhost:8080/operator-dna-profile.html?operatorId=${masterId}`
      );
      if (omitExtendedFields) {
        console.log(
          "\nNote: challenge / data_status omitted. Add Airtable columns and re-run without --omit-extended-fields."
        );
      }
    })
    .catch((e) => {
      console.error("\nSeed failed:", e.message || e);
      console.error(
        "See docs/operator-case-study-airtable-fields.md for required columns."
      );
      process.exit(1);
    });
}

main();
