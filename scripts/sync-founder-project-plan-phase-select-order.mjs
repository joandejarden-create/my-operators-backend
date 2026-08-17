/**
 * Reorder Phase single-select options to match Roadmap Phase Number (1–15).
 * Airtable group-by-Phase uses this option order, not Phase Number.
 *
 *   node scripts/sync-founder-project-plan-phase-select-order.mjs --dry-run
 *   node scripts/sync-founder-project-plan-phase-select-order.mjs --execute
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ROADMAP_PHASE_INDEX } from "../lib/dealality-master-todo/founder-project-plan-phase-order.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const TABLE_ID = "tblpCg0QZ0kIPXihE";
const PHASE_FIELD_NAME = "Phase";
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-phase-select-order-report.json");

/** Roadmap order first; any extra Airtable options append at end. */
export const ROADMAP_PHASE_SELECT_ORDER = Object.entries(ROADMAP_PHASE_INDEX)
  .sort((a, b) => a[1] - b[1])
  .map(([name]) => name);

function getConfig() {
  const token = (
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_GTM_API_KEY ||
    ""
  ).trim();
  const baseId = (
    process.env.AIRTABLE_GTM_BASE_ID ||
    process.env.AIRTABLE_BASE_ID ||
    "appKZuK006BWIVjNW"
  ).trim();
  if (!token) throw new Error("Missing AIRTABLE_PAT / AIRTABLE_TOKEN");
  return { token, baseId };
}

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

export function buildReorderedPhaseChoices(existingChoices) {
  const byName = new Map((existingChoices || []).map((c) => [c.name, c]));
  const ordered = [];
  const used = new Set();

  for (const name of ROADMAP_PHASE_SELECT_ORDER) {
    const choice = byName.get(name);
    if (choice) {
      ordered.push({ id: choice.id, name: choice.name });
      used.add(name);
    }
  }

  for (const choice of existingChoices || []) {
    if (!used.has(choice.name)) {
      ordered.push({ id: choice.id, name: choice.name });
    }
  }

  return ordered;
}

async function main() {
  const { token, baseId } = getConfig();
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.id === TABLE_ID);
  if (!table) throw new Error("Founder Project Plan table not found");

  const phaseField = (table.fields || []).find((f) => f.name === PHASE_FIELD_NAME);
  if (!phaseField) throw new Error("Phase field not found");

  const before = (phaseField.options?.choices || []).map((c) => c.name);
  const reordered = buildReorderedPhaseChoices(phaseField.options?.choices || []);
  const after = reordered.map((c) => c.name);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    fieldId: phaseField.id,
    before,
    after,
    changed: before.join("|") !== after.join("|"),
    roadmapOrder: ROADMAP_PHASE_SELECT_ORDER,
  };

  if (!report.changed) {
    console.log("Phase select options already in roadmap order.");
    fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
    return;
  }

  if (EXECUTE) {
    const { res, json } = await metaFetch(
      baseId,
      token,
      `/tables/${TABLE_ID}/fields/${phaseField.id}`,
      {
        method: "PATCH",
        body: JSON.stringify({
          type: "singleSelect",
          options: { choices: reordered },
        }),
      }
    );
    if (!res.ok) {
      report.error = json;
      fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
      console.error("PATCH failed:", res.status, JSON.stringify(json));
      process.exit(1);
    }
    report.patched = true;
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nPhase select order (${report.mode})`);
  console.log("Before:", before.join(" → "));
  console.log("After: ", after.join(" → "));
  console.log(`Report: ${REPORT_PATH}`);
  console.log(
    "\nIn Airtable: when grouping by Phase, sections now follow roadmap order (1–15)."
  );
  console.log("Still sort rows within view by Roadmap Sort ↑.");
}

main().catch((err) => {
  console.error("[sync-founder-project-plan-phase-select-order]", err.message || err);
  process.exit(1);
});
