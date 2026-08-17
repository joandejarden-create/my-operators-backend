/**
 * Create Duration (Days) formula field on Founder Project Plan.
 * Renames legacy number field to Duration (Days) [manual] if still named Duration (Days).
 *
 *   node scripts/ensure-founder-project-plan-duration-formula.mjs --dry-run
 *   node scripts/ensure-founder-project-plan-duration-formula.mjs --execute
 */
import "../load-env.js";

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;

const TABLE_ID = "tblpCg0QZ0kIPXihE";
const LEGACY_NUMBER_FIELD_ID = "fldfTxbVbQ5S6i7WU";
const PROBE_FIELD_ID = "fldRoijJKTFvSuPeB";
const START_FIELD = "Start";
const END_FIELD = "End";
const FORMULA_FIELD_NAME = "Duration (Days)";
const LEGACY_FIELD_NAME = "Duration (Days) [manual]";
const FORMULA = `IF(AND({${START_FIELD}}, {${END_FIELD}}), DATETIME_DIFF({${END_FIELD}}, {${START_FIELD}}, 'days'), BLANK())`;

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
    ""
  ).trim();
  if (!token) throw new Error("Set AIRTABLE_TOKEN or AIRTABLE_PAT.");
  if (!baseId) throw new Error("Set AIRTABLE_GTM_BASE_ID or AIRTABLE_BASE_ID.");
  return { token, baseId };
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
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

async function main() {
  const { token, baseId } = getConfig();
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed (${listRes.status}): ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.id === TABLE_ID);
  if (!table) throw new Error(`Founder Project Plan (${TABLE_ID}) not found in base ${baseId}`);

  const byName = new Map((table.fields || []).map((f) => [f.name, f]));
  const legacyNumber = byName.get("Duration (Days)") || byName.get(LEGACY_FIELD_NAME);
  const existingFormula = (table.fields || []).find(
    (f) => f.type === "formula" && (f.name === FORMULA_FIELD_NAME || f.name.includes("Duration"))
  );
  const probe = byName.get("Duration (Days) Calc Probe");

  const report = {
    mode: DRY_RUN ? "dry-run" : "execute",
    baseId,
    tableId: TABLE_ID,
    formula: FORMULA,
    actions: [],
    errors: [],
  };

  if (existingFormula?.type === "formula" && existingFormula.name === FORMULA_FIELD_NAME) {
    report.actions.push({ action: "skip", reason: "formula field already exists", fieldId: existingFormula.id });
    console.log("Duration (Days) formula field already exists:", existingFormula.id);
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  if (probe && !DRY_RUN) {
    const { res, json } = await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields/${probe.id}`, {
      method: "PATCH",
      body: JSON.stringify({ name: "__remove_duration_probe__" }),
    });
    report.actions.push({
      action: "rename_probe",
      fieldId: probe.id,
      ok: res.ok,
      status: res.status,
      error: res.ok ? null : json,
    });
  }

  if (legacyNumber?.type === "number" && legacyNumber.name === "Duration (Days)") {
    report.actions.push({ action: "rename_legacy_number", from: legacyNumber.name, to: LEGACY_FIELD_NAME });
    if (!DRY_RUN) {
      const { res, json } = await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields/${legacyNumber.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          name: LEGACY_FIELD_NAME,
          description: "Legacy manual duration entry. Hide or delete after verifying the Duration (Days) formula field.",
        }),
      });
      if (!res.ok) {
        report.errors.push({ action: "rename_legacy_number", status: res.status, error: json });
      }
    }
  }

  report.actions.push({
    action: "create_formula",
    name: FORMULA_FIELD_NAME,
    formula: FORMULA,
  });

  if (!DRY_RUN) {
    const { res, json } = await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields`, {
      method: "POST",
      body: JSON.stringify({
        name: FORMULA_FIELD_NAME,
        type: "formula",
        description: `Calculated days from ${START_FIELD} to ${END_FIELD}. Updates automatically when dates change.`,
        options: { formula: FORMULA },
      }),
    });
    if (res.ok) {
      report.actions.push({ action: "created_formula", fieldId: json.id, resolvedFormula: json.options?.formula });
      console.log("Created Duration (Days) formula field:", json.id);
      console.log("Resolved formula:", json.options?.formula);
    } else {
      report.errors.push({ action: "create_formula", status: res.status, error: json });
      console.error("Create formula failed:", res.status, JSON.stringify(json));
      process.exitCode = 1;
    }
  } else {
    console.log("[dry-run] Would create formula field:", FORMULA_FIELD_NAME);
    console.log("Formula:", FORMULA);
    if (legacyNumber?.name === "Duration (Days)") {
      console.log("[dry-run] Would rename legacy number field to:", LEGACY_FIELD_NAME);
    }
  }

  console.log("\nManual cleanup (Airtable UI):");
  console.log("  1. Verify formula values match expectations.");
  console.log("  2. Hide or delete field:", LEGACY_FIELD_NAME);
  console.log("  3. Delete probe fields if any remain (__remove_duration_probe__, Duration (Days) Calc Probe).");
  console.log("\nNo record data was changed.");
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
