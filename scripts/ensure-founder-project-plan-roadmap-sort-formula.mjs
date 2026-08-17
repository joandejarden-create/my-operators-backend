/**
 * Create Roadmap Sort formula on Founder Project Plan (Phase Number + Step Number).
 *
 *   node scripts/ensure-founder-project-plan-roadmap-sort-formula.mjs --dry-run
 *   node scripts/ensure-founder-project-plan-roadmap-sort-formula.mjs --execute
 */
import "../load-env.js";
import {
  ROADMAP_SORT_FIELD,
  ROADMAP_SORT_FORMULA_ONELINE,
} from "../lib/dealality-master-todo/founder-project-plan-roadmap-sort-formula.js";

const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = process.argv.includes("--dry-run") || !EXECUTE;
const TABLE_ID = "tblpCg0QZ0kIPXihE";

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

async function main() {
  const { token, baseId } = getConfig();
  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed (${listRes.status}): ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.id === TABLE_ID);
  if (!table) throw new Error(`Founder Project Plan (${TABLE_ID}) not found`);

  const existing = (table.fields || []).find((f) => f.name === ROADMAP_SORT_FIELD);
  if (existing?.type === "formula") {
    const current = existing.options?.formula || "";
    const needsRound = !current.includes("ROUND(");
    if (!needsRound) {
      console.log(`${ROADMAP_SORT_FIELD} formula OK:`, existing.id);
      return;
    }
    if (DRY_RUN) {
      console.log(`[dry-run] Would patch ${ROADMAP_SORT_FIELD} to add ROUND()`);
      console.log("New formula:", ROADMAP_SORT_FORMULA_ONELINE);
      return;
    }
    const { res, json } = await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields/${existing.id}`, {
      method: "PATCH",
      body: JSON.stringify({
        description:
          "Sort key: ROUND(Phase Number + Step Number/100). Steps follow logical roadmap order within each phase.",
        options: { formula: ROADMAP_SORT_FORMULA_ONELINE },
      }),
    });
    if (!res.ok) {
      console.error("Patch formula failed:", res.status, JSON.stringify(json));
      process.exit(1);
    }
    console.log(`Patched ${ROADMAP_SORT_FIELD} formula:`, json.id);
    console.log("Resolved formula:", json.options?.formula);
    return;
  }

  if (DRY_RUN) {
    console.log(`[dry-run] Would create formula field: ${ROADMAP_SORT_FIELD}`);
    console.log("Formula:", ROADMAP_SORT_FORMULA_ONELINE);
    console.log("\nExample values: phase 1 step 1 → 1.01 | phase 1 step 12 → 1.12 | phase 5 step 3 → 5.03");
    console.log("View sort: Roadmap Sort (asc) — single column.");
    return;
  }

  const { res, json } = await metaFetch(baseId, token, `/tables/${TABLE_ID}/fields`, {
    method: "POST",
    body: JSON.stringify({
      name: ROADMAP_SORT_FIELD,
      type: "formula",
      description:
        "Sort key: Phase Number + Step Number/100 (e.g. 1.01, 5.03). Sort this column ascending for full roadmap order.",
      options: { formula: ROADMAP_SORT_FORMULA_ONELINE },
    }),
  });

  if (!res.ok) {
    console.error("Create formula failed:", res.status, JSON.stringify(json));
    process.exit(1);
  }

  console.log(`Created ${ROADMAP_SORT_FIELD} formula field:`, json.id);
  console.log("Resolved formula:", json.options?.formula);
  console.log("\nIn Airtable: sort by Roadmap Sort (asc) for one-click roadmap order.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
