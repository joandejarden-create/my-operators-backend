/**
 * Set Explorer Hero labels on Hotel Equities (CALA) Master row.
 *
 *   node scripts/seed-he-cala-explorer-hero-labels.mjs
 *   node scripts/seed-he-cala-explorer-hero-labels.mjs --dry-run
 */
import "../load-env.js";
import {
  OPERATOR_EXPLORER_HERO_AIRTABLE,
} from "../lib/operator-explorer-hero-labels.js";

const MASTER_ID = "recWPKu5laVZxsvpn";
const TABLE = "Operator Setup - Master";

const LABELS = {
  [OPERATOR_EXPLORER_HERO_AIRTABLE.verification]: "Verified — Operator Setup",
  [OPERATOR_EXPLORER_HERO_AIRTABLE.dataSource]: "Live Airtable / Operator Setup data",
};

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY");

  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${MASTER_ID}`;
  if (dryRun) {
    console.log("Dry run — would PATCH", MASTER_ID, LABELS);
    return;
  }

  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: LABELS }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(
      `PATCH failed ${res.status}: ${JSON.stringify(json)} — run ensure-operator-master-explorer-hero-labels.mjs first`
    );
  }
  console.log("Updated", MASTER_ID, LABELS);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
