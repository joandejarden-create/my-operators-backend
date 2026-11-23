#!/usr/bin/env node
/**
 * Smoke test — Track C DealSignal CALA identity package (no Airtable writes).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dir = path.join(root, "reports", "sec-edgar-hif-track-c-dealsignal-cala-identity");

function load(name) {
  return JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
}

const table = load("DEALSIGNAL_CALA_IDENTITY_TABLE.json");
const handoff = load("TRACK_A_IC_HANDOFF.json");
const he = load("HOTEL_EXPLORER_HANDOFF_DEALSIGNAL.json");
const apply = load("PERSISTENCE_LEDGER_APPLY_WITH_HPC_CREATE.json");

const fails = [];
function ok(cond, msg) {
  if (!cond) fails.push(msg);
}

ok(table.publicationState === "CUSTOMER_BLOCKED", "table publicationState");
ok(table.paidResearchUsd === 0, "paidResearchUsd");
ok(table.examined === 9, "examined=9");
ok(table.existingHpcMatches === 9, "existingHpcMatches=9");
ok(table.newHpcCreates === 0, "newHpcCreates=0 (ledger truth; no create claim)");
ok(Array.isArray(table.rows) && table.rows.length === 9, "rows=9");
ok(apply.summary.matches === 9 && apply.summary.creates === 0, "apply ledger matches/creates");
ok(apply.summary.inventoryLinked === 9 && apply.summary.readBackOk === 9, "inventory+readBack");
ok(handoff.summary.matches === 9 && handoff.canonicalIds.length === 9, "IC handoff");
ok(Array.isArray(he.items) && he.items.length === 9, "HE handoff items");

const expectedNames = [
  "Hyatt Ziva Riviera Cancún",
  "Hyatt Zilara Riviera Maya",
  "The Bidwell Marriott Portland",
  "Residence Inn Long Island Holtsville",
  "Crowne Plaza Atlanta Perimeter at Ravinia",
  "Margaritaville Hollywood Beach Resort",
  "Margaritaville Beach House Key West",
  "Kimpton Hotel Palomar Phoenix",
  "Westin Denver International Airport",
];
for (const name of expectedNames) {
  ok(table.rows.some((r) => r.hotelName === name), `missing hotel ${name}`);
}

for (const r of table.rows) {
  ok(/^rec[A-Za-z0-9]{14}$/.test(r.canonicalHpcRecordId || ""), `hpc id ${r.hotelName}`);
  ok(/^rec[A-Za-z0-9]{14}$/.test(r.inventoryAirtableId || ""), `inventory id ${r.hotelName}`);
  ok(r.matchOrCreate === "MATCHED_EXISTING", `match ${r.hotelName}`);
  ok(r.idempotencyOk === true, `idempotency ${r.hotelName}`);
  ok(r.hpcReadBack?.id === r.canonicalHpcRecordId, `readBack id ${r.hotelName}`);
  ok(r.hpcReadBack?.country === r.country, `readBack country ${r.hotelName}`);
}

const cala = table.rows.filter((r) => r.country === "Mexico");
ok(cala.length === 2, "two Mexico CALA properties");
ok(
  cala.every((r) => r.owner == null && r.operator == null),
  "Ziva/Zilara owner+operator remain explicit null",
);

if (fails.length) {
  console.error(JSON.stringify({ ok: false, fails }, null, 2));
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      examined: 9,
      matches: 9,
      creates: 0,
      inventoryLinked: 9,
      readBackOk: 9,
      publicationState: "CUSTOMER_BLOCKED",
      paidResearchUsd: 0,
    },
    null,
    2,
  ),
);
