#!/usr/bin/env node
/**
 * Offline + optional live read-back gates for Contact Enrichment Airtable Schema V1.
 * Default: offline only. Pass --live for Meta API read-back (no writes).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1,
  GDI_OPS_BASE,
  HI_BASE,
  USAGE_EVENTS_TABLE_NAME,
  USAGE_EVENTS_TABLE_ID_LIVE,
  USAGE_EVENTS_FIELD_IDS,
  HI_CONTACT_CHANNEL_ENRICHMENT_FIELD_IDS,
  MAP_USAGE_EVENT,
  MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1,
  USAGE_PROVIDER_CHOICES,
  USAGE_LOOKUP_OUTCOME_CHOICES,
  mapUsageOutcomeToAirtable,
  mapProviderToAirtable,
  assertUsageEventHasNoPii,
  resolveUsageEventsTableId,
} from "../lib/group-demand-intelligence/contact-intelligence/gdi-contact-enrichment-airtable-schema-v1.js";
import {
  FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1,
  buildUsageLedgerEvent,
  USAGE_OUTCOMES,
} from "../lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js";
import { MAP_HI_CONTACT_CHANNEL } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import { PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE } from "../lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const LIVE = process.argv.includes("--live");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
    console.error(`FAIL ${name}: ${e.message || e}`);
  }
}

check("schema_ids_locked", () => {
  assert.equal(GDI_OPS_BASE.baseId, "appa2cE7FTRmIbB32");
  assert.equal(HI_BASE.baseId, "appCCUsuGsE1ifoLk");
  assert.equal(HI_BASE.contactChannelsTableId, "tblqC7C4ypy5Zojyw");
  assert.equal(USAGE_EVENTS_TABLE_ID_LIVE, "tblvUJlik63pETitY");
  assert.equal(USAGE_EVENTS_TABLE_NAME, "GDI Contact Enrichment Usage Events");
});

check("usage_field_map_complete", () => {
  for (const name of Object.values(MAP_USAGE_EVENT)) {
    assert.ok(USAGE_EVENTS_FIELD_IDS[name], `missing field id for ${name}`);
  }
  assert.equal(Object.keys(USAGE_EVENTS_FIELD_IDS).length, 14);
});

check("hi_enrichment_field_ids", () => {
  for (const name of Object.values(MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1)) {
    assert.ok(
      HI_CONTACT_CHANNEL_ENRICHMENT_FIELD_IDS[name],
      `missing HI field id for ${name}`,
    );
  }
  assert.equal(MAP_HI_CONTACT_CHANNEL.lastCheckedAt, "Last Checked At");
  assert.equal(
    MAP_HI_CONTACT_CHANNEL.licenseRetentionClass,
    "License / Retention Class",
  );
});

check("outcome_mapping_collapses_blocked", () => {
  assert.equal(
    mapUsageOutcomeToAirtable(USAGE_OUTCOMES.LOOKUP_BLOCKED_DUPLICATE_WINDOW),
    "LOOKUP_BLOCKED",
  );
  assert.equal(mapUsageOutcomeToAirtable(USAGE_OUTCOMES.REUSE_HIT), "REUSE_HIT");
  assert.ok(USAGE_LOOKUP_OUTCOME_CHOICES.includes("LOOKUP_ERROR"));
});

check("provider_mapping", () => {
  assert.equal(mapProviderToAirtable("findymail"), "Findymail");
  assert.equal(mapProviderToAirtable("surfe"), "Surfe");
  assert.equal(mapProviderToAirtable("existing"), "Reuse");
  assert.ok(USAGE_PROVIDER_CHOICES.includes("Public Research"));
});

check("usage_ledger_no_pii_fields", () => {
  const row = buildUsageLedgerEvent({
    provider: "findymail",
    outcome: USAGE_OUTCOMES.LOOKUP_FOUND,
    personIdentityKeyHash: "abc123",
    hotelId: "recX",
    opportunityId: "opp1",
    creditsCharged: 1,
  });
  assert.equal(row.airtable.tableId, "tblvUJlik63pETitY");
  assert.equal(
    row.airtable.fields["Lookup Event ID"],
    row.lookupEventId,
  );
  const keys = Object.keys(row.airtable.fields);
  assert.ok(!keys.some((k) => /email|phone|name/i.test(k)));
  assert.equal(assertUsageEventHasNoPii(row.airtable.fields).ok, true);
});

check("findymail_remains_inactive_in_destinations", () => {
  assert.equal(FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1.findymailApiEnabled, false);
  assert.equal(FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1.surfeDurableEmailStorage, false);
  assert.equal(FINDYMAIL_AIRTABLE_DESTINATIONS_V1_1.dryRunDefault, true);
});

check("surfe_usage_ledger_coexists_with_enrichment_ledger", () => {
  // Surfe keeps its dedicated usage-events proposal (session-only contact PII policy).
  // The provider-agnostic enrichment ledger is additive for Findymail/Public/Reuse —
  // do not collapse Surfe into a competing single ledger.
  assert.equal(
    PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.tableName,
    "GDI Surfe Usage Events",
  );
  assert.notEqual(
    PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.tableName,
    USAGE_EVENTS_TABLE_NAME,
  );
  assert.equal(USAGE_EVENTS_TABLE_NAME, "GDI Contact Enrichment Usage Events");
  assert.equal(USAGE_EVENTS_TABLE_ID_LIVE, "tblvUJlik63pETitY");
});

check("resolve_table_id", () => {
  assert.equal(resolveUsageEventsTableId({}), USAGE_EVENTS_TABLE_ID_LIVE);
  assert.equal(
    resolveUsageEventsTableId({
      GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID: "tblOverride",
    }),
    "tblOverride",
  );
});

check("ensure_script_and_apply_report_present", () => {
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs"),
    ),
  );
  const apply = path.join(
    ROOT,
    "reports/gdi-contact-enrichment-airtable-schema-v1/APPLY_REPORT.json",
  );
  assert.ok(fs.existsSync(apply));
  const j = JSON.parse(fs.readFileSync(apply, "utf8"));
  assert.equal(j.usageEvents.tableId, USAGE_EVENTS_TABLE_ID_LIVE);
  assert.equal(j.safety.findymailApi, false);
  assert.equal(j.safety.contactRecordWrites, false);
});

if (LIVE) {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  check("live_meta_readback", async () => {
    assert.ok(token, "token required for --live");
  });
  // sync live checks below
}

async function liveChecks() {
  if (!LIVE) return;
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  assert.ok(token);
  const r = await fetch(
    `https://api.airtable.com/v0/meta/bases/${GDI_OPS_BASE.baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const j = await r.json();
  assert.equal(r.ok, true);
  const t = (j.tables || []).find((x) => x.id === USAGE_EVENTS_TABLE_ID_LIVE);
  assert.ok(t, "usage table missing live");
  assert.equal(t.name, USAGE_EVENTS_TABLE_NAME);
  const names = new Set((t.fields || []).map((f) => f.name));
  for (const n of Object.values(MAP_USAGE_EVENT)) assert.ok(names.has(n), n);
  for (const f of t.fields || []) {
    assert.doesNotMatch(f.name, /email|phone|token|api.?key/i);
  }

  const r2 = await fetch(
    `https://api.airtable.com/v0/meta/bases/${HI_BASE.baseId}/tables`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const j2 = await r2.json();
  const ch = (j2.tables || []).find(
    (x) => x.id === HI_BASE.contactChannelsTableId,
  );
  assert.ok(ch);
  const chNames = new Set((ch.fields || []).map((f) => f.name));
  for (const n of Object.values(MAP_HI_CONTACT_CHANNEL_ENRICHMENT_V1)) {
    assert.ok(chNames.has(n), n);
  }

  // Usage table must have zero records (no synthetic writes)
  const list = await fetch(
    `https://api.airtable.com/v0/${GDI_OPS_BASE.baseId}/${USAGE_EVENTS_TABLE_ID_LIVE}?pageSize=1`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const listJ = await list.json();
  assert.equal(list.ok, true);
  assert.equal((listJ.records || []).length, 0);

  results.push({ name: "live_meta_and_empty_usage", ok: true });
  console.log("PASS live_meta_and_empty_usage");
}

await liveChecks().catch((e) => {
  results.push({ name: "live_meta_and_empty_usage", ok: false, error: String(e.message || e) });
  console.error("FAIL live_meta_and_empty_usage:", e.message || e);
});

const failed = results.filter((r) => !r.ok);
const out = {
  contract: GDI_CONTACT_ENRICHMENT_AIRTABLE_SCHEMA_V1,
  at: new Date().toISOString(),
  live: LIVE,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
};
fs.mkdirSync(
  path.join(ROOT, "reports/gdi-contact-enrichment-airtable-schema-v1"),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-contact-enrichment-airtable-schema-v1/TEST_RESULTS.json",
  ),
  JSON.stringify(out, null, 2),
);
if (failed.length) {
  console.error(`FAILED ${failed.length}`);
  process.exit(1);
}
console.log(`\nOK ${results.length} schema gates`);
