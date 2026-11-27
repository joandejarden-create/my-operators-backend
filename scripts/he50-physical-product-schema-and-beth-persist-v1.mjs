#!/usr/bin/env node
/**
 * HE 5.0 — founder-authorized physical-product HPC schema CREATE + Bethesda persist.
 *
 * Dry-run (default):
 *   node scripts/he50-physical-product-schema-and-beth-persist-v1.mjs
 *
 * Apply schema + Bethesda values:
 *   node scripts/he50-physical-product-schema-and-beth-persist-v1.mjs --apply \
 *     --confirm-he50-physical-product-schema \
 *     --confirm-additive-only \
 *     --confirm-no-legacy-census
 */
import dotenv from "dotenv";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import {
  HE_PHYSICAL_PRODUCT_SCHEMA_V1,
  HE_PHYSICAL_PRODUCT_BASE_ID,
  HE_PHYSICAL_PRODUCT_HPC_TABLE_ID,
  HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE,
  HE_PHYSICAL_PRODUCT_EXISTING_FIELDS,
  BETHESDA_PHYSICAL_PRODUCT_VALUES,
  assertBethesdaPhysicalProductInvariants,
} from "../lib/hotel-intelligence/he-physical-product-schema-v1.js";

dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });
dotenv.config({ override: false });

const argv = process.argv.slice(2);
const APPLY = argv.includes("--apply");
const CONFIRM_SCHEMA = argv.includes("--confirm-he50-physical-product-schema");
const CONFIRM_ADDITIVE = argv.includes("--confirm-additive-only");
const CONFIRM_NO_LEGACY = argv.includes("--confirm-no-legacy-census");

const OUT = join(
  process.cwd(),
  "reports/hotel-explorer/he-intelligence-factory-v1",
);
mkdirSync(OUT, { recursive: true });

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error(JSON.stringify({ ok: false, error: "missing_airtable_token" }));
  process.exit(2);
}

const BASE = HE_PHYSICAL_PRODUCT_BASE_ID;
const TABLE = HE_PHYSICAL_PRODUCT_HPC_TABLE_ID;
const META_HEADERS = {
  Authorization: `Bearer ${token}`,
  "Content-Type": "application/json",
};

async function listHpcFields() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${BASE}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`meta tables ${res.status} ${await res.text()}`);
  const j = await res.json();
  const hpc = (j.tables || []).find((t) => t.id === TABLE);
  if (!hpc) throw new Error("HPC table not found");
  return hpc.fields || [];
}

async function createField(spec) {
  const body = {
    name: spec.name,
    type: spec.type,
    description: spec.description,
  };
  if (spec.options) body.options = spec.options;
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${BASE}/tables/${TABLE}/fields`,
    { method: "POST", headers: META_HEADERS, body: JSON.stringify(body) },
  );
  if (!res.ok) throw new Error(`createField ${spec.name} ${res.status} ${await res.text()}`);
  return res.json();
}

async function getRecord(id) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`GET ${id} ${res.status} ${await res.text()}`);
  return res.json();
}

async function patchRecord(id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}/${id}`, {
    method: "PATCH",
    headers: META_HEADERS,
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!res.ok) throw new Error(`PATCH ${id} ${res.status} ${await res.text()}`);
  return res.json();
}

function verifyStagedEvidence() {
  const p = join(
    process.cwd(),
    "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
  );
  if (!existsSync(p)) return { ok: false, error: "staged_packet_missing" };
  const staged = JSON.parse(readFileSync(p, "utf8"));
  const gr = staged.guestRooms || {};
  const ms = staged.meetingSpaceSummary || {};
  const checks = {
    total: gr.totalGuestrooms?.value === 407,
    single: gr.singleBedRooms?.value === 171,
    double: gr.doubleBedRooms?.value === 236,
    suites: gr.suites?.value === 8,
    partition: gr.singleBedRooms?.value + gr.doubleBedRooms?.value === 407,
    meeting: ms.totalMeetingSpaceSqFt?.value === 18719,
    rooms: ms.meetingRoomCount?.value === 27,
    largestName: ms.largestRoom?.name === "Grand Ballroom",
    largestSqFt: ms.largestRoom?.areaSqFt === 4592,
    marketingDistinct: ms.privateSpaceMarketingSqFt?.value === 19000,
  };
  return { ok: Object.values(checks).every(Boolean), checks, sourceUrl: ms.totalMeetingSpaceSqFt?.sourceUrl };
}

const existingFields = await listHpcFields();
const byName = new Map(existingFields.map((f) => [f.name, f]));
const plan = {
  version: HE_PHYSICAL_PRODUCT_SCHEMA_V1,
  at: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  founderAuthorization: "HE_5_0_PHYSICAL_PRODUCT_SCHEMA_2026-11-27",
  baseId: BASE,
  tableId: TABLE,
  confirms: {
    schema: CONFIRM_SCHEMA,
    additiveOnly: CONFIRM_ADDITIVE,
    noLegacyCensus: CONFIRM_NO_LEGACY,
  },
  existingNearDuplicates: existingFields
    .filter((f) => /meet|guestroom|suite|single|double|ballroom/i.test(f.name))
    .map((f) => ({ name: f.name, id: f.id, type: f.type })),
  createPlan: [],
  skipAlreadyExists: [],
  quarantined: [],
  evidenceGate: verifyStagedEvidence(),
  schemaCreated: [],
  schemaReadback: null,
  bethPersist: null,
  gates: {
    PHYSICAL_PRODUCT_SCHEMA_VALIDATED: false,
    BETHESDA_PHYSICAL_PRODUCT_PERSISTED: false,
    AIRTABLE_READBACK_VALIDATED: false,
  },
};

for (const spec of HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE) {
  if (byName.has(spec.name)) {
    plan.skipAlreadyExists.push({ name: spec.name, id: byName.get(spec.name).id });
  } else if (HE_PHYSICAL_PRODUCT_EXISTING_FIELDS.includes(spec.name)) {
    plan.quarantined.push({ name: spec.name, reason: "listed_as_existing_canonical" });
  } else {
    plan.createPlan.push(spec);
  }
}

if (!plan.evidenceGate.ok) {
  writeFileSync(join(OUT, "HE50_PHYSICAL_PRODUCT_SCHEMA_APPLY.json"), JSON.stringify(plan, null, 2));
  console.error(JSON.stringify({ ok: false, error: "evidence_gate_failed", plan }, null, 2));
  process.exit(3);
}

if (APPLY) {
  if (!CONFIRM_SCHEMA || !CONFIRM_ADDITIVE || !CONFIRM_NO_LEGACY) {
    console.error(
      JSON.stringify({
        ok: false,
        error: "missing_confirms",
        required: [
          "--confirm-he50-physical-product-schema",
          "--confirm-additive-only",
          "--confirm-no-legacy-census",
        ],
      }),
    );
    process.exit(4);
  }

  for (const spec of plan.createPlan) {
    const created = await createField(spec);
    plan.schemaCreated.push({
      name: created.name,
      id: created.id,
      type: created.type,
    });
  }

  const afterFields = await listHpcFields();
  plan.schemaReadback = {
    fieldCount: afterFields.length,
    physicalProductFields: HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE.map((s) => {
      const hit = afterFields.find((f) => f.name === s.name);
      return hit
        ? { name: hit.name, id: hit.id, type: hit.type, present: true }
        : { name: s.name, present: false };
    }),
  };
  plan.gates.PHYSICAL_PRODUCT_SCHEMA_VALIDATED = plan.schemaReadback.physicalProductFields.every(
    (f) => f.present,
  );

  if (!plan.gates.PHYSICAL_PRODUCT_SCHEMA_VALIDATED) {
    writeFileSync(join(OUT, "HE50_PHYSICAL_PRODUCT_SCHEMA_APPLY.json"), JSON.stringify(plan, null, 2));
    console.error(JSON.stringify({ ok: false, error: "schema_readback_incomplete", plan }, null, 2));
    process.exit(5);
  }

  const values = { ...BETHESDA_PHYSICAL_PRODUCT_VALUES.fields };
  assertBethesdaPhysicalProductInvariants(values);
  const before = await getRecord(BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId);
  const patch = {};
  const skipped = [];
  for (const [k, v] of Object.entries(values)) {
    const cur = before.fields?.[k];
    if (k === "Rooms / Keys") {
      if (Number(cur) === Number(v)) skipped.push({ field: k, reason: "already_matches", value: cur });
      else if (cur == null || cur === "") patch[k] = v;
      else skipped.push({ field: k, reason: "non_empty_conflict_quarantine", current: cur, proposed: v });
      continue;
    }
    if (cur == null || cur === "" || Number(cur) === Number(v) || cur === v) {
      if (cur === v || Number(cur) === Number(v)) skipped.push({ field: k, reason: "already_matches", value: cur });
      else patch[k] = v;
    } else {
      // Allow overwrite only for newly created empty fields; quarantine conflicts
      skipped.push({ field: k, reason: "non_empty_conflict_quarantine", current: cur, proposed: v });
    }
  }

  // For brand-new fields, force write even if skipped as empty handled above
  for (const [k, v] of Object.entries(values)) {
    if (k === "Rooms / Keys") continue;
    if (!(k in patch) && (before.fields?.[k] == null || before.fields?.[k] === "")) {
      patch[k] = v;
    }
  }

  let applied = null;
  if (Object.keys(patch).length) {
    applied = await patchRecord(BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId, patch);
  }
  const after = await getRecord(BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId);
  const rb = {};
  for (const k of Object.keys(values)) rb[k] = after.fields?.[k] ?? null;

  const readbackOk =
    Number(rb["Rooms / Keys"]) === 407 &&
    Number(rb["Total Meeting Space (sq ft)"]) === 18719 &&
    Number(rb["Meeting Rooms Count"]) === 27 &&
    rb["Largest Meeting Room Name"] === "Grand Ballroom" &&
    Number(rb["Largest Meeting Room (sq ft)"]) === 4592 &&
    Number(rb["Guestroom Single Count"]) === 171 &&
    Number(rb["Guestroom Double Count"]) === 236 &&
    Number(rb["Guestroom Suites Count"]) === 8 &&
    rb["Guestroom Mix Confidence"] === "Brand Materials Reference";

  plan.bethPersist = {
    hpcId: BETHESDA_PHYSICAL_PRODUCT_VALUES.hpcId,
    beforeRooms: before.fields?.["Rooms / Keys"] ?? null,
    patch,
    skipped,
    applied: applied ? { id: applied.id, keys: Object.keys(patch) } : null,
    readback: rb,
    readbackOk,
  };
  plan.gates.BETHESDA_PHYSICAL_PRODUCT_PERSISTED = readbackOk;
  plan.gates.AIRTABLE_READBACK_VALIDATED = readbackOk;
} else {
  plan.gates.PHYSICAL_PRODUCT_SCHEMA_VALIDATED = false;
  plan.note = "Dry-run only — no schema CREATE, no record writes";
}

writeFileSync(join(OUT, "HE50_PHYSICAL_PRODUCT_SCHEMA_APPLY.json"), JSON.stringify(plan, null, 2));
console.log(
  JSON.stringify(
    {
      ok: true,
      mode: plan.mode,
      createCount: plan.createPlan.length,
      skipCount: plan.skipAlreadyExists.length,
      evidenceOk: plan.evidenceGate.ok,
      gates: plan.gates,
      out: join(OUT, "HE50_PHYSICAL_PRODUCT_SCHEMA_APPLY.json"),
    },
    null,
    2,
  ),
);

if (APPLY && (!plan.gates.PHYSICAL_PRODUCT_SCHEMA_VALIDATED || !plan.gates.AIRTABLE_READBACK_VALIDATED)) {
  process.exit(6);
}
