/**
 * Bethesda controlled Airtable production pilot V1.
 *
 * Narrowly scoped: exactly four founder-authorized canonical identities.
 * Fail-closed unless authorizeProductionWrites + validated exactWriteSetAuthorization
 * matching the frozen allowlist and field-level changes.
 */

import {
  findDemandGeneratorById,
  findDemandGeneratorByOrganizationName,
} from "../demand-generators/airtable-stores.js";
import {
  DEMAND_GENERATORS_TABLE_NAME,
  MAP_DEMAND_GENERATOR,
} from "../demand-generators/airtable-field-map.js";
import { getGdiOpportunitiesAirtableBaseId } from "../airtable-opportunity-store.js";
import {
  MAP_PE_VENUE as VENUE_F,
  PE_VENUES_TABLE_NAME,
} from "../private-events/airtable-field-map.js";
import Airtable from "airtable";

export const GDI_BETHESDA_AIRTABLE_PRODUCTION_PILOT_V1 =
  "gdi-bethesda-airtable-production-pilot-v1";

/** Frozen founder authorization id — not a secret; validates exact allowlist. */
export const BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION =
  "bethesda-marriott-four-record-pilot-v1";

export const BETHESDA_PILOT_HOTEL_ID = "recLuxvwwxID7U2B8";

export const BETHESDA_PILOT_ALLOWLIST = Object.freeze([
  Object.freeze({
    canonicalId: "dg_5320e4f7f6cd2bf0",
    tableName: DEMAND_GENERATORS_TABLE_NAME,
    action: "CREATE",
    organizationName: "Amentum Holdings, Inc. / Amentum",
    allowedFields: Object.freeze({
      [MAP_DEMAND_GENERATOR.demandGeneratorId]: "dg_5320e4f7f6cd2bf0",
      [MAP_DEMAND_GENERATOR.organizationName]:
        "Amentum Holdings, Inc. / Amentum",
      [MAP_DEMAND_GENERATOR.generatorStatus]: "DISCOVERED",
      [MAP_DEMAND_GENERATOR.primaryMarkets]: "Bethesda",
      [MAP_DEMAND_GENERATOR.researchReason]: "corporate_accounts_scaling",
    }),
  }),
  Object.freeze({
    canonicalId: "dg_b5b62937a741d606",
    tableName: DEMAND_GENERATORS_TABLE_NAME,
    action: "CREATE",
    organizationName: "American Political Science Association",
    allowedFields: Object.freeze({
      [MAP_DEMAND_GENERATOR.demandGeneratorId]: "dg_b5b62937a741d606",
      [MAP_DEMAND_GENERATOR.organizationName]:
        "American Political Science Association",
      [MAP_DEMAND_GENERATOR.generatorStatus]: "DISCOVERED",
      [MAP_DEMAND_GENERATOR.primaryMarkets]: "Bethesda",
      [MAP_DEMAND_GENERATOR.researchReason]: "government_public_sector",
    }),
  }),
  Object.freeze({
    canonicalId: "dg_82dd10dca7693c2b",
    tableName: DEMAND_GENERATORS_TABLE_NAME,
    action: "CREATE",
    organizationName: "SAME Foundation",
    allowedFields: Object.freeze({
      [MAP_DEMAND_GENERATOR.demandGeneratorId]: "dg_82dd10dca7693c2b",
      [MAP_DEMAND_GENERATOR.organizationName]: "SAME Foundation",
      [MAP_DEMAND_GENERATOR.generatorStatus]: "DISCOVERED",
      [MAP_DEMAND_GENERATOR.primaryMarkets]: "Bethesda",
      [MAP_DEMAND_GENERATOR.researchReason]: "social_special_events",
    }),
  }),
  Object.freeze({
    canonicalId: "pev_42542bf58aac936b",
    tableName: PE_VENUES_TABLE_NAME,
    action: "UPDATE",
    organizationName: "Rock Creek Mansion",
    existingAirtableRecordId: "recf35Q9B6jpX3Ivm",
    allowedFields: Object.freeze({
      [VENUE_F.venueId]: "pev_42542bf58aac936b",
      [VENUE_F.venueName]: "Rock Creek Mansion",
      [VENUE_F.city]: "Bethesda",
      [VENUE_F.onSiteLodgingStatus]: "UNKNOWN",
    }),
  }),
]);

export const BETHESDA_PILOT_DEFERRED_GDOS = Object.freeze([
  "disc_ff8f41b34c5b",
  "disc_ab0b19495c15",
  "disc_2cac04452eec",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function getToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function getBase() {
  const token = getToken();
  if (!token) throw new Error("AIRTABLE_API_KEY / AIRTABLE_PAT required");
  const baseId = getGdiOpportunitiesAirtableBaseId();
  return new Airtable({ apiKey: token }).base(baseId);
}

function fieldDiff(before = {}, after = {}) {
  // Only consider keys present in `after` (the proposed patch set).
  // Keys omitted from after are intentionally preserved, not cleared.
  const changed = [];
  for (const k of Object.keys(after || {})) {
    const b = before[k] == null || before[k] === "" ? null : before[k];
    const a = after[k] == null || after[k] === "" ? null : after[k];
    if (JSON.stringify(b) !== JSON.stringify(a)) {
      changed.push({ field: k, before: b, after: a });
    }
  }
  return changed;
}

/**
 * Validate founder authorization flags against frozen allowlist (not arbitrary strings).
 */
export function validateBethesdaPilotAuthorization(input = {}) {
  const authorizeProductionWrites = input.authorizeProductionWrites === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true;
  const hotelId = text(input.hotelId);

  if (!authorizeProductionWrites) {
    return {
      ok: false,
      code: "MISSING_AUTHORIZE_PRODUCTION_WRITES",
      detail: "authorizeProductionWrites=true required",
    };
  }
  if (exactWriteSetAuthorization !== BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION) {
    return {
      ok: false,
      code: "INVALID_EXACT_WRITE_SET_AUTHORIZATION",
      detail:
        "exactWriteSetAuthorization must equal the frozen four-record Bethesda pilot id",
      expected: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
    };
  }
  if (hotelId && hotelId !== BETHESDA_PILOT_HOTEL_ID) {
    return {
      ok: false,
      code: "HOTEL_IDENTITY_MISMATCH",
      detail: `hotelId must be ${BETHESDA_PILOT_HOTEL_ID}`,
    };
  }
  if (!apply) {
    return {
      ok: true,
      apply: false,
      code: "DRY_RUN_ONLY",
      allowlistCount: BETHESDA_PILOT_ALLOWLIST.length,
    };
  }
  return {
    ok: true,
    apply: true,
    code: "AUTHORIZED_FOUR_RECORD_PILOT",
    allowlistCount: BETHESDA_PILOT_ALLOWLIST.length,
  };
}

async function fetchVenueRecord(recordId) {
  const base = getBase();
  return base(PE_VENUES_TABLE_NAME).find(recordId);
}

async function preflightDemandGenerator(entry) {
  const byId = await findDemandGeneratorById(entry.canonicalId);
  const byName = await findDemandGeneratorByOrganizationName(
    entry.organizationName,
  );
  const existing = byId || byName || null;
  const collision =
    byId && byName && byId.id !== byName.id
      ? "IDENTITY_COLLISION_ID_VS_NAME"
      : byName && !byId
        ? "MATCH_EXISTING_BY_NAME"
        : byId
          ? "MATCH_EXISTING_BY_ID"
          : "NONE";

  if (collision === "IDENTITY_COLLISION_ID_VS_NAME") {
    return {
      ...entry,
      proposedAction: "DEFERRED",
      collision,
      airtableRecordId: null,
      deferReason: "Unresolved ID vs name collision — founder review required",
      existingValues: byId?.fields || null,
      nameMatchRecordId: byName.id,
      changedFields: [],
      safeRetryKey: `dg:${entry.canonicalId}`,
    };
  }

  if (existing) {
    const before = {};
    for (const k of Object.keys(entry.allowedFields)) {
      before[k] = existing.fields?.[k] ?? null;
    }
    const changed = fieldDiff(before, entry.allowedFields);
    // Existing by name with different canonical ID → do not create duplicate
    if (!byId && byName) {
      return {
        ...entry,
        proposedAction: "DEFERRED",
        collision: "MATCH_EXISTING_BY_NAME",
        airtableRecordId: byName.id,
        deferReason:
          "Organization already exists under another Airtable record — no duplicate CREATE",
        existingValues: before,
        existingCanonicalId:
          byName.fields?.[MAP_DEMAND_GENERATOR.demandGeneratorId] || null,
        changedFields: changed,
        safeRetryKey: `dg:${entry.canonicalId}`,
      };
    }
    return {
      ...entry,
      proposedAction: changed.length ? "UPDATE" : "UNCHANGED",
      // CREATE allowlist becomes UPDATE/UNCHANGED if already present — never force create
      collision: "MATCH_EXISTING_BY_ID",
      airtableRecordId: existing.id,
      existingValues: before,
      newValues: entry.allowedFields,
      changedFields: changed,
      // Pilot authorization is CREATE-only for DGs; updates beyond reviewed create set are deferred
      applyEligible: false,
      deferReason: changed.length
        ? "Existing DG found; pilot authorizes CREATE only — treat as MATCH_EXISTING (no expand-to-UPDATE)"
        : "Existing DG already matches reviewed fields",
      safeRetryKey: `dg:${entry.canonicalId}`,
    };
  }

  return {
    ...entry,
    proposedAction: "CREATE",
    collision: "NONE",
    airtableRecordId: null,
    existingValues: null,
    newValues: entry.allowedFields,
    changedFields: Object.entries(entry.allowedFields).map(([field, after]) => ({
      field,
      before: null,
      after,
    })),
    applyEligible: true,
    safeRetryKey: `dg:${entry.canonicalId}`,
  };
}

async function preflightVenue(entry) {
  const rec = await fetchVenueRecord(entry.existingAirtableRecordId);
  if (!rec) {
    return {
      ...entry,
      proposedAction: "DEFERRED",
      collision: "MISSING_EXISTING_RECORD",
      deferReason: `Expected venue record ${entry.existingAirtableRecordId} not found`,
      applyEligible: false,
      safeRetryKey: `pev:${entry.canonicalId}`,
    };
  }
  const before = {};
  for (const k of Object.keys(entry.allowedFields)) {
    before[k] = rec.fields?.[k] ?? null;
  }
  // Safe patch set: never downgrade a more-specific lodging status to UNKNOWN,
  // and never clear city/name when already matching.
  const safeAfter = { ...entry.allowedFields };
  const deferredFieldNotes = [];
  const lodgingKey = VENUE_F.onSiteLodgingStatus;
  const existingLodging = text(before[lodgingKey]);
  if (
    existingLodging &&
    existingLodging !== "UNKNOWN" &&
    text(safeAfter[lodgingKey]) === "UNKNOWN"
  ) {
    deferredFieldNotes.push(
      `${lodgingKey}: preserve existing ${existingLodging}; do not downgrade to UNKNOWN`,
    );
    delete safeAfter[lodgingKey];
  }
  const changed = fieldDiff(before, safeAfter).filter((c) => {
    // Name/city already match → no-op
    if (c.before != null && String(c.before) === String(c.after)) return false;
    return true;
  });
  return {
    ...entry,
    proposedAction: changed.length ? "UPDATE" : "UNCHANGED",
    collision: "EXISTING_VENUE_RECORD",
    airtableRecordId: rec.id,
    existingValues: before,
    newValues: safeAfter,
    changedFields: changed,
    applyEligible: changed.length > 0,
    deferredFieldNotes,
    allExistingFieldNames: Object.keys(rec.fields || {}),
    preservedFieldCount: Object.keys(rec.fields || {}).length,
    materialBeyondAllowlist: false,
    safeRetryKey: `pev:${entry.canonicalId}:${rec.id}`,
  };
}

/**
 * Build exact pre-apply manifest (read-only).
 */
export async function buildBethesdaPilotPreApplyManifest(input = {}) {
  const auth = validateBethesdaPilotAuthorization({
    ...input,
    apply: false,
    authorizeProductionWrites: true,
    exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
    hotelId: BETHESDA_PILOT_HOTEL_ID,
  });
  const operations = [];
  for (const entry of BETHESDA_PILOT_ALLOWLIST) {
    if (entry.tableName === DEMAND_GENERATORS_TABLE_NAME) {
      operations.push(await preflightDemandGenerator(entry));
    } else {
      operations.push(await preflightVenue(entry));
    }
  }
  const eligible = operations.filter((o) => o.applyEligible);
  const deferred = operations.filter((o) => !o.applyEligible);
  return {
    version: GDI_BETHESDA_AIRTABLE_PRODUCTION_PILOT_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_PILOT_HOTEL_ID,
    baseId: getGdiOpportunitiesAirtableBaseId(),
    exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
    auth,
    allowlistCount: BETHESDA_PILOT_ALLOWLIST.length,
    deferredGdosMustRemain: BETHESDA_PILOT_DEFERRED_GDOS,
    operations,
    eligibleApplyCount: eligible.length,
    deferredOrUnchangedCount: deferred.length,
    mutation: false,
  };
}

async function applyDemandGeneratorCreate(op) {
  const base = getBase();
  const created = await base(DEMAND_GENERATORS_TABLE_NAME).create(
    op.newValues,
  );
  const readBack = await base(DEMAND_GENERATORS_TABLE_NAME).find(created.id);
  const verified = {};
  for (const [k, v] of Object.entries(op.newValues)) {
    verified[k] = {
      expected: v,
      actual: readBack.fields?.[k] ?? null,
      ok: String(readBack.fields?.[k] ?? "") === String(v),
    };
  }
  return {
    canonicalId: op.canonicalId,
    action: "CREATE",
    airtableRecordId: created.id,
    apiResponseId: created.id,
    readBackOk: Object.values(verified).every((x) => x.ok),
    verifiedFields: verified,
    outcome: "CREATED",
  };
}

async function applyVenuePatch(op) {
  const base = getBase();
  const beforeRec = await base(PE_VENUES_TABLE_NAME).find(op.airtableRecordId);
  const beforeSnapshot = { ...(beforeRec.fields || {}) };
  // Patch ONLY changed allowlisted fields — never replace the whole record.
  const patch = {};
  for (const c of op.changedFields) {
    patch[c.field] = c.after;
  }
  const updated = await base(PE_VENUES_TABLE_NAME).update(
    op.airtableRecordId,
    patch,
  );
  const readBack = await base(PE_VENUES_TABLE_NAME).find(op.airtableRecordId);
  const verified = {};
  for (const [k, v] of Object.entries(patch)) {
    verified[k] = {
      expected: v,
      actual: readBack.fields?.[k] ?? null,
      ok: String(readBack.fields?.[k] ?? "") === String(v),
    };
  }
  // Ensure unrelated fields were not cleared
  const cleared = [];
  for (const [k, v] of Object.entries(beforeSnapshot)) {
    if (k in patch) continue;
    const after = readBack.fields?.[k];
    if (v != null && v !== "" && (after == null || after === "")) {
      cleared.push(k);
    }
  }
  return {
    canonicalId: op.canonicalId,
    action: "UPDATE",
    airtableRecordId: op.airtableRecordId,
    apiResponseId: updated.id,
    patch,
    readBackOk: Object.values(verified).every((x) => x.ok) && cleared.length === 0,
    verifiedFields: verified,
    clearedUnrelatedFields: cleared,
    outcome: "UPDATED",
  };
}

/**
 * Execute the pilot. apply=true requires validated authorization.
 */
export async function executeBethesdaAirtableProductionPilot(input = {}) {
  const auth = validateBethesdaPilotAuthorization({
    ...input,
    hotelId: input.hotelId || BETHESDA_PILOT_HOTEL_ID,
  });
  const pre = await buildBethesdaPilotPreApplyManifest(input);
  if (!auth.ok) {
    return { ok: false, blocked: true, auth, preApply: pre };
  }
  if (!auth.apply) {
    return {
      ok: true,
      applied: false,
      auth,
      preApply: pre,
      detail: "Dry-run only — set apply=true to execute eligible ops",
    };
  }

  // Re-validate every eligible op still matches allowlist exactly
  const results = [];
  for (const op of pre.operations) {
    if (!op.applyEligible) {
      results.push({
        canonicalId: op.canonicalId,
        outcome: op.proposedAction === "UNCHANGED" ? "UNCHANGED" : "DEFERRED",
        reason: op.deferReason || op.proposedAction,
        airtableRecordId: op.airtableRecordId,
      });
      continue;
    }
    const allow = BETHESDA_PILOT_ALLOWLIST.find(
      (a) => a.canonicalId === op.canonicalId,
    );
    if (!allow) {
      results.push({
        canonicalId: op.canonicalId,
        outcome: "DEFERRED",
        reason: "NOT_IN_ALLOWLIST",
      });
      continue;
    }
    if (op.tableName !== allow.tableName) {
      results.push({
        canonicalId: op.canonicalId,
        outcome: "DEFERRED",
        reason: "TABLE_MISMATCH",
      });
      continue;
    }
    try {
      if (op.proposedAction === "CREATE") {
        results.push(await applyDemandGeneratorCreate(op));
      } else if (op.proposedAction === "UPDATE") {
        results.push(await applyVenuePatch(op));
      } else {
        results.push({
          canonicalId: op.canonicalId,
          outcome: "UNCHANGED",
          airtableRecordId: op.airtableRecordId,
        });
      }
    } catch (err) {
      results.push({
        canonicalId: op.canonicalId,
        outcome: "FAILED",
        error: String(err?.message || err),
        airtableRecordId: op.airtableRecordId || null,
      });
    }
  }

  const created = results.filter((r) => r.outcome === "CREATED").length;
  const updated = results.filter((r) => r.outcome === "UPDATED").length;
  const failed = results.filter((r) => r.outcome === "FAILED").length;
  return {
    ok: failed === 0 && results.every((r) => r.readBackOk !== false),
    applied: true,
    auth,
    preApply: pre,
    results,
    summary: {
      created,
      updated,
      unchangedOrDeferred: results.filter((r) =>
        ["UNCHANGED", "DEFERRED"].includes(r.outcome),
      ).length,
      failed,
    },
  };
}

/**
 * Idempotency recheck — planner only; expect no new creates.
 */
export async function recheckBethesdaPilotIdempotency() {
  const pre = await buildBethesdaPilotPreApplyManifest({
    authorizeProductionWrites: true,
    exactWriteSetAuthorization: BETHESDA_PILOT_EXACT_WRITE_SET_AUTHORIZATION,
    hotelId: BETHESDA_PILOT_HOTEL_ID,
    apply: false,
  });
  const wouldCreate = pre.operations.filter((o) => o.proposedAction === "CREATE");
  return {
    ok: wouldCreate.length === 0,
    wouldCreateCount: wouldCreate.length,
    operations: pre.operations.map((o) => ({
      canonicalId: o.canonicalId,
      proposedAction: o.proposedAction,
      airtableRecordId: o.airtableRecordId,
      collision: o.collision,
      changedFieldCount: (o.changedFields || []).length,
    })),
  };
}

export { DEMAND_GENERATORS_TABLE_NAME, PE_VENUES_TABLE_NAME };
