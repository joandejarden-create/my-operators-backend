/**
 * Offline in-memory Demand Generator graph store (Slice 2B).
 * Mirrors CREATE / UPDATE / UNCHANGED / DUPLICATE_LINKED classification
 * without touching Airtable.
 */

import { WRITE_OUTCOME } from "./gdi-full-e2e-airtable-persistence-contract-v1.js";

export const GDI_DEMAND_GENERATOR_MEMORY_STORE_V1 =
  "gdi-demand-generator-memory-store-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stableJson(v) {
  return JSON.stringify(v, Object.keys(v || {}).sort());
}

function materialEqual(a, b, keys) {
  for (const k of keys) {
    if (stableJson(a?.[k] ?? null) !== stableJson(b?.[k] ?? null)) return false;
  }
  return true;
}

function changedKeys(a, b, keys) {
  return keys.filter(
    (k) => stableJson(a?.[k] ?? null) !== stableJson(b?.[k] ?? null),
  );
}

/**
 * @param {{ demandGenerators?: object[], programs?: object[], signals?: object[], hotelFits?: object[] }} [seed]
 */
export function createMemoryDemandGeneratorGraphStore(seed = {}) {
  const generators = new Map();
  const programs = new Map();
  const signals = new Map();
  const hotelFits = new Map();
  let seq = 0;

  for (const row of seed.demandGenerators || []) {
    const id = text(row.demandGeneratorId);
    if (id) generators.set(id, { ...row, id: row.id || `rec_dg_${++seq}` });
  }
  for (const row of seed.programs || []) {
    const id = text(row.programId);
    if (id) programs.set(id, { ...row, id: row.id || `rec_prog_${++seq}` });
  }
  for (const row of seed.signals || []) {
    const id = text(row.signalId);
    if (id) signals.set(id, { ...row, id: row.id || `rec_sig_${++seq}` });
  }
  for (const row of seed.hotelFits || []) {
    const id = text(row.fitId);
    if (id) hotelFits.set(id, { ...row, id: row.id || `rec_fit_${++seq}` });
  }

  function upsertMap(map, identityKey, next, materialKeys, linkOnly = false) {
    const id = text(next[identityKey]);
    if (!id) {
      return {
        ok: false,
        outcome: WRITE_OUTCOME.FAILED,
        reason: `MISSING_${identityKey}`,
        airtableWrites: 0,
      };
    }
    const existing = map.get(id) || null;
    if (existing && linkOnly) {
      return {
        ok: true,
        outcome: WRITE_OUTCOME.DUPLICATE_LINKED,
        id: existing.id,
        [identityKey]: id,
        record: existing,
        airtableWrites: 0,
      };
    }
    if (!existing) {
      const record = { ...next, id: `rec_mem_${++seq}`, [identityKey]: id };
      map.set(id, record);
      return {
        ok: true,
        outcome: WRITE_OUTCOME.CREATED,
        id: record.id,
        [identityKey]: id,
        record,
        airtableWrites: 0,
      };
    }
    if (materialEqual(existing, next, materialKeys)) {
      return {
        ok: true,
        outcome: WRITE_OUTCOME.UNCHANGED,
        id: existing.id,
        [identityKey]: id,
        record: existing,
        airtableWrites: 0,
      };
    }
    const updated = {
      ...existing,
      ...next,
      id: existing.id,
      [identityKey]: id,
    };
    map.set(id, updated);
    return {
      ok: true,
      outcome: WRITE_OUTCOME.UPDATED,
      id: updated.id,
      [identityKey]: id,
      record: updated,
      changedFields: changedKeys(existing, next, materialKeys),
      airtableWrites: 0,
    };
  }

  return {
    version: GDI_DEMAND_GENERATOR_MEMORY_STORE_V1,
    airtableWrites: 0,
    productionWrites: 0,

    upsertDemandGenerator(entity, opts = {}) {
      return upsertMap(
        generators,
        "demandGeneratorId",
        entity,
        [
          "organizationName",
          "officialDomain",
          "website",
          "organizationType",
          "sourceUrls",
          "payload",
        ],
        opts.linkOnly === true,
      );
    },

    upsertDemandProgram(entity, opts = {}) {
      return upsertMap(
        programs,
        "programId",
        entity,
        [
          "demandGeneratorId",
          "programName",
          "programType",
          "recurrenceStatus",
          "sourceUrls",
        ],
        opts.linkOnly === true,
      );
    },

    upsertDemandGeneratorSignal(entity, opts = {}) {
      return upsertMap(
        signals,
        "signalId",
        entity,
        [
          "demandGeneratorId",
          "programId",
          "title",
          "eventStartDate",
          "sourceUrl",
          "signalStatus",
        ],
        opts.linkOnly === true,
      );
    },

    upsertHotelGeneratorFit(entity, opts = {}) {
      return upsertMap(
        hotelFits,
        "fitId",
        entity,
        [
          "hotelId",
          "demandGeneratorId",
          "relevanceClass",
          "generatorPriority",
          "fitRationale",
        ],
        opts.linkOnly === true,
      );
    },

    readBackDemandGenerator(demandGeneratorId) {
      return generators.get(text(demandGeneratorId)) || null;
    },
    readBackProgram(programId) {
      return programs.get(text(programId)) || null;
    },
    readBackSignal(signalId) {
      return signals.get(text(signalId)) || null;
    },
    readBackHotelFit(fitId) {
      return hotelFits.get(text(fitId)) || null;
    },

    snapshot() {
      return {
        demandGenerators: [...generators.values()],
        programs: [...programs.values()],
        signals: [...signals.values()],
        hotelFits: [...hotelFits.values()],
      };
    },
  };
}

export default {
  GDI_DEMAND_GENERATOR_MEMORY_STORE_V1,
  createMemoryDemandGeneratorGraphStore,
};
