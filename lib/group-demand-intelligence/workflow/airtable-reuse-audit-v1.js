/**
 * Read-only Airtable reuse audit for the GDI coordinator.
 * Per-table: records read / matched / excluded / unresolved.
 * Distinguishes successful empty vs failed vs skipped.
 */

import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  isGdiOpportunityAirtableConfigured,
} from "../airtable-opportunity-store.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../opportunity-field-map.js";
import {
  DEMAND_GENERATORS_TABLE_NAME,
  DEMAND_PROGRAMS_TABLE_NAME,
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
} from "../demand-generators/constants.js";
import { isPeAirtableConfigured } from "../private-events/airtable-client.js";
import {
  HOTEL_VENUE_FIT_TABLE_NAME,
  PE_SIGNALS_TABLE_NAME,
  PE_VENUES_TABLE_NAME,
} from "../private-events/airtable-field-map.js";
import { listVenuesFromAirtable } from "../private-events/airtable-venue-store.js";
import { loadOpportunitiesCanonical } from "../opportunity-persistence.js";
import { loadFeedback } from "../repository.js";

export const AIRTABLE_REUSE_AUDIT_VERSION = "gdi-airtable-reuse-audit-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function token() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function emptyTableResult(table, status, reason) {
  return {
    table,
    status, // executed | skipped | failed | empty_success
    reason: reason || null,
    read: 0,
    matched: 0,
    excluded: 0,
    unresolved: 0,
    sampleIds: [],
  };
}

async function selectAll(base, tableName, opts = {}) {
  const rows = [];
  const selectOpts = {
    maxRecords: opts.maxRecords || 200,
  };
  if (opts.filterByFormula) selectOpts.filterByFormula = opts.filterByFormula;
  if (opts.fields) selectOpts.fields = opts.fields;
  await base(tableName)
    .select(selectOpts)
    .eachPage((records, next) => {
      for (const r of records) {
        rows.push({ id: r.id, fields: r.fields || {} });
      }
      next();
    });
  return rows;
}

/**
 * Audit one hotel against configured Airtable sources (read-only).
 */
export async function auditAirtableReuseForHotel(hotelId, opts = {}) {
  const id = text(hotelId);
  const skip = opts.skipAirtable === true;
  const tables = [];
  const startedAt = new Date().toISOString();

  if (skip) {
    return {
      version: AIRTABLE_REUSE_AUDIT_VERSION,
      hotelId: id,
      startedAt,
      skipped: true,
      reason: "skip_airtable_explicit",
      configured: {
        tokenPresent: Boolean(token()),
        gdiBaseIdPresent: Boolean(getGdiOpportunitiesAirtableBaseId()),
        opportunitiesConfigured: isGdiOpportunityAirtableConfigured(),
        peConfigured: isPeAirtableConfigured(),
      },
      tables: [],
      summary: { executed: 0, empty_success: 0, failed: 0, skipped: 1 },
    };
  }

  const tok = token();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  const configured = {
    tokenPresent: Boolean(tok),
    gdiBaseIdPresent: Boolean(baseId),
    opportunitiesConfigured: isGdiOpportunityAirtableConfigured(),
    peConfigured: isPeAirtableConfigured(),
  };

  if (!tok || !baseId) {
    return {
      version: AIRTABLE_REUSE_AUDIT_VERSION,
      hotelId: id,
      startedAt,
      skipped: false,
      reason: "airtable_not_configured",
      configured,
      tables: [
        emptyTableResult(HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME, "failed", "airtable_not_configured"),
        emptyTableResult(DEMAND_GENERATORS_TABLE_NAME, "failed", "airtable_not_configured"),
        emptyTableResult(DEMAND_PROGRAMS_TABLE_NAME, "failed", "airtable_not_configured"),
        emptyTableResult(DEMAND_GENERATOR_SIGNALS_TABLE_NAME, "failed", "airtable_not_configured"),
        emptyTableResult(GDI_OPPORTUNITIES_TABLE_NAME, "failed", "airtable_not_configured"),
      ],
      summary: { executed: 0, empty_success: 0, failed: 5, skipped: 0 },
    };
  }

  const base = new Airtable({ apiKey: tok }).base(baseId);
  const hotelFormula = `{Hotel ID} = "${id.replace(/"/g, '\\"')}"`;

  // --- Hotel Demand Generator Fit (hotel-scoped) ---
  try {
    const fits = await selectAll(base, HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME, {
      filterByFormula: hotelFormula,
      maxRecords: opts.maxFit || 150,
    });
    tables.push({
      table: HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
      status: fits.length ? "executed" : "empty_success",
      reason: fits.length ? "hotel_scoped_read" : "no_fit_rows_for_hotel",
      read: fits.length,
      matched: fits.length,
      excluded: 0,
      unresolved: 0,
      sampleIds: fits.slice(0, 5).map((r) => r.id),
    });
  } catch (err) {
    tables.push(
      emptyTableResult(
        HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
        "failed",
        err?.message || "read_failed"
      )
    );
  }

  // --- Demand Generators (sample / linked by fit org names — read recent page) ---
  try {
    const gens = await selectAll(base, DEMAND_GENERATORS_TABLE_NAME, {
      maxRecords: opts.maxGenerators || 50,
    });
    tables.push({
      table: DEMAND_GENERATORS_TABLE_NAME,
      status: gens.length ? "executed" : "empty_success",
      reason: "universe_sample_read_not_hotel_filtered",
      read: gens.length,
      matched: gens.length,
      excluded: 0,
      unresolved: 0,
      sampleIds: gens.slice(0, 5).map((r) => r.id),
    });
  } catch (err) {
    tables.push(
      emptyTableResult(DEMAND_GENERATORS_TABLE_NAME, "failed", err?.message || "read_failed")
    );
  }

  // --- Demand Programs ---
  try {
    const programs = await selectAll(base, DEMAND_PROGRAMS_TABLE_NAME, {
      maxRecords: opts.maxPrograms || 50,
    });
    tables.push({
      table: DEMAND_PROGRAMS_TABLE_NAME,
      status: programs.length ? "executed" : "empty_success",
      reason: "universe_sample_read",
      read: programs.length,
      matched: programs.length,
      excluded: 0,
      unresolved: 0,
      sampleIds: programs.slice(0, 5).map((r) => r.id),
    });
  } catch (err) {
    tables.push(
      emptyTableResult(DEMAND_PROGRAMS_TABLE_NAME, "failed", err?.message || "read_failed")
    );
  }

  // --- Demand Generator Signals ---
  try {
    const signals = await selectAll(base, DEMAND_GENERATOR_SIGNALS_TABLE_NAME, {
      maxRecords: opts.maxSignals || 50,
    });
    tables.push({
      table: DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
      status: signals.length ? "executed" : "empty_success",
      reason: "universe_sample_read",
      read: signals.length,
      matched: signals.length,
      excluded: 0,
      unresolved: 0,
      sampleIds: signals.slice(0, 5).map((r) => r.id),
    });
  } catch (err) {
    tables.push(
      emptyTableResult(
        DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
        "failed",
        err?.message || "read_failed"
      )
    );
  }

  // --- Group Demand Opportunities (hotel-scoped via canonical loader) ---
  try {
    // Prefer airtable persistence for this read when configured
    const prev = process.env.GDI_OPPORTUNITIES_PERSISTENCE;
    if (opts.forceAirtableOpportunities !== false) {
      process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
    }
    const doc = await loadOpportunitiesCanonical(id);
    if (opts.forceAirtableOpportunities !== false && prev != null) {
      process.env.GDI_OPPORTUNITIES_PERSISTENCE = prev;
    } else if (opts.forceAirtableOpportunities !== false && prev == null) {
      delete process.env.GDI_OPPORTUNITIES_PERSISTENCE;
    }
    const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
    const persistence = doc?.persistence || doc?.source || process.env.GDI_OPPORTUNITIES_PERSISTENCE;
    tables.push({
      table: GDI_OPPORTUNITIES_TABLE_NAME,
      status: ops.length ? "executed" : "empty_success",
      reason: `canonical_load persistence=${persistence || "unknown"}`,
      read: ops.length,
      matched: ops.filter((o) => o.hotelId === id || !o.hotelId).length,
      excluded: ops.filter((o) => o.hotelId && o.hotelId !== id).length,
      unresolved: ops.filter((o) => !o.id).length,
      sampleIds: ops.slice(0, 5).map((o) => o.id).filter(Boolean),
      persistence,
    });
  } catch (err) {
    tables.push(
      emptyTableResult(GDI_OPPORTUNITIES_TABLE_NAME, "failed", err?.message || "read_failed")
    );
  }

  // --- Hotel feedback (local FS primary; AT not always separate) ---
  try {
    const fb = loadFeedback(id) || {};
    const items = Array.isArray(fb.items)
      ? fb.items
      : fb.items && typeof fb.items === "object"
        ? Object.values(fb.items)
        : [];
    tables.push({
      table: "Hotel Feedback (local share-validation / feedback store)",
      status: items.length ? "executed" : "empty_success",
      reason: "local_feedback_store",
      read: items.length,
      matched: items.length,
      excluded: 0,
      unresolved: 0,
      sampleIds: items.slice(0, 5).map((i) => i.opportunityId || i.id).filter(Boolean),
    });
  } catch (err) {
    tables.push(
      emptyTableResult("Hotel Feedback", "failed", err?.message || "read_failed")
    );
  }

  // --- Private Event Venues / Fits / Signals ---
  if (isPeAirtableConfigured()) {
    try {
      const venues = await listVenuesFromAirtable({ maxRecords: opts.maxVenues || 100 });
      tables.push({
        table: PE_VENUES_TABLE_NAME,
        status: venues.length ? "executed" : "empty_success",
        reason: "pe_venues_universe_read",
        read: venues.length,
        matched: venues.length,
        excluded: 0,
        unresolved: 0,
        sampleIds: venues.slice(0, 5).map((v) => v.venueId || v.id).filter(Boolean),
      });
    } catch (err) {
      tables.push(
        emptyTableResult(PE_VENUES_TABLE_NAME, "failed", err?.message || "read_failed")
      );
    }
    try {
      const fits = await selectAll(base, HOTEL_VENUE_FIT_TABLE_NAME, {
        filterByFormula: hotelFormula,
        maxRecords: opts.maxPeFits || 100,
      });
      tables.push({
        table: HOTEL_VENUE_FIT_TABLE_NAME,
        status: fits.length ? "executed" : "empty_success",
        reason: fits.length ? "hotel_scoped_pe_fit" : "no_pe_fit_rows_for_hotel",
        read: fits.length,
        matched: fits.length,
        excluded: 0,
        unresolved: 0,
        sampleIds: fits.slice(0, 5).map((r) => r.id),
      });
    } catch (err) {
      tables.push(
        emptyTableResult(HOTEL_VENUE_FIT_TABLE_NAME, "failed", err?.message || "read_failed")
      );
    }
    try {
      const peSignals = await selectAll(base, PE_SIGNALS_TABLE_NAME, {
        maxRecords: opts.maxPeSignals || 50,
      });
      tables.push({
        table: PE_SIGNALS_TABLE_NAME,
        status: peSignals.length ? "executed" : "empty_success",
        reason: "pe_signals_universe_sample",
        read: peSignals.length,
        matched: peSignals.length,
        excluded: 0,
        unresolved: 0,
        sampleIds: peSignals.slice(0, 5).map((r) => r.id),
      });
    } catch (err) {
      tables.push(
        emptyTableResult(PE_SIGNALS_TABLE_NAME, "failed", err?.message || "read_failed")
      );
    }
  } else {
    tables.push(
      emptyTableResult(PE_VENUES_TABLE_NAME, "skipped", "pe_airtable_not_configured")
    );
    tables.push(
      emptyTableResult(HOTEL_VENUE_FIT_TABLE_NAME, "skipped", "pe_airtable_not_configured")
    );
    tables.push(
      emptyTableResult(PE_SIGNALS_TABLE_NAME, "skipped", "pe_airtable_not_configured")
    );
  }

  const summary = { executed: 0, empty_success: 0, failed: 0, skipped: 0 };
  for (const t of tables) {
    summary[t.status] = (summary[t.status] || 0) + 1;
  }

  return {
    version: AIRTABLE_REUSE_AUDIT_VERSION,
    hotelId: id,
    startedAt,
    finishedAt: new Date().toISOString(),
    skipped: false,
    reason: null,
    configured,
    tables,
    summary,
  };
}
