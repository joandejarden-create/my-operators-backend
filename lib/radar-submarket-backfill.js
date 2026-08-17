/**
 * Build submarket backfill patches from Notes prefix.
 */

import {
  extractSubmarketFromNotes,
  normalizeSubmarketLabel,
  inferSubmarketFromCity,
} from "./radar-submarket.js";

function strVal(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  return String(v).trim();
}

/**
 * @param {{ id: string, fields: Record<string, unknown> }} record
 * @param {{ nameField: string, notesField: string, submarketField: string, countryField: string, cityField?: string, countryFilter?: string, force?: boolean, inferFromCity?: boolean }} opts
 */
export function buildSubmarketBackfillPatch(record, opts) {
  const fields = record?.fields || {};
  const country = strVal(fields[opts.countryField]);
  const countryFilter = String(opts.countryFilter || "Puerto Rico").trim();

  if (country.toLowerCase() !== countryFilter.toLowerCase()) {
    return {
      recordId: record.id,
      skip: true,
      reason: "country_mismatch",
      country,
      countryFilter,
    };
  }

  const current = strVal(fields[opts.submarketField]);
  const notes = strVal(fields[opts.notesField]);
  const extracted = extractSubmarketFromNotes(notes);
  const inferred =
    !extracted && opts.inferFromCity !== false && opts.cityField
      ? inferSubmarketFromCity(strVal(fields[opts.cityField]), country)
      : "";
  const target = extracted
    ? normalizeSubmarketLabel(extracted, country)
    : inferred
      ? normalizeSubmarketLabel(inferred, country)
      : "";

  if (!target) {
    return {
      recordId: record.id,
      name: strVal(fields[opts.nameField]),
      skip: true,
      reason: "no_submarket_in_notes",
      country,
      city: opts.cityField ? strVal(fields[opts.cityField]) : "",
      notesPreview: notes.slice(0, 80),
      current: current || null,
    };
  }

  const populatedNonOther = current && current !== "Other";
  if (populatedNonOther && !opts.force) {
    return {
      recordId: record.id,
      name: strVal(fields[opts.nameField]),
      skip: true,
      reason: "already_populated",
      current,
      target,
      country,
    };
  }

  if (current === target) {
    return {
      recordId: record.id,
      name: strVal(fields[opts.nameField]),
      skip: true,
      reason: "unchanged",
      current,
      country,
      target,
    };
  }

  return {
    recordId: record.id,
    name: strVal(fields[opts.nameField]),
    needsUpdate: true,
    patch: { [opts.submarketField]: target },
    previous: current || null,
    target,
    country,
    source: extracted ? "notes" : inferred ? "city" : "",
  };
}

/**
 * @param {ReturnType<typeof buildSubmarketBackfillPatch>[]} results
 */
export function summarizeSubmarketBackfill(results) {
  const summary = {
    totalScanned: results.length,
    needingUpdate: 0,
    skippedAlreadyPopulated: 0,
    skippedNoNotesPrefix: 0,
    skippedCountryMismatch: 0,
    skippedUnchanged: 0,
    stillOtherAfter: 0,
    bySubmarket: {},
    samples: [],
    missingSubmarketAfter: [],
    wouldRemainOther: [],
  };

  for (const r of results) {
    if (r.needsUpdate) {
      summary.needingUpdate += 1;
      summary.bySubmarket[r.target] = (summary.bySubmarket[r.target] || 0) + 1;
      if (r.target === "Other") summary.stillOtherAfter += 1;
      if (summary.samples.length < 12) {
        summary.samples.push({
          id: r.recordId,
          name: r.name,
          previous: r.previous,
          target: r.target,
        });
      }
      continue;
    }
    if (r.reason === "already_populated") summary.skippedAlreadyPopulated += 1;
    else if (r.reason === "no_submarket_in_notes") {
      summary.skippedNoNotesPrefix += 1;
      if (summary.missingSubmarketAfter.length < 20) {
        summary.missingSubmarketAfter.push({
          id: r.recordId,
          name: r.name,
          current: r.current || "(empty)",
          notesPreview: r.notesPreview || "(empty)",
        });
      }
      if ((r.current || "") === "Other" || !r.current) {
        summary.stillOtherAfter += 1;
        if (summary.wouldRemainOther.length < 20) {
          summary.wouldRemainOther.push({ id: r.recordId, name: r.name, current: r.current || "(empty)" });
        }
      }
    } else if (r.reason === "country_mismatch") summary.skippedCountryMismatch += 1;
    else if (r.reason === "unchanged") {
      summary.skippedUnchanged += 1;
      if (r.current === "Other") summary.stillOtherAfter += 1;
    }
  }

  return summary;
}
