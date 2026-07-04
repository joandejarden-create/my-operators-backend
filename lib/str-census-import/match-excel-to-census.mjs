import { fieldValue } from "./field-mapping.mjs";
import { normalizeStrId, normalizeKey, nameCityCountryKey } from "./normalize.mjs";

export function buildCensusIndexes(records, mapping) {
  const byStrId = new Map();
  const byNcc = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const strId = normalizeStrId(fieldValue(f, mapping.strId));
    const name = fieldValue(f, mapping.hotelName);
    const city = fieldValue(f, mapping.city);
    const country = fieldValue(f, mapping.country);

    const entry = {
      recordId: rec.id,
      fields: f,
      strId,
      name,
      city,
      country,
      strMarket: fieldValue(f, mapping.strMarket),
      strSubmarket: fieldValue(f, mapping.strSubmarket),
    };

    if (strId) {
      if (!byStrId.has(strId)) byStrId.set(strId, []);
      byStrId.get(strId).push(entry);
    }
    const ncc = nameCityCountryKey(name, city, country);
    if (ncc !== "||") {
      if (!byNcc.has(ncc)) byNcc.set(ncc, []);
      byNcc.get(ncc).push(entry);
    }
  }

  return { byStrId, byNcc };
}

export function censusSnapshot(entry) {
  return {
    censusStrId: entry.strId,
    censusHotelName: entry.name,
    censusCity: entry.city,
    censusCountry: entry.country,
    existingStrMarket: entry.strMarket,
    existingStrSubmarket: entry.strSubmarket,
  };
}

/**
 * @returns {{ status, confidence, needsReview, notes, matchedRecordId, census, proposed, censusEntry }}
 */
export function matchExcelRow(ex, byStrId, byNcc, excelStrIdCounts) {
  let status = "No Match";
  let confidence = "None";
  let needsReview = "Yes";
  const notes = [];
  let matchedRecordId = "";
  let censusEntry = null;
  let census = {
    censusStrId: "",
    censusHotelName: "",
    censusCity: "",
    censusCountry: "",
    existingStrMarket: "",
    existingStrSubmarket: "",
  };

  const proposed = {
    strMarket: ex.strMarket,
    strSubmarket: ex.strSubmarket,
    city: ex.city,
    country: ex.country,
    hotelName: ex.hotelName,
  };

  if (ex.strId && (excelStrIdCounts.get(ex.strId) || 0) > 1) {
    status = "Duplicate STR ID in Excel";
    confidence = "Low";
    notes.push("STR ID appears on multiple Excel rows");
  } else if (ex.strId && byStrId.has(ex.strId)) {
    const matches = byStrId.get(ex.strId);
    if (matches.length > 1) {
      status = "Duplicate STR ID in Census";
      confidence = "Low";
      notes.push(`${matches.length} census records share this STR ID`);
      matchedRecordId = matches.map((m) => m.recordId).join("; ");
      censusEntry = matches[0];
      census = censusSnapshot(matches[0]);
    } else {
      censusEntry = matches[0];
      matchedRecordId = matches[0].recordId;
      census = censusSnapshot(matches[0]);

      const nameMismatch =
        normalizeKey(ex.hotelName) &&
        normalizeKey(matches[0].name) &&
        normalizeKey(ex.hotelName) !== normalizeKey(matches[0].name);
      if (nameMismatch) {
        status = "Conflict";
        confidence = "Medium";
        notes.push("STR ID match but hotel name differs");
      } else {
        status = "Matched by STR ID";
        confidence = "High";
        needsReview = "No";
      }
    }
  } else {
    const ncc = nameCityCountryKey(ex.hotelName, ex.city, ex.country);
    if (ncc !== "||" && byNcc.has(ncc)) {
      const matches = byNcc.get(ncc);
      if (matches.length > 1) {
        status = "Needs Human Review";
        confidence = "Low";
        notes.push("Multiple census rows share Name+City+Country");
        matchedRecordId = matches.map((m) => m.recordId).join("; ");
        censusEntry = matches[0];
        census = censusSnapshot(matches[0]);
      } else if (ex.strId && matches[0].strId && ex.strId !== matches[0].strId) {
        status = "Conflict";
        confidence = "Medium";
        notes.push("Name/City/Country match but STR ID differs");
        matchedRecordId = matches[0].recordId;
        censusEntry = matches[0];
        census = censusSnapshot(matches[0]);
      } else {
        status = "Matched by Name City Country";
        confidence = "Medium";
        needsReview = "Yes";
        matchedRecordId = matches[0].recordId;
        censusEntry = matches[0];
        census = censusSnapshot(matches[0]);
        if (!ex.strId) notes.push("Excel row missing STR ID; matched on name/location");
      }
    } else if (!ex.strId) {
      notes.push("No STR ID and no census match on Name+City+Country");
    } else {
      notes.push("STR ID not found in census; no fallback match");
    }
  }

  return {
    status,
    confidence,
    needsReview,
    notes,
    matchedRecordId,
    census,
    proposed,
    censusEntry,
  };
}

/**
 * Build Airtable patch from Excel row.
 * Mapping: Market ← STR Market, Submarket ← STR Submarket (not STR Market).
 * @param {{ force?: boolean }} opts — force=true writes every non-empty Excel cell (full sync)
 */
export function buildCensusUpdateFields(mapping, ex, censusEntry, opts = {}) {
  const force = opts.force === true;
  if (!censusEntry) return {};
  const fields = {};

  const pairs = [
    [mapping.strMarket, ex.strMarket, censusEntry.strMarket],
    [mapping.strSubmarket, ex.strSubmarket, censusEntry.strSubmarket],
    [mapping.city, ex.city, censusEntry.city],
    [mapping.country, ex.country, censusEntry.country],
    [mapping.hotelName, ex.hotelName, censusEntry.name],
  ];

  for (const [airtableField, excelVal, censusVal] of pairs) {
    if (!airtableField) continue;
    const next = String(excelVal ?? "").trim();
    if (!next) continue;
    if (force || normalizeKey(next) !== normalizeKey(censusVal)) {
      fields[airtableField] = next;
    }
  }

  return fields;
}
