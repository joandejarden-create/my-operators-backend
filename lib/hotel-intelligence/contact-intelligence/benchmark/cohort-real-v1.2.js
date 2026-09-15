/**
 * Contact Intelligence V1.2 — real Census-backed benchmark manifest (versioned).
 * Synthetic V1 cohort remains as regression fixtures only.
 * Does not invent hotels to fill to 100.
 */

export const CONTACT_BENCHMARK_REAL_VERSION = "contact-benchmark-real-v1.2";

/** Fixed comparison set — same 10 as live slice (do not retarget). */
export const FIXED_COMPARISON_TEN = Object.freeze([
  "recUNycnMwOVFX0hc",
  "recId5nDFUgVbJnzH",
  "recIwaP1etgx2g9nA",
  "recsYJb2R1jarPpK3",
  "recTYaiA4S6fR6ixx",
  "recGZZCek9vDQGG1L",
  "rec79Xs4mZkuiWnuN",
  "recFspIiglYxJp1N1",
  "recogJrXdZHRV06Bl",
  "recZxCHVNG0bDQhfG",
]);

/**
 * Build a real-hotel manifest entry from a Census-normalized record.
 * Owner groups are provisional until leakage assessment.
 */
export function mapCensusRecordToManifestEntry(record, opts = {}) {
  const id = record.airtable_record_id || record.id;
  const fields = record.fields || record;
  const country = String(fields.country || fields.Country || "").trim();
  const city = String(fields.city || fields.City || "").trim();
  const name =
    fields.property_name ||
    fields.official_name ||
    fields["Property Name"] ||
    fields["Canonical Property Name"] ||
    null;
  const website = fields.website || fields["Official Property URL"] || null;
  const phone = fields.phone || fields.Phone || null;
  const lang =
    /brazil|brasil|portugal/i.test(country) || /são|sao paulo|rio de janeiro/i.test(city)
      ? "pt"
      : /united states|bermuda|canada|uk|united kingdom/i.test(country)
        ? "en"
        : "es";

  return {
    hotel_id: id,
    hotel_name: name,
    city: city || null,
    country: country || null,
    language: lang,
    owner_group: opts.owner_group || "provisional_unassigned",
    owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
    split: opts.split || "development",
    census: {
      property_identity_key: fields.property_identity_key || fields["Property Identity Key"] || null,
      website,
      phone,
      source: "hotel_property_census",
    },
    synthetic: false,
  };
}

/**
 * @param {object[]} censusRecords — from join or list (must have real Airtable IDs)
 * @param {object} [opts]
 */
export function buildRealBenchmarkManifest(censusRecords = [], opts = {}) {
  const target = opts.target || 100;
  const heldOutRatio = opts.heldOutRatio ?? 0.25;
  const fixedSet = new Set(FIXED_COMPARISON_TEN);
  const seen = new Set();
  const hotels = [];

  for (const id of FIXED_COMPARISON_TEN) {
    const rec = censusRecords.find(
      (r) => (r.airtable_record_id || r.id) === id || r.record?.airtable_record_id === id
    );
    if (rec) {
      const mapped = mapCensusRecordToManifestEntry(rec.record || rec, {
        owner_group: opts.fixedOwnerGroups?.[id] || "fixed_comparison",
        split: "development",
      });
      hotels.push({ ...mapped, fixed_comparison_ten: true });
      seen.add(id);
    } else {
      hotels.push({
        hotel_id: id,
        hotel_name: null,
        fixed_comparison_ten: true,
        split: "development",
        owner_group: "fixed_comparison",
        owner_group_status: "PROVISIONAL_UNTIL_LEAKAGE_ASSESSED",
        census_join_pending: true,
        synthetic: false,
      });
      seen.add(id);
    }
  }

  const portuguese = [];
  for (const raw of censusRecords) {
    const id = raw.airtable_record_id || raw.id || raw.record?.airtable_record_id;
    if (!id || seen.has(id) || !/^rec[A-Za-z0-9]{14}$/.test(id)) continue;
    if (String(id).startsWith("recBench")) continue;
    const entry = mapCensusRecordToManifestEntry(raw.record || raw, {
      split: "development",
    });
    if (!entry.hotel_name) continue;
    if (entry.language === "pt") portuguese.push(entry);
    if (hotels.length < target) {
      hotels.push(entry);
      seen.add(id);
    }
  }

  // Reserve held-out slots without researching them
  const nHeld = Math.min(
    Math.floor(hotels.length * heldOutRatio),
    Math.max(0, hotels.length - FIXED_COMPARISON_TEN.length)
  );
  let heldAssigned = 0;
  for (let i = hotels.length - 1; i >= 0 && heldAssigned < nHeld; i--) {
    if (hotels[i].fixed_comparison_ten) continue;
    hotels[i].split = "held_out";
    hotels[i].held_out_reserved = true;
    hotels[i].research_status = "NOT_RESEARCHED_HELD_OUT";
    heldAssigned += 1;
  }

  return {
    version: CONTACT_BENCHMARK_REAL_VERSION,
    generated_at: new Date().toISOString(),
    target_size: target,
    populated_size: hotels.filter((h) => h.hotel_id && !h.census_join_pending).length,
    pending_census_join: hotels.filter((h) => h.census_join_pending).length,
    synthetic_fill: 0,
    portuguese_supplemental: portuguese.slice(0, 5).map((h) => ({
      hotel_id: h.hotel_id,
      hotel_name: h.hotel_name,
      country: h.country,
      city: h.city,
      note: "Supplemental PT case — reported separately from fixed ten",
    })),
    portuguese_available: portuguese.length > 0,
    portuguese_constraint:
      portuguese.length > 0
        ? null
        : "No Brazil/Portugal Census rows in the supplied record set — check data coverage, not HI fixtures.",
    fixed_comparison_ten: FIXED_COMPARISON_TEN,
    held_out_count: heldAssigned,
    hotels,
    synthetic_v1_status: "ARCHIVED_AS_REGRESSION_FIXTURE",
    synthetic_v1_path: "lib/hotel-intelligence/contact-intelligence/benchmark/cohort-100.js",
  };
}
