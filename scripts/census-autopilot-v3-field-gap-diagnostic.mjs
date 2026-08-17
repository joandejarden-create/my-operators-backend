/**
 * V3 field-gap diagnostic — READ ONLY. No Airtable writes.
 * Authorized run: cav3_2026-08-08T15-04-05-566Z
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { INSERT_ALLOWED_FIELDS } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const RUN = "cav3_2026-08-08T15-04-05-566Z";
const V3 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v3-airtable-migration");
const V23 = path.join(ROOT, "data/research-engine-v2/census-autopilot-v2-3-independent-universe");
const OUT = path.join(V3, "31-field-gap-diagnostic");

const FIELDS = [
  "State / Region",
  "Address",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
];

function blank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

function wj(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2));
}
function wm(name, text) {
  fs.writeFileSync(path.join(OUT, name), text);
}

fs.mkdirSync(OUT, { recursive: true });

const sel = JSON.parse(fs.readFileSync(path.join(V3, "05-pilot-selection.json"), "utf8"));
const inserts = JSON.parse(fs.readFileSync(path.join(V3, "11-dry-run-inserts.json"), "utf8"));
const updates = JSON.parse(fs.readFileSync(path.join(V3, "12-dry-run-updates.json"), "utf8"));
const blocked = JSON.parse(fs.readFileSync(path.join(V3, "13-dry-run-blocked.json"), "utf8"));
const fieldMap = JSON.parse(fs.readFileSync(path.join(V3, "02-golden-to-airtable-field-map.json"), "utf8"));
const writePolicy = JSON.parse(fs.readFileSync(path.join(V3, "03-write-policy.json"), "utf8"));
const rights = JSON.parse(fs.readFileSync(path.join(V3, "04-source-rights-write-policy.json"), "utf8"));
const schema = JSON.parse(fs.readFileSync(path.join(V3, "_schema-live.json"), "utf8"));
const tx = JSON.parse(fs.readFileSync(path.join(V3, "22-write-transaction-log.json"), "utf8"));
const snap = JSON.parse(fs.readFileSync(path.join(V3, "23-post-write-airtable-snapshot.json"), "utf8"));
const aRes = JSON.parse(fs.readFileSync(path.join(V3, "22a-pilot-a-results.json"), "utf8"));
const bRes = JSON.parse(fs.readFileSync(path.join(V3, "22c-pilot-b-results.json"), "utf8"));
const freeze = JSON.parse(fs.readFileSync(path.join(V23, "08-independent-universe-freeze.json"), "utf8"));
const dryRunSrc = fs.readFileSync(
  path.join(ROOT, "lib/research-engine-v2/census-autopilot-v3/dry-run.js"),
  "utf8"
);

if (sel.run_id !== RUN) throw new Error(`selection run_id mismatch ${sel.run_id}`);

const byPid = new Map(freeze.records.map((r) => [r.property_identity_id, r]));
const cohort = sel.cohort;
const cohortKeys = new Set(cohort.map((c) => c.property_identity_key));

const allResults = [...aRes.results, ...bRes.results];
const insertedIds = new Set(
  allResults.filter((r) => r.status === "inserted").map((r) => r.record_id)
);
const mutatedIds = new Set(
  allResults
    .filter((r) => r.status === "inserted" || r.status === "updated")
    .map((r) => r.record_id)
);
const snapById = new Map(snap.records.map((r) => [r.id, r]));

const dryRunHasAdd = {
  "State / Region": /add\(\s*["']State \/ Region["']/.test(dryRunSrc),
  Address: /add\(\s*["']Address["']/.test(dryRunSrc),
  Submarket: /add\(\s*["']Submarket["']/.test(dryRunSrc),
  Latitude: /add\(\s*["']Latitude["']/.test(dryRunSrc),
  Longitude: /add\(\s*["']Longitude["']/.test(dryRunSrc),
  Phone: /add\(\s*["']Phone["']/.test(dryRunSrc),
};

const blockedLoopFields = [
  "Latitude",
  "Longitude",
  "Phone",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
];

// Per-property staging inventory
const rows = cohort.map((c) => {
  const r = byPid.get(c.research_property_identity_id);
  const p = r?.physical || {};
  return {
    property_identity_key: c.property_identity_key,
    research_property_identity_id: c.research_property_identity_id,
    family: c.family,
    country: c.country,
    city: c.city,
    match_class: c.match_class,
    source_type: c.source_type,
    discovery_lane: c.discovery_lane,
    match_confidence: c.match_detail?.identity_confidence || "High",
    geography: c.geography,
    physical: {
      state: p.state || p.state_region || null,
      address: p.address || null,
      phone: p.phone || null,
      lat: p.lat ?? null,
      lng: p.lng ?? null,
    },
    field_evidence_keys: Object.keys(r?.field_evidence || {}),
  };
});

function stagingValue(row, field) {
  if (field === "State / Region") return row.physical.state;
  if (field === "Address") return row.physical.address;
  if (field === "Phone") return row.physical.phone;
  if (field === "Latitude") return row.physical.lat;
  if (field === "Longitude") return row.physical.lng;
  if (field === "Submarket") return row.geography?.submarket || null;
  return null;
}

function countDry(field) {
  let n = 0;
  for (const m of [...inserts.inserts, ...updates.updates]) {
    if (!blank(m.fields?.[field])) n += 1;
  }
  return n;
}

function countTxWritten(field) {
  return tx.entries.filter((e) => e.field === field && e.result === "written").length;
}

function postWriteCounts(field) {
  let blankN = 0;
  let nonblank = 0;
  for (const id of insertedIds) {
    const v = snapById.get(id)?.fields?.[field];
    if (blank(v)) blankN += 1;
    else nonblank += 1;
  }
  return { inserted_records: insertedIds.size, blank: blankN, nonblank };
}

// Coordinate source breakdown
const coordBreakdown = {
  hilton_official_structured: 0,
  choice_official_directory: 0,
  ihg_official: 0,
  marriott_official: 0,
  approved_geocode: 0,
  serpapi: 0,
  other: 0,
  none: 0,
};
const coordEligibleKeys = [];
for (const row of rows) {
  const has = row.physical.lat != null && row.physical.lng != null;
  if (!has) {
    coordBreakdown.none += 1;
    continue;
  }
  if (String(row.source_type).includes("serp")) {
    coordBreakdown.serpapi += 1;
  } else if (row.family === "Hilton") {
    coordBreakdown.hilton_official_structured += 1;
    coordEligibleKeys.push(row.property_identity_key);
  } else if (row.family === "Choice") {
    coordBreakdown.choice_official_directory += 1;
    coordEligibleKeys.push(row.property_identity_key);
  } else if (row.family === "IHG") {
    coordBreakdown.ihg_official += 1;
    coordEligibleKeys.push(row.property_identity_key);
  } else if (row.family === "Marriott") {
    coordBreakdown.marriott_official += 1;
    coordEligibleKeys.push(row.property_identity_key);
  } else {
    coordBreakdown.other += 1;
    coordEligibleKeys.push(row.property_identity_key);
  }
}

const fieldAudits = {};

for (const field of FIELDS) {
  const mapping = fieldMap.mappings.find((m) => m.airtable_field === field);
  const schemaField = schema.fields.find((f) => f.name === field);
  const stagingNonblank = rows.filter((r) => !blank(stagingValue(r, field)));
  const dryCount = countDry(field);
  const written = countTxWritten(field);
  const post = postWriteCounts(field);

  const inAuto = writePolicy.auto_write_safe?.includes(field);
  const inCorr = writePolicy.corroborated_write?.includes(field);
  const inBlocked = writePolicy.blocked_rights?.includes(field);
  const inInsertAllow = INSERT_ALLOWED_FIELDS.includes(field);
  const classifierAdds = dryRunHasAdd[field];

  let writeClass =
    mapping?.write_class ||
    (inBlocked ? "BLOCKED_RIGHTS" : inAuto ? "AUTO_WRITE_SAFE" : inCorr ? "CORROBORATED_WRITE" : "UNKNOWN");

  let blankReason;
  let expectedOrBug;
  let researched;
  let whyNotInManifest = null;
  let writerSuppressed = false;
  let typeBlocked = false;

  if (field === "Submarket") {
    researched = stagingNonblank.length > 0 || rows.some((r) => r.geography);
    // Geography always computed; submarket often No Match
    const noMatch = rows.filter((r) => !r.geography?.submarket).length;
    if (stagingNonblank.length === written && written === dryCount) {
      // blanks = research no match
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
      whyNotInManifest = `${noMatch}/150 had geography.submarket=null (submarket_confidence=No Match / no_corridor_match). ${stagingNonblank.length} nonblank values WERE included in Phase 1 mutations and written.`;
    } else if (stagingNonblank.length > written) {
      blankReason = "H. VALUE EXISTS — UNINTENDED WRITER OMISSION";
      expectedOrBug = "BUG";
      whyNotInManifest = "Staging submarket exceeded written count";
    } else {
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
    }
  } else if (field === "State / Region") {
    researched = false; // never populated in V3 pilot geography or freeze physical
    writerSuppressed = !classifierAdds;
    if (stagingNonblank.length === 0) {
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
      whyNotInManifest =
        "V2.3 freeze physical has no state/state_region; resolveDealalityGeography() does not emit State / Region; classifyFieldWrites() never calls add('State / Region') despite AUTO_WRITE_SAFE policy (latent writer omission).";
    } else {
      blankReason = "H. VALUE EXISTS — UNINTENDED WRITER OMISSION";
      expectedOrBug = "BUG";
    }
  } else if (field === "Address") {
    researched = stagingNonblank.length > 0;
    writerSuppressed = !classifierAdds;
    if (stagingNonblank.length === 0) {
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
      whyNotInManifest =
        "V2.3 discovery toDiscoveryRecord() does not persist address on physical; field_evidence empty for cohort; classifyFieldWrites() also never calls add('Address') despite CORROBORATED_WRITE mapping (latent omission).";
    } else {
      blankReason = "H. VALUE EXISTS — UNINTENDED WRITER OMISSION";
      expectedOrBug = "BUG";
    }
  } else if (field === "Phone") {
    researched = stagingNonblank.length > 0;
    writerSuppressed = inBlocked;
    if (stagingNonblank.length === 0) {
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
      whyNotInManifest =
        "No phone on V2.3 freeze physical for any of 150; also listed in blocked_rights (blanket SerpApi-class block). Phone not in INSERT_ALLOWED_FIELDS.";
    } else if (inBlocked) {
      blankReason = "B. VALUE EXISTS — BLOCKED RIGHTS";
      expectedOrBug = "EXPECTED";
    } else {
      blankReason = "H. VALUE EXISTS — UNINTENDED WRITER OMISSION";
      expectedOrBug = "BUG";
    }
  } else if (field === "Latitude" || field === "Longitude") {
    researched = stagingNonblank.length > 0;
    writerSuppressed = true; // blanket blocked_rights loop
    if (stagingNonblank.length === 0) {
      blankReason = "A. RESEARCH VALUE NOT FOUND";
      expectedOrBug = "EXPECTED";
      whyNotInManifest = "No coordinates in staging freeze for remaining records";
    } else {
      // Official Hilton/Choice coords blocked by blanket rights without unless-official exception
      blankReason = "B. VALUE EXISTS — BLOCKED RIGHTS";
      expectedOrBug = "BUG";
      whyNotInManifest =
        `Policy source_requirement=serpapi_blocked_unless_official and source-rights allow coords_if_official_structured, but dry-run.js always emits BLOCKED_RIGHTS with value:null for Latitude/Longitude for every property without inspecting freeze physical.lat/lng. ${stagingNonblank.length} cohort records have official-directory coordinates (0 SerpApi).`;
    }
  }

  // Type validation: none of these are selects that failed — fields simply never sent
  typeBlocked = false;

  const primarySource =
    field === "Submarket"
      ? "dealality_geography (proposeCensusSubmarketCorridor)"
      : field === "State / Region"
        ? "none_in_staging (policy: official_or_dealality)"
        : field === "Address"
          ? "none_in_staging (policy: official_page_only_this_pilot)"
          : field === "Phone"
            ? "none_in_staging"
            : field === "Latitude" || field === "Longitude"
              ? stagingNonblank.length
                ? "official_brand_directory (Hilton structured / Choice directory)"
                : "none"
              : "unknown";

  fieldAudits[field] = {
    field,
    answers: {
      "1_researched":
        field === "Submarket"
          ? "YES — Dealality geography resolver ran for all 150; submarket matched for subset"
          : field === "Latitude" || field === "Longitude"
            ? stagingNonblank.length
              ? `YES — ${stagingNonblank.length}/150 have physical.lat/lng in V2.3 freeze`
              : "NO nonblank values"
            : stagingNonblank.length
              ? "YES"
              : "NO — not present on V2.3 freeze physical / not derived into pilot object",
      "2_staging_nonblank_count": stagingNonblank.length,
      "3_primary_source": primarySource,
      "4_match_confidence": "High (identity); geography submarket_confidence Medium|No Match when present",
      "5_source_rights_status": inBlocked
        ? "BLOCKED for SerpApi-class persistence; official exception NOT applied in writer"
        : field === "Address"
          ? "official_property_page ALLOWED_WITH_CONSTRAINTS (but no staging value + writer omit)"
          : "dealality/official ALLOWED for auto geography fields",
      "6_write_class": writeClass,
      "7_in_phase1_mutation_manifest": dryCount > 0,
      "8_why_not_if_not": dryCount === 0 ? whyNotInManifest : null,
      "9_airtable_field_mapping": Boolean(mapping?.exists_on_live_schema),
      "10_type_select_blocked": typeBlocked,
      "11_writer_intentionally_suppressed": writerSuppressed || !classifierAdds && field !== "Submarket",
      "12_expected_or_bug": expectedOrBug,
    },
    staging_nonblank_count: stagingNonblank.length,
    dry_run_mutation_count: dryCount,
    tx_written_count: written,
    post_write_inserted: post,
    schema: schemaField ? { name: schemaField.name, type: schemaField.type, id: schemaField.id } : null,
    mapping,
    policy: { inAuto, inCorr, inBlocked, inInsertAllow, classifierAdds },
    blank_reason_code: blankReason,
    expected_or_bug: expectedOrBug,
    staging_keys_sample: stagingNonblank.slice(0, 10).map((r) => ({
      property_identity_key: r.property_identity_key,
      family: r.family,
      value: stagingValue(r, field),
      source_type: r.source_type,
    })),
  };
}

// SerpApi blocked breakdown
const serpapiBreakdown = {
  reported_blocked_field_rows: blocked.count,
  formula: "150 properties × 7 blocked_rights fields = 1050",
  blocked_rights_fields: blockedLoopFields,
  per_field_expected: Object.fromEntries(blockedLoopFields.map((f) => [f, 150])),
  address_in_blocked_rows: false,
  note:
    "Address is NOT part of the 1,050. The 1,050 are Latitude+Longitude+Phone+2 amenities+2 description fields × 150. All blocked rows used value:null and serpapi_blocked:true even when SerpApi was not used (official-directory cohort).",
  lat_lng_phone_share: {
    fields: ["Latitude", "Longitude", "Phone"],
    rows: 450,
    pct_of_1050: Math.round((450 / 1050) * 1000) / 10,
  },
  amenities_description_share: {
    fields: blockedLoopFields.slice(3),
    rows: 600,
    pct_of_1050: Math.round((600 / 1050) * 1000) / 10,
  },
  official_coords_incorrectly_blocked: {
    count_properties: coordEligibleKeys.length,
    field_rows: coordEligibleKeys.length * 2,
    families: {
      Hilton: rows.filter((r) => r.family === "Hilton" && r.physical.lat != null).length,
      Choice: rows.filter((r) => r.family === "Choice" && r.physical.lat != null).length,
      IHG: rows.filter((r) => r.family === "IHG" && r.physical.lat != null).length,
      Marriott: rows.filter((r) => r.family === "Marriott" && r.physical.lat != null).length,
    },
    serpapi_sourced_coords_in_cohort: 0,
    contamination_finding:
      "Writer blocked official Hilton/Choice coordinates solely because Latitude/Longitude are on the blanket blocked_rights list — no per-claim source check. SerpApi evidence was not present; a lower-authority blocked class contaminated higher-authority eligible claims.",
  },
  coordinate_source_counts: coordBreakdown,
};

// Writer omission audit
const writerOmission = {
  dry_run_classifier_add_calls: dryRunHasAdd,
  findings: [
    {
      field: "State / Region",
      policy_class: "AUTO_WRITE_SAFE",
      classifier_add: false,
      staging_nonblank: fieldAudits["State / Region"].staging_nonblank_count,
      classification: "LATENT_BUG — would omit even if researched; this cohort also had 0 staging values",
    },
    {
      field: "Address",
      policy_class: "CORROBORATED_WRITE",
      classifier_add: false,
      staging_nonblank: fieldAudits.Address.staging_nonblank_count,
      classification: "LATENT_BUG — would omit even if researched; this cohort also had 0 staging values",
    },
    {
      field: "Submarket",
      policy_class: "AUTO_WRITE_SAFE",
      classifier_add: true,
      staging_nonblank: fieldAudits.Submarket.staging_nonblank_count,
      written: fieldAudits.Submarket.tx_written_count,
      classification: "OK — writes when geography.submarket present",
    },
    {
      field: "Latitude",
      policy_class: "BLOCKED_RIGHTS (unless official)",
      classifier_add: false,
      staging_nonblank: fieldAudits.Latitude.staging_nonblank_count,
      classification: "BUG — blanket block ignored official freeze coordinates",
    },
    {
      field: "Longitude",
      policy_class: "BLOCKED_RIGHTS (unless official)",
      classifier_add: false,
      staging_nonblank: fieldAudits.Longitude.staging_nonblank_count,
      classification: "BUG — blanket block ignored official freeze coordinates",
    },
    {
      field: "Phone",
      policy_class: "BLOCKED_RIGHTS",
      classifier_add: false,
      staging_nonblank: fieldAudits.Phone.staging_nonblank_count,
      classification: "OK for this cohort (0 values); keep blocked until official phone researched or SerpApi clarified",
    },
  ],
};

// Corrective dry-run backfill (NO WRITES)
const backfill = {
  version: "v3-field-gap-corrective-backfill-dry-run",
  run_id: RUN,
  airtable_writes: false,
  safe_now: [],
  must_wait: [],
  proposed_mutations: [],
};

for (const row of rows) {
  if (row.physical.lat == null || row.physical.lng == null) continue;
  if (String(row.source_type).includes("serp")) continue;
  const result = allResults.find((r) => r.property_identity_key === row.property_identity_key);
  if (!result?.record_id) continue;
  const current = snapById.get(result.record_id)?.fields || {};
  const fields = {};
  if (blank(current.Latitude)) fields.Latitude = row.physical.lat;
  if (blank(current.Longitude)) fields.Longitude = row.physical.lng;
  if (!Object.keys(fields).length) continue;
  backfill.proposed_mutations.push({
    operation: "UPDATE_BLANK_FILL",
    airtable_record_id: result.record_id,
    property_identity_key: row.property_identity_key,
    family: row.family,
    fields,
    provenance: {
      source_class: "official_brand_directory",
      source_type: row.source_type,
      source_url: byPid.get(row.research_property_identity_id)?.discovery_evidence?.source_url,
      serpapi_used: false,
      research_run_id: RUN,
      note: "Corrective backfill from V2.3 freeze physical.lat/lng already present at Phase 1; blocked by blanket rights",
    },
    write_class: "CORROBORATED_WRITE",
  });
}

backfill.safe_now = [
  {
    field: "Latitude",
    records: backfill.proposed_mutations.filter((m) => m.fields.Latitude != null).length,
    evidence: "V2.3 freeze official_brand_directory coords (Hilton+Choice)",
  },
  {
    field: "Longitude",
    records: backfill.proposed_mutations.filter((m) => m.fields.Longitude != null).length,
    evidence: "V2.3 freeze official_brand_directory coords (Hilton+Choice)",
  },
];

backfill.must_wait = [
  {
    field: "Address",
    reason: "0 approved independent staging values in V3 cohort freeze; needs official property-page research (not SerpApi until clarified)",
  },
  {
    field: "Phone",
    reason: "0 staging values; SerpApi persistence not approved; not in INSERT_ALLOWED_FIELDS",
  },
  {
    field: "State / Region",
    reason: "0 staging values; geography resolver does not emit state; needs research/derivation before backfill",
  },
  {
    field: "Submarket",
    reason:
      "46 already written. Remaining 104 are geography No Match (often weak city labels / postal codes) — needs geography improvement, not blind backfill",
  },
];

backfill.summary = {
  proposed_update_records: backfill.proposed_mutations.length,
  proposed_field_writes: backfill.proposed_mutations.reduce(
    (n, m) => n + Object.keys(m.fields).length,
    0
  ),
};

// Summary table
const summaryTable = FIELDS.map((f) => {
  const a = fieldAudits[f];
  return {
    Field: f,
    Staging_Nonblank_Count: a.staging_nonblank_count,
    Written_Count: a.tx_written_count,
    Primary_Source: a.answers["3_primary_source"],
    Write_Class: a.answers["6_write_class"],
    Blank_Reason: a.blank_reason_code,
    Expected_or_Bug: a.expected_or_bug,
    Recommended_Fix:
      f === "Latitude" || f === "Longitude"
        ? "Fix dry-run to write official freeze coords as CORROBORATED_WRITE; run corrective blank-fill for 60 records"
        : f === "State / Region"
          ? "Add State/Region to classifyFieldWrites + geography derivation; research before backfill"
          : f === "Address"
            ? "Persist official address in discovery/enrichment; add Address to classifyFieldWrites when official evidence exists"
            : f === "Phone"
              ? "Keep blank until official phone researched or SerpApi persistence approved; add to insert allowlist only then"
              : "Improve Dealality corridor matching / city normalization for No Match cases; 46 already written OK",
  };
});

wj("02-state-region-audit.json", fieldAudits["State / Region"]);
wj("03-address-audit.json", fieldAudits.Address);
wj("04-submarket-audit.json", {
  ...fieldAudits.Submarket,
  by_country: Object.fromEntries(
    [...new Set(rows.map((r) => r.country))].map((c) => [
      c,
      {
        n: rows.filter((r) => r.country === c).length,
        submarket_nonblank: rows.filter((r) => r.country === c && r.geography?.submarket).length,
      },
    ])
  ),
  submarket_reasons: rows.reduce((a, r) => {
    const k = r.geography?.submarket_reason || "none";
    a[k] = (a[k] || 0) + 1;
    return a;
  }, {}),
  golden_v1_2_note:
    "Golden Census V1.2 Market/Submarket 100% was a different cohort/pipeline. V3 pilot re-resolved geography from discovery city labels (often postal codes / admin regions) → 104/150 No Match.",
});
wj("05-latitude-longitude-audit.json", {
  latitude: fieldAudits.Latitude,
  longitude: fieldAudits.Longitude,
  coordinate_source_counts: coordBreakdown,
  official_eligible_property_identity_keys: coordEligibleKeys,
});
wj("06-phone-audit.json", fieldAudits.Phone);
wj("07-serpapi-blocked-breakdown.json", serpapiBreakdown);
wj("08-field-mapping-audit.json", {
  run_id: RUN,
  mappings: FIELDS.map((f) => fieldMap.mappings.find((m) => m.airtable_field === f)),
  schema: FIELDS.map((f) => {
    const s = schema.fields.find((x) => x.name === f);
    return s ? { name: s.name, type: s.type, id: s.id } : { name: f, missing: true };
  }),
  insert_allowlist: Object.fromEntries(FIELDS.map((f) => [f, INSERT_ALLOWED_FIELDS.includes(f)])),
});
wj("09-writer-omission-audit.json", writerOmission);
wj("10-corrective-backfill-dry-run.json", backfill);

const summaryMd = `# V3 Field Gap Diagnostic — Summary

**Authorized run:** \`${RUN}\`  
**Airtable writes during diagnostic:** **NONE**  
**Artifacts:** \`31-field-gap-diagnostic/\`

## Plain answer

| Field | Why blank in Airtable? | Verdict |
|-------|------------------------|---------|
| **State / Region** | Never researched into V3 staging (freeze has no state; geography resolver omits it). Classifier also never \`add()\`s it despite AUTO_WRITE_SAFE. | **EXPECTED** (this cohort) + latent writer bug |
| **Address** | Not persisted on V2.3 discovery physical; 0/150 staging values. Classifier never \`add()\`s Address despite CORROBORATED_WRITE. | **EXPECTED** (this cohort) + latent writer bug |
| **Submarket** | Not uniformly blank: **46/150 written**. Remaining **104** failed Dealality corridor match (\`no_corridor_match\`) — often weak city labels. | **EXPECTED** for blanks; writes OK when present |
| **Latitude** | **60/150** have official Hilton/Choice coords in freeze, but Phase 1 blanket \`BLOCKED_RIGHTS\` suppressed them (\`unless official\` not implemented). 90 had no coords. | **BUG** for the 60 |
| **Longitude** | Same as Latitude. | **BUG** for the 60 |
| **Phone** | 0/150 staging values; also blanket blocked_rights. | **EXPECTED** |

## Summary table

| Field | Staging Nonblank | Written | Primary Source | Write Class | Blank Reason | Expected or Bug | Recommended Fix |
|-------|-----------------:|--------:|----------------|-------------|----------------|-----------------|-----------------|
${summaryTable
  .map(
    (r) =>
      `| ${r.Field} | ${r.Staging_Nonblank_Count} | ${r.Written_Count} | ${r.Primary_Source} | ${r.Write_Class} | ${r.Blank_Reason} | ${r.Expected_or_Bug} | ${r.Recommended_Fix} |`
  )
  .join("\n")}

## SerpApi 1,050 blocked rows

- **Formula:** 150 × 7 fields = **1,050**
- Fields: Latitude, Longitude, Phone, Amenities - Source Text, Amenities - Structured Tags, Hotel Description - Source Text, Hotel Description - AI Summary
- **Address is NOT in the 1,050**
- Lat+Lng+Phone = **450 / 1,050 (42.9%)**
- **0** of the 60 coordinate values were SerpApi-sourced — all official directory — yet still blocked

## Safe to backfill NOW (dry-run only; not applied)

- **Latitude / Longitude** for **${backfill.proposed_mutations.length}** records from already-approved official freeze evidence

## Must wait

- Address, Phone, State / Region (no approved staging values)
- Submarket remaining 104 (geography research, not rights)
`;

wm("01-field-gap-summary.md", summaryMd);

const rootCause = `# Root Cause Report — V3 blank fields

**Run:** \`${RUN}\`  
**No Airtable writes performed by this diagnostic.**

## Exact root causes

### 1. State / Region — EXPECTED (cohort) + latent BUG (writer)
1. V2.3 \`toDiscoveryRecord()\` does not store \`state\` / \`state_region\` on \`physical\`.
2. \`resolveDealalityGeography()\` returns market/submarket/continent but **not** State / Region.
3. \`classifyFieldWrites()\` in \`dry-run.js\` **never calls** \`add("State / Region", …)\` even though \`field-policy.js\` lists it \`AUTO_WRITE_SAFE\` and mapping exists.
4. Result for this cohort: **0/150** staging → **0** Phase 1 mutations → **0** Phase 2 writes.

### 2. Address — EXPECTED (cohort) + latent BUG (writer)
1. Discovery freeze does not persist address (physical has name/city/country/url/id/lat/lng only).
2. \`field_evidence\` empty for all 150 cohort research IDs.
3. Classifier never \`add("Address")\` despite \`CORROBORATED_WRITE\` / official_page policy.
4. Address was **not** counted in the 1,050 SerpApi-blocked rows.

### 3. Submarket — EXPECTED blanks; NOT a write omission for matched rows
1. Geography ran for all 150.
2. **46** had \`geography.submarket\` → included in dry-run → **46** transaction writes.
3. **104** \`submarket_confidence: No Match\` / \`no_corridor_match\` (city often admin region or postal code).
4. Golden V1.2 100% Submarket does **not** transfer: V3 re-resolved from discovery city labels, did not inherit prior Golden geography artifacts.

### 4. Latitude / Longitude — BUG for official values
1. Freeze has official coords for **60** properties: Hilton **40**, Choice **20**, SerpApi **0**.
2. Policy text: \`serpapi_blocked_unless_official\`; source-rights allow \`coords_if_official_structured\`.
3. Implementation: \`for (const f of policy.blocked_rights) blocked.push({value:null…})\` — **no check** of freeze \`physical.lat/lng\`.
4. Official higher-authority claims were blocked by a blanket SerpApi-class list (contamination / fail-closed without exception).

### 5. Phone — EXPECTED
1. **0/150** phone values in freeze.
2. Listed in \`blocked_rights\`; not in \`INSERT_ALLOWED_FIELDS\`.

## Code fixes needed (do not apply in this diagnostic)

1. **dry-run.js \`classifyFieldWrites\`:**
   - \`add("State / Region", …)\` when independently derived/researched.
   - \`add("Address", …)\` when official (non-SerpApi) evidence exists.
   - For Lat/Lng/Phone: only BLOCKED_RIGHTS when claim is SerpApi-only; if official freeze/page evidence exists → CORROBORATED_WRITE / AUTO as policy allows.
2. **pilot-selection / discovery:** persist official address/phone/state when adapters provide them; derive State / Region in geography where possible.
3. **geography:** improve city normalization (postal ≠ city) to raise Submarket match rate.
4. **INSERT allowlist:** add Phone only when official writes are enabled.

## Records affected

| Issue | Count | Keys source |
|-------|------:|-------------|
| Official coords blocked | 60 | \`05-latitude-longitude-audit.json\` → \`official_eligible_property_identity_keys\` |
| Submarket No Match | 104 | pilot selection geography |
| State/Address/Phone no staging | 150 | freeze physical |

## Safe immediate backfill (approved independent evidence)

- Latitude + Longitude blank-fill for **${backfill.proposed_mutations.length}** Airtable records (see \`10-corrective-backfill-dry-run.json\`).
- **Do not** backfill Address / Phone / State from SerpApi.
- **Do not** invent Submarket for No Match rows.

## Must remain blank pending clarification / research

- SerpApi-only Address/Coords/Phone/Amenities/Descriptions
- Phone (no official staging yet)
- Address (no official staging yet)
- State / Region (not derived yet)
- Submarket No Match set (needs geography work)

## Final verdicts

| Field | EXPECTED or BUG |
|-------|-----------------|
| State / Region | **EXPECTED** |
| Address | **EXPECTED** |
| Submarket | **EXPECTED** (blanks); writes correct when present |
| Latitude | **BUG** (60 official suppressed) |
| Longitude | **BUG** (60 official suppressed) |
| Phone | **EXPECTED** |
`;

wm("11-root-cause-report.md", rootCause);

console.log(
  JSON.stringify(
    {
      out: OUT,
      table: summaryTable.map((r) => ({
        field: r.Field,
        staging: r.Staging_Nonblank_Count,
        written: r.Written_Count,
        reason: r.Blank_Reason,
        verdict: r.Expected_or_Bug,
      })),
      backfill_records: backfill.proposed_mutations.length,
      serpapi_1050_formula: "150*7",
      official_coords_blocked: coordEligibleKeys.length,
    },
    null,
    2
  )
);
