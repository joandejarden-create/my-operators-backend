/**
 * External hotel content source pilot runner — default no writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  createExternalSourceAdapterStub,
  listPilotSources,
  resolveExternalSource,
} from "./external-hotel-source-registry.js";
import {
  assertExternalSourceWriteAllowed,
  isFieldApprovedForSource,
  resolveExternalWriteGate,
  sanitizeExternalPatch,
  EXTERNAL_ENRICHMENT_FIELDS,
} from "./external-hotel-source-policy.js";
import {
  matchExternalToCensusPool,
  scoreExternalToCensusMatch,
  MATCH_CONFIDENCE,
} from "./external-hotel-match-engine.js";
import {
  reportExternalProvenanceSchemaGaps,
  attachRoomsProvenance,
  buildExternalProvenanceNotes,
} from "./external-hotel-field-provenance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const EXTERNAL_SOURCE_PILOT_VERSION = "external-hotel-source-pilot-runner-v1";

function isBlank(v) {
  return v == null || !String(v).trim();
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(filePath, md) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

/**
 * Estimate which incomplete fields a source *could* help (capability-based, not live API).
 * @param {object[]} incompleteRecords
 * @param {object} source
 */
export function estimateCoverageImprovement(incompleteRecords, source) {
  const caps = source?.capabilities || {};
  const fieldMap = {
    hotel_name: ["Canonical Property Name"],
    address: ["Address"],
    lat_long: ["Latitude", "Longitude"],
    phone: ["Phone"],
    hotel_website: ["Official Property URL"],
    rooms: ["Rooms / Keys"],
    brand_chain: ["Current Brand", "Brand Family"],
    descriptions: ["Hotel Description - AI Summary"],
  };

  /** @type {Record<string, { missing: number, potentially_fillable: number }>} */
  const byField = {};
  for (const [cap, fields] of Object.entries(fieldMap)) {
    const capable = caps[cap] === "yes" || caps[cap] === "partial";
    for (const field of fields) {
      const missing = incompleteRecords.filter((r) =>
        isBlank(r.fields?.[field])
      ).length;
      byField[field] = {
        missing,
        potentially_fillable: capable ? missing : 0,
        capability: caps[cap] || "unknown",
      };
    }
  }

  const roomsMissing = byField["Rooms / Keys"]?.missing || 0;
  const addressMissing = byField.Address?.missing || 0;
  const phoneMissing = byField.Phone?.missing || 0;

  return {
    sample_size: incompleteRecords.length,
    by_field: byField,
    headline: {
      rooms_potentially_fillable: byField["Rooms / Keys"]?.potentially_fillable || 0,
      address_potentially_fillable: byField.Address?.potentially_fillable || 0,
      phone_potentially_fillable: byField.Phone?.potentially_fillable || 0,
      lat_long_potentially_fillable: byField.Latitude?.potentially_fillable || 0,
    },
    note:
      "Estimates assume licensed access + successful High/Medium matches. Live API not called in pilot-only stub mode.",
    rooms_missing_in_sample: roomsMissing,
    address_missing_in_sample: addressMissing,
    phone_missing_in_sample: phoneMissing,
  };
}

/**
 * Build synthetic external candidates from Census itself for match-engine self-test
 * (does not invent vendor data — uses census name/city as "external" for scoring demos).
 * @param {object} fields
 */
export function buildSyntheticSelfCandidate(fields) {
  return {
    name: fields["Canonical Property Name"] || fields["Property Name"],
    city: fields.City,
    country: fields.Country,
    brand: fields["Current Brand"],
    address: fields.Address,
    lat: fields.Latitude,
    lng: fields.Longitude,
    official_url: fields["Official Property URL"],
    external_id: fields["Property Identity Key"] || null,
  };
}

/**
 * Run pilot for one source against incomplete census sample.
 * @param {{
 *   sourceId: string,
 *   records: object[],
 *   sampleSize?: number,
 *   pilotOnly?: boolean,
 *   env?: NodeJS.ProcessEnv,
 *   existingFieldNames?: string[],
 *   log?: Function,
 * }} opts
 */
export function runExternalSourcePilot(opts = {}) {
  const log = opts.log || (() => {});
  const env = opts.env || process.env;
  const pilotOnly = opts.pilotOnly !== false;
  const source = resolveExternalSource(opts.sourceId);
  if (!source) {
    return {
      ok: false,
      status: "blocked_unknown_source",
      source_id: opts.sourceId,
      airtable_writes: false,
    };
  }

  const writeGate = assertExternalSourceWriteAllowed(source.id, {
    pilotOnly,
    env,
  });
  const adapter = createExternalSourceAdapterStub(source.id);

  const incomplete = (opts.records || []).filter((r) => {
    const f = r.fields || {};
    return EXTERNAL_ENRICHMENT_FIELDS.some((field) => isBlank(f[field]));
  });

  const sampleSize = Math.min(
    Number(opts.sampleSize || 75),
    incomplete.length || (opts.records || []).length
  );
  const sample = incomplete.slice(0, sampleSize);

  log(
    `[external-pilot] source=${source.id} sample=${sample.length} pilotOnly=${pilotOnly} writes=${writeGate.write}`
  );

  const matchScores = [];
  let matchesHigh = 0;
  let matchesMedium = 0;
  let matchesNone = 0;

  for (const rec of sample) {
    const external = buildSyntheticSelfCandidate(rec.fields || {});
    // Self-candidate vs pool excluding self → tests ambiguity; vs self → high
    const selfScore = scoreExternalToCensusMatch(rec.fields || {}, external);
    const poolMatch = matchExternalToCensusPool(
      sample.filter((r) => r.id !== rec.id).slice(0, 40).concat([rec]),
      external,
      { requireWritable: true }
    );
    if (selfScore.confidence === MATCH_CONFIDENCE.HIGH) matchesHigh += 1;
    else if (selfScore.confidence === MATCH_CONFIDENCE.MEDIUM) matchesMedium += 1;
    else matchesNone += 1;

    matchScores.push({
      census_record_id: rec.id,
      self_score: selfScore,
      pool_match_ok: poolMatch.ok,
      pool_reason: poolMatch.reason,
    });
  }

  const coverage = estimateCoverageImprovement(sample, source);

  /** Field approval matrix for this source */
  const fieldApprovals = {};
  for (const field of EXTERNAL_ENRICHMENT_FIELDS) {
    fieldApprovals[field] = isFieldApprovedForSource(field, source.id, env);
  }

  const blockedFields = Object.entries(fieldApprovals)
    .filter(([, v]) => !v.ok)
    .map(([field, v]) => ({ field, reason: v.reason }));
  const recommendedFields = Object.entries(fieldApprovals)
    .filter(([, v]) => v.ok)
    .map(([field]) => field);

  // Example sanitize — ensure rooms never get Google Places
  const examplePatch = sanitizeExternalPatch(
    {
      Address: "Example St 1",
      "Rooms / Keys": 120,
      Phone: "+10000000000",
      Owner: "SHOULD_REJECT",
    },
    source.id,
    env
  );

  const provenanceGaps = reportExternalProvenanceSchemaGaps(
    opts.existingFieldNames || [
      "Rooms Source URL",
      "Rooms Source Type",
      "Rooms Evidence Tier",
      "Rooms Confidence",
      "Rooms Notes",
      "Address Source URL",
    ]
  );

  const goNoGo = source.go_no_go;
  let recommendation = "no_go";
  if (goNoGo === "go_already_in_production_paths" || goNoGo === "go_for_existing_adapters_expand_country_by_country") {
    recommendation = "go";
  } else if (String(goNoGo || "").startsWith("conditional_go")) {
    recommendation = "conditional_go";
  } else if (goNoGo === "no_go_until_storage_policy_approved" || goNoGo === "no_go_until_odbl_policy_approved") {
    recommendation = "policy_decision_needed";
  } else if (String(goNoGo || "").includes("license") || String(goNoGo || "").includes("partnership") || String(goNoGo || "").includes("credentials")) {
    recommendation = "vendor_access_needed";
  }

  const report = {
    ok: true,
    version: EXTERNAL_SOURCE_PILOT_VERSION,
    generated_at: new Date().toISOString(),
    source_evaluated: source.id,
    source_name: source.name,
    tier: source.tier,
    pilot_only: pilotOnly,
    airtable_writes: false,
    write_gate: writeGate,
    adapter_status: adapter.adapter_status,
    records_tested: sample.length,
    matches_found: {
      high_self_identity: matchesHigh,
      medium_self_identity: matchesMedium,
      none_or_low: matchesNone,
      note: "Self-identity scores validate match engine; live vendor search not configured.",
    },
    match_confidence_summary: {
      high_pct: sample.length ? +(100 * (matchesHigh / sample.length)).toFixed(1) : 0,
      medium_pct: sample.length ? +(100 * (matchesMedium / sample.length)).toFixed(1) : 0,
    },
    field_coverage_available: source.capabilities,
    expected_census_improvement: coverage,
    license_storage_concern: source.licensing_storage,
    cost_commercial_friction: source.cost_friction,
    recommended_fields: recommendedFields,
    blocked_fields: blockedFields,
    legal_notes: source.legal_notes,
    go_no_go_source: goNoGo,
    recommendation,
    provenance_schema: provenanceGaps,
    sanitize_example: examplePatch,
    sample_match_rows: matchScores.slice(0, 15),
  };

  return report;
}

/**
 * Persist pilot report files.
 * @param {object} report
 * @param {{ root?: string }} [opts]
 */
export function writeExternalSourcePilotReports(report, opts = {}) {
  const root = opts.root || ROOT;
  const id = report.source_evaluated || "unknown";
  const jsonPath = path.join(
    root,
    `reports/research-engine-v2/external-source-pilot-${id}.json`
  );
  const mdPath = path.join(
    root,
    `reports/research-engine-v2/external-source-pilot-${id}.md`
  );
  writeJson(jsonPath, report);
  const md = [
    `# External Source Pilot — ${report.source_name}`,
    ``,
    `**Source:** \`${report.source_evaluated}\``,
    `**Tier:** \`${report.tier}\``,
    `**Recommendation:** **${report.recommendation}**`,
    `**Pilot only / Airtable writes:** ${report.pilot_only} / ${report.airtable_writes}`,
    `**Generated:** ${report.generated_at}`,
    ``,
    `## Write gate`,
    ``,
    `- Allowed: ${report.write_gate?.write}`,
    `- Reason: \`${report.write_gate?.reason}\``,
    ``,
    `## Sample`,
    ``,
    `- Records tested: ${report.records_tested}`,
    `- High self-identity matches: ${report.matches_found?.high_self_identity}`,
    `- Medium: ${report.matches_found?.medium_self_identity}`,
    ``,
    `## Expected Census improvement (capability estimate)`,
    ``,
    "```json",
    JSON.stringify(report.expected_census_improvement?.headline || {}, null, 2),
    "```",
    ``,
    `## Recommended fields`,
    ``,
    ...(report.recommended_fields || []).map((f) => `- ${f}`),
    ``,
    `## Blocked fields`,
    ``,
    ...(report.blocked_fields || []).map((b) => `- ${b.field}: \`${b.reason}\``),
    ``,
    `## License / storage`,
    ``,
    `- ${report.license_storage_concern}`,
    `- Cost friction: ${report.cost_commercial_friction}`,
    ``,
    `## Legal notes`,
    ``,
    report.legal_notes || "",
    ``,
    `## Provenance schema gaps`,
    ``,
    `- Missing: ${(report.provenance_schema?.missing || []).join(", ") || "none"}`,
    ``,
  ].join("\n");
  writeMd(mdPath, md);
  return { jsonPath, mdPath };
}

/**
 * Build full evaluation matrix document payload.
 */
export function buildExternalSourceEvaluationDocument() {
  const sources = listPilotSources().concat([
    resolveExternalSource("ota_consumer_sites"),
  ].filter(Boolean));

  const rows = sources.map((s) => ({
    id: s.id,
    name: s.name,
    tier: s.tier,
    pilot_order: s.pilot_order,
    capabilities: s.capabilities,
    licensing_storage: s.licensing_storage,
    cost_friction: s.cost_friction,
    cala_coverage: s.cala_coverage,
    field_reliability: s.field_reliability,
    expected_match_quality: s.expected_match_quality,
    best_use_fields: s.best_use_fields,
    blocked_fields_until_license: s.blocked_fields_until_license,
    legal_notes: s.legal_notes,
    recommended_use: s.recommended_use,
    go_no_go: s.go_no_go,
    adapter_status: s.adapter_status,
  }));

  return {
    version: "hotel-census-external-source-evaluation-v1",
    generated_at: new Date().toISOString(),
    production_target: {
      base: "Deal Capture Platform",
      table: "Hotel Property Census",
      table_id: "tbl9aY5ijiuIzzWam",
    },
    tiers: {
      A: "Licensed Hotel Master Data",
      B: "Licensed Travel Content API",
      C: "Official / Government / Tourism Registry",
      D: "Place / Geo Verification API",
      E: "Public Web Verification",
    },
    pilot_order: [
      "giata / northstar",
      "expedia / hotelbeds / amadeus / booking",
      "google_places (policy first)",
      "tourism_registry country adapters",
      "cvent / costar / str",
    ],
    write_requirements: {
      ENABLE_EXTERNAL_HOTEL_CONTENT_WRITES: "1",
      APPROVED_EXTERNAL_SOURCE: "<source_id>",
      pilot_default: "no writes",
    },
    sources: rows,
  };
}

export function renderExternalSourceEvaluationMarkdown(doc) {
  const lines = [
    `# Hotel Census External Source Evaluation`,
    ``,
    `**Version:** \`${doc.version}\``,
    `**Generated:** ${doc.generated_at}`,
    `**Production target:** ${doc.production_target.base} → ${doc.production_target.table} (\`${doc.production_target.table_id}\`)`,
    ``,
    `## Evidence tiers`,
    ``,
    `- **Tier A** — ${doc.tiers.A}`,
    `- **Tier B** — ${doc.tiers.B}`,
    `- **Tier C** — ${doc.tiers.C}`,
    `- **Tier D** — ${doc.tiers.D}`,
    `- **Tier E** — ${doc.tiers.E}`,
    ``,
    `## Pilot order`,
    ``,
    ...doc.pilot_order.map((p, i) => `${i + 1}. ${p}`),
    ``,
    `## Write gate`,
    ``,
    `- Default: **pilot-only, no Census writes**`,
    `- Live writes require \`ENABLE_EXTERNAL_HOTEL_CONTENT_WRITES=1\` and \`APPROVED_EXTERNAL_SOURCE=<id>\``,
    `- Plus field approval, match confidence, and license/storage policy where gated`,
    ``,
    `## Source matrix`,
    ``,
  ];

  for (const s of doc.sources) {
    lines.push(
      `### ${s.name} (\`${s.id}\`)`,
      ``,
      `- Tier: \`${s.tier}\``,
      `- Pilot order: ${s.pilot_order}`,
      `- Adapter: \`${s.adapter_status}\``,
      `- Go/No-Go: **${s.go_no_go}**`,
      `- License/storage: ${s.licensing_storage}`,
      `- Cost friction: ${s.cost_friction}`,
      `- CALA coverage: ${s.cala_coverage}`,
      `- Reliability: ${s.field_reliability}`,
      `- Expected match quality: ${s.expected_match_quality}`,
      `- Best-use fields: ${(s.best_use_fields || []).join(", ") || "—"}`,
      `- Blocked until license/policy: ${(s.blocked_fields_until_license || []).join(", ") || "—"}`,
      `- Capabilities: ${JSON.stringify(s.capabilities || {})}`,
      `- Legal: ${s.legal_notes}`,
      `- Recommended Dealality use: ${s.recommended_use}`,
      ``
    );
  }

  lines.push(
    `## Hard rules`,
    ``,
    `- Never write owner / operator / developer / opening or renovation dates / Recent Momentum / Company Validated / Brand Verified / Brand Status`,
    `- Never write Brand Setup or Brand Explorer`,
    `- Google Places: never Rooms / Keys; storage policy required`,
    `- OpenStreetMap: ODbL policy required; never Rooms`,
    `- OTA consumer scrape: hard blocked — use licensed APIs only`,
    `- CoStar/STR: never product-facing without license`,
    `- Every Rooms write still needs Rooms Source URL / Type / Confidence / Reviewed Date / Evidence Tier`,
    ``
  );
  return lines.join("\n");
}

void attachRoomsProvenance;
void buildExternalProvenanceNotes;
void resolveExternalWriteGate;
