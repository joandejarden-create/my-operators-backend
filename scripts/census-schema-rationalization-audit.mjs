#!/usr/bin/env node
/**
 * Hotel Property Census schema rationalization audit (READ-ONLY).
 *
 * Does NOT delete/rename Airtable fields or write census data.
 *
 *   node scripts/census-schema-rationalization-audit.mjs
 *   node scripts/census-schema-rationalization-audit.mjs --skip-population
 *   node scripts/census-schema-rationalization-audit.mjs --skip-deps
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { MAP_CENSUS_FIELDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/census-schema-rationalization-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/census-schema-rationalization-v1"
);

const TABLE_ID = "tbl9aY5ijiuIzzWam";
const TABLE_NAME = "Hotel Property Census";

const DISPOSITIONS = [
  "KEEP_CORE",
  "KEEP_SUPPORTING",
  "REPURPOSE",
  "MOVE_TO_HOTEL_INTELLIGENCE",
  "MOVE_TO_EVIDENCE_STORE",
  "MOVE_TO_EXTERNAL_IDS",
  "MOVE_TO_OWNER_INTELLIGENCE",
  "CONSOLIDATE",
  "DEPRECATE",
  "DELETE_CANDIDATE",
  "UNKNOWN_REQUIRES_REVIEW",
];

function parseArgs(argv) {
  const out = { skipPopulation: false, skipDeps: false };
  for (const a of argv.slice(2)) {
    if (a === "--skip-population") out.skipPopulation = true;
    if (a === "--skip-deps") out.skipDeps = true;
  }
  return out;
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(file, data) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function blank(v) {
  if (v == null) return true;
  if (Array.isArray(v)) return v.length === 0;
  if (typeof v === "object") return Object.keys(v).length === 0;
  return String(v).trim() === "";
}
function pct(n, d) {
  if (!d) return null;
  return Number(((100 * n) / d).toFixed(2));
}

async function fetchLiveSchema(token, baseId) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const j = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(j).slice(0, 400));
  const table = (j.tables || []).find(
    (t) => t.id === TABLE_ID || t.name === TABLE_NAME
  );
  if (!table) throw new Error("Hotel Property Census table not found");
  return {
    tableId: table.id,
    tableName: table.name,
    fields: (table.fields || []).map((f) => ({
      id: f.id,
      name: f.name,
      type: f.type,
      description: f.description || null,
      options: f.options || null,
    })),
  };
}

async function computePopulation(token, baseId, fieldNames) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  const stats = Object.fromEntries(
    fieldNames.map((n) => [
      n,
      {
        populated: 0,
        nullish: 0,
        unique: new Set(),
        sample_values: [],
      },
    ])
  );
  let total = 0;
  await base(TABLE_ID)
    .select({ pageSize: 100 })
    .eachPage((page, next) => {
      for (const rec of page) {
        total += 1;
        const f = rec.fields || {};
        for (const name of fieldNames) {
          const v = f[name];
          if (blank(v)) {
            stats[name].nullish += 1;
          } else {
            stats[name].populated += 1;
            const key =
              typeof v === "object" ? JSON.stringify(v).slice(0, 200) : String(v);
            if (stats[name].unique.size < 5000) stats[name].unique.add(key);
            if (stats[name].sample_values.length < 3) {
              stats[name].sample_values.push(
                typeof v === "string" ? v.slice(0, 120) : v
              );
            }
          }
        }
      }
      if (total % 2000 === 0) console.error(`population scanned ${total}…`);
      next();
    });

  const out = {};
  for (const name of fieldNames) {
    const s = stats[name];
    out[name] = {
      POPULATED_COUNT: s.populated,
      NULL_COUNT: s.nullish,
      COMPLETENESS_PERCENT: pct(s.populated, total),
      UNIQUE_VALUE_COUNT: s.unique.size,
      SAMPLE_VALUES: s.sample_values,
    };
  }
  return { total, byField: out };
}

/**
 * Single-pass dependency scan using one alternation regex for field IDs
 * and longest-first name matching per file (skipped for very large files).
 */
function scanRepoDependencies(fields) {
  // High-signal roots only (exclude bulky reports/data/worker clones).
  const ROOTS = ["lib", "api", "scripts", "docs", "public", "airtable"].map(
    (d) => path.join(ROOT, d)
  );
  const TEXT_EXT = new Set([
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".json",
    ".md",
    ".html",
    ".css",
    ".yml",
    ".yaml",
    ".txt",
  ]);

  const byKey = new Map();
  const idToName = new Map();
  for (const f of fields) {
    idToName.set(f.id, f.name);
    byKey.set(f.name, {
      FIELD_NAME: f.name,
      FIELD_ID: f.id,
      by_name: { ok: true, fileHits: 0, matchSum: 0, files: [] },
      by_id: { ok: true, fileHits: 0, matchSum: 0, files: [] },
    });
  }

  const nameList = fields
    .map((f) => f.name)
    .sort((a, b) => b.length - a.length);
  const idRegex = new RegExp(
    `(${fields.map((f) => f.id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})`,
    "g"
  );

  let filesScanned = 0;

  function bump(bucket, rel, count) {
    bucket.matchSum += count;
    bucket.fileHits += 1;
    if (bucket.files.length < 12) {
      bucket.files.push({ path: rel, matches: count });
    }
  }

  function walk(dir) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const ent of entries) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (ent.name === "node_modules" || ent.name === ".git") continue;
        walk(full);
        continue;
      }
      if (!ent.isFile()) continue;
      const ext = path.extname(ent.name).toLowerCase();
      if (!TEXT_EXT.has(ext)) continue;
      let text;
      try {
        const st = fs.statSync(full);
        if (st.size > 2_000_000) continue;
        text = fs.readFileSync(full, "utf8");
      } catch {
        continue;
      }
      filesScanned += 1;
      const rel = path.relative(ROOT, full);

      const idCounts = new Map();
      for (const m of text.matchAll(idRegex)) {
        idCounts.set(m[1], (idCounts.get(m[1]) || 0) + 1);
      }
      for (const [id, count] of idCounts) {
        const name = idToName.get(id);
        if (!name) continue;
        bump(byKey.get(name).by_id, rel, count);
      }

      for (const name of nameList) {
        if (!text.includes(name)) continue;
        let idx = 0;
        let count = 0;
        while ((idx = text.indexOf(name, idx)) !== -1) {
          count += 1;
          idx += name.length;
        }
        if (count > 0) bump(byKey.get(name).by_name, rel, count);
      }
    }
  }

  for (const root of ROOTS) walk(root);
  console.error(`dependency files scanned: ${filesScanned}`);
  return [...byKey.values()];
}

function classifyBucketRefs(files) {
  const paths = (files || []).map((f) => f.path.replace(/\\/g, "/"));
  const has = (re) => paths.some((p) => re.test(p));
  return {
    REFERENCED_BY_CODE: has(/\/(lib|api|public|scripts)\//) && !has(/\/reports\//),
    REFERENCED_BY_TESTS: has(/test|\/scripts\/test-/i),
    REFERENCED_BY_DOCS: has(/\/docs\//),
    REFERENCED_BY_REPORTS: has(/\/reports\//),
    REFERENCED_BY_BRAND_EXPLORER: has(/brand-explorer/i),
    REFERENCED_BY_HOTEL_INTELLIGENCE: has(/hotel-intelligence|hotel-census|research-engine/i),
    REFERENCED_BY_UI: has(/\/public\//),
    REFERENCED_BY_MCP: has(/mcp|hotel.intelligence.*mcp|map_hotel/i),
    REFERENCED_BY_MAPPINGS: has(/map_hotel_intelligence_fields|field-bindings|census-map/i),
  };
}

/**
 * Primary disposition heuristics grounded in Dealality architecture.
 * Manual overrides applied after auto classify for known clusters.
 */
function autoDisposition(field, pop, refs) {
  const name = field.name;
  const type = field.type;
  const populated = pop?.POPULATED_COUNT ?? 0;
  const completeness = pop?.COMPLETENESS_PERCENT ?? 0;
  const codeRefs = refs?.byName?.matchSum || 0;
  const idRefs = refs?.byId?.matchSum || 0;
  const totalRefs = codeRefs + idRefs;
  const mapped = Object.values(MAP_CENSUS_FIELDS).includes(name);

  // Linked record tables — supporting structure, keep
  if (
    [
      "Hotel Property Brand Affiliations",
      "Hotel Property Source Evidence",
      "Hotel Property Steward Review",
    ].includes(name)
  ) {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Linked-record bridge to related census tables",
      rationale: "Structural link fields — do not delete",
    };
  }

  // Core identity / location / property / affiliation / contact
  const coreExact = new Set([
    "Property Name",
    "Canonical Property Name",
    "Property Identity Key",
    "Country",
    "State / Region",
    "City",
    "Address",
    "Latitude",
    "Longitude",
    "Market",
    "Submarket",
    "Rooms / Keys",
    "Official Property URL",
    "Phone",
    "Current Brand",
    "Brand Family",
    "Affiliation Status",
    "Property Type",
    "Hotel Class / Segment",
    "Production Use Status",
    "Identity Confidence",
  ]);
  if (coreExact.has(name)) {
    return {
      disposition: "KEEP_CORE",
      purpose: "Canonical hotel master attribute",
      rationale: mapped
        ? "In MAP_CENSUS_FIELDS / core identity-product surface"
        : "Durable hotel fact for census master",
    };
  }

  // Rooms provenance suite — keep supporting (authoritative rooms + provenance)
  if (/^Rooms (Source|Confidence|Notes|Evidence|Reviewed)/.test(name) || name === "Rooms Source URL") {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Authoritative rooms provenance companions",
      rationale: "Required for rooms confidence model; candidates stay in evidence store",
    };
  }

  // Coordinate / address / phone provenance — keep supporting (lean companions)
  if (
    /^(Address Confidence|Address Source URL|Coordinate |Geocode |Phone Confidence|Phone Source|Phone Review|Phone Notes|Phone Reviewed)/.test(
      name
    )
  ) {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Field-level provenance companion for core contact/geo",
      rationale: "Prefer keeping companion columns over scattering provider_* fields; long-term may thin to evidence store",
    };
  }

  // HBX provider-specific — move to external IDs / evidence; deprecate on census
  if (name.startsWith("HBX ")) {
    if (["HBX Hotel Code", "HBX Chain Code"].includes(name)) {
      return {
        disposition: "MOVE_TO_EXTERNAL_IDS",
        purpose: "Hotelbeds provider identifier",
        rationale: "Provider ID belongs in external-ids registry; deprecate census columns after migration",
      };
    }
    return {
      disposition: "MOVE_TO_EVIDENCE_STORE",
      purpose: "Hotelbeds observation / linkage metadata",
      rationale: "Provider-specific enrichment metadata should not live as permanent census columns",
    };
  }

  // Owner / operator / developer — owner intelligence
  if (
    /^(Owner |Developer |Operator |Management Model|Ownership Review|Operator Review|Possible Operator)/.test(
      name
    )
  ) {
    return {
      disposition: "MOVE_TO_OWNER_INTELLIGENCE",
      purpose: "Ownership / operator relationship intelligence",
      rationale: "Belongs outside lean hotel master; preserve via Owner Intelligence + evidence",
    };
  }

  // Opportunity / GTM flags
  if (/^Possible (Soft-Brand|Brand Conversion|Owner Outreach|Financing|Dealality)/.test(name)) {
    return {
      disposition: "MOVE_TO_HOTEL_INTELLIGENCE",
      purpose: "Opportunity / outreach scoring flag",
      rationale: "Dynamic product intelligence — not durable census identity",
    };
  }

  // Long text / AI / description bloat
  if (
    [
      "Hotel Description - Source Text",
      "Hotel Description - AI Summary",
      "Short Property Summary",
      "Property Positioning",
      "Amenities - Source Text",
      "Building / Asset Notes",
      "Notes for Steward",
      "Candidate Brand Text",
    ].includes(name)
  ) {
    return {
      disposition: "MOVE_TO_HOTEL_INTELLIGENCE",
      purpose: "Narrative / research content",
      rationale: "Long-text Airtable burden; store as HI content or evidence blobs",
    };
  }

  // Amenity / product flags — HI product profile (structured tags may stay supporting if used by Radar)
  if (
    / Flag$/.test(name) ||
    ["Amenities - Structured Tags", "Asset Context", "Market / Submarket"].includes(name)
  ) {
    if (name === "Amenities - Structured Tags") {
      return {
        disposition: "KEEP_SUPPORTING",
        purpose: "Normalized amenity tags for product profile",
        rationale: "Useful durable-ish product facts; raw amenity dumps stay HI/evidence",
      };
    }
    if (name === "Market / Submarket") {
      return {
        disposition: "CONSOLIDATE",
        purpose: "Duplicate geography concept vs Market + Submarket",
        rationale: "Consolidate into Market and Submarket fields",
        consolidate_into: ["Market", "Submarket"],
      };
    }
    return {
      disposition: "MOVE_TO_HOTEL_INTELLIGENCE",
      purpose: "Product / amenity observational flag",
      rationale: "Prefer Profile Pack / HI product attributes over many census boolean columns",
    };
  }

  // Radar / public display — supporting operational
  if (/^Radar |^Public /.test(name)) {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Radar / public census operational gate",
      rationale: "Product display controls — keep until Radar reads move off census",
    };
  }

  // Shell insert / candidate brand pipeline
  if (/^Shell |^Candidate Brand|^Brand Validation Status|^Discovery Source|^Source Candidate|^Candidate Source/.test(name)) {
    if (completeness < 1 && totalRefs === 0) {
      return {
        disposition: "DELETE_CANDIDATE",
        purpose: "Ingest / shell pipeline metadata",
        rationale: "Near-empty + no repo refs — verify Airtable views/automations before delete",
      };
    }
    return {
      disposition: "DEPRECATE",
      purpose: "Ingest / candidate pipeline metadata",
      rationale: "Operational history; stop new writes after pipeline migration; retain until backup",
    };
  }

  // Continent / Sub-Continent — often redundant with Country
  if (name === "Continent" || name === "Sub-Continent") {
    return {
      disposition: "CONSOLIDATE",
      purpose: "Derived geography hierarchy",
      rationale: "Prefer derive from Country config; deprecate stored duplicates after validation",
      consolidate_into: ["Country"],
    };
  }

  // Family / Source Family — supporting lineage
  if (name === "Family / Source Family") {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Census shell/family lineage",
      rationale: "Used in production census identity model",
    };
  }

  // Source URL/Type/Confidence/Discovery — supporting provenance (may thin later)
  if (
    [
      "Source URL",
      "Source Type",
      "Source Confidence",
      "Discovery Date",
      "VIC Freeze Hash",
      "Data Eligible",
      "Data Confidence Tier",
      "Relationship Confidence",
      "Source Confidence",
    ].includes(name)
  ) {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Record-level provenance / eligibility",
      rationale: "Needed for steward + production use; long-term evidence store may absorb some",
    };
  }

  // Enrichment / review workflow
  if (
    /^(Enrichment |Human Review|Review Status|Last Reviewed|Next Review|Steward Review|Brand Confidence|Affiliation |Prior Brand|Future Opening|Brand Explorer Slug|Independent )/.test(
      name
    )
  ) {
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Workflow / affiliation / review state",
      rationale: "Operational census fields — keep lean set; do not expand",
    };
  }

  // Opening / renovation
  if (/^Opening |^Renovation /.test(name)) {
    if (name === "Opening Date") {
      return {
        disposition: "KEEP_CORE",
        purpose: "Property lifecycle fact",
        rationale: "Durable hotel attribute when known",
      };
    }
    return {
      disposition: "KEEP_SUPPORTING",
      purpose: "Lifecycle provenance / status",
      rationale: "Keep companions for Opening Date; move raw URLs to evidence long-term",
    };
  }

  // Empty + no refs → delete candidate
  if (populated === 0 && totalRefs === 0) {
    return {
      disposition: "DELETE_CANDIDATE",
      purpose: "Unknown / unused",
      rationale: "Zero population and zero repo references — still verify Airtable views/automations",
    };
  }

  if (populated === 0 && totalRefs > 0) {
    return {
      disposition: "DEPRECATE",
      purpose: "Referenced but unpopulated",
      rationale: "Code/docs reference exists — migrate references before delete",
    };
  }

  if (completeness < 0.5 && totalRefs === 0 && type === "multilineText") {
    return {
      disposition: "UNKNOWN_REQUIRES_REVIEW",
      purpose: "Sparse long-text field",
      rationale: "Low population; confirm Airtable-only usage before delete",
    };
  }

  return {
    disposition: "UNKNOWN_REQUIRES_REVIEW",
    purpose: "Unclassified — needs founder/steward review",
    rationale: `populated=${populated} refs=${totalRefs} type=${type}`,
  };
}

function idealLeanSchema() {
  return {
    principle:
      "Lean canonical hotel master — WHAT hotel is this + durable core attributes only",
    groups: {
      IDENTITY: [
        "Property Identity Key",
        "Canonical Property Name",
        "Property Name",
        "Production Use Status",
        "Identity Confidence",
      ],
      LOCATION: [
        "Address",
        "City",
        "State / Region",
        "Country",
        "Market",
        "Submarket",
        "Latitude",
        "Longitude",
      ],
      PROPERTY: [
        "Rooms / Keys",
        "Property Type",
        "Hotel Class / Segment",
        "Opening Date",
      ],
      AFFILIATION: [
        "Current Brand",
        "Brand Family",
        "Affiliation Status",
      ],
      CONTACT: ["Official Property URL", "Phone"],
      PROVENANCE_MIN: [
        "Source URL",
        "Source Type",
        "Source Confidence",
        "Discovery Date",
        "Rooms Confidence",
        "Rooms Source Type",
        "Address Confidence",
        "Coordinate Confidence",
        "Coordinate Source Type",
        "Phone Confidence",
        "Phone Source Type",
      ],
      WORKFLOW_MIN: [
        "Enrichment Status",
        "Review Status",
        "Human Review Required",
        "Last Reviewed Date",
        "Data Eligible",
        "Family / Source Family",
      ],
      LINKS: [
        "Hotel Property Brand Affiliations",
        "Hotel Property Source Evidence",
        "Hotel Property Steward Review",
      ],
      OPTIONAL_SUPPORTING_IF_STILL_NEEDED: [
        "Brand Explorer Slug if mapped",
        "Radar Display Status",
        "Public Census Eligibility",
        "Amenities - Structured Tags",
        "VIC Freeze Hash",
      ],
    },
    explicitly_out_of_census: [
      "Tripadvisor / GIATA / Google provider observation columns",
      "Owner / operator / developer entity graphs",
      "Opportunity flags",
      "Long narrative descriptions / AI summaries",
      "Historical snapshots / rankings / ratings",
      "HBX detail columns (keep codes in external-ids)",
      "Shell-insert batch metadata (archive then delete)",
      "Duplicate Market / Submarket combo field",
      "Continent / Sub-Continent if derivable",
    ],
    target_field_count_estimate: {
      core_plus_min_provenance_workflow_links: 48,
      with_optional_supporting: 55,
      stretch_ceiling: 65,
    },
  };
}

function tripadvisorRemap(dispositionsByName) {
  const fields = [
    {
      ta_concept: "Official Property URL",
      tier: "A",
      destination_after: "Official Property URL",
      disposition_of_destination: dispositionsByName["Official Property URL"],
      needs_new_column: false,
    },
    {
      ta_concept: "Phone",
      tier: "A",
      destination_after: "Phone (+ Phone Confidence/Source companions)",
      disposition_of_destination: dispositionsByName.Phone,
      needs_new_column: false,
    },
    {
      ta_concept: "Address",
      tier: "A",
      destination_after: "Address",
      disposition_of_destination: dispositionsByName.Address,
      needs_new_column: false,
    },
    {
      ta_concept: "Latitude",
      tier: "A",
      destination_after: "Latitude",
      disposition_of_destination: dispositionsByName.Latitude,
      needs_new_column: false,
    },
    {
      ta_concept: "Longitude",
      tier: "A",
      destination_after: "Longitude",
      disposition_of_destination: dispositionsByName.Longitude,
      needs_new_column: false,
    },
    {
      ta_concept: "Hotel Class / Segment",
      tier: "B",
      destination_after: "Hotel Class / Segment",
      disposition_of_destination: dispositionsByName["Hotel Class / Segment"],
      needs_new_column: false,
    },
    {
      ta_concept: "Property Type",
      tier: "B",
      destination_after: "Property Type",
      disposition_of_destination: dispositionsByName["Property Type"],
      needs_new_column: false,
    },
    {
      ta_concept: "City",
      tier: "B",
      destination_after: "City",
      disposition_of_destination: dispositionsByName.City,
      needs_new_column: false,
    },
    {
      ta_concept: "State / Region",
      tier: "B",
      destination_after: "State / Region",
      disposition_of_destination: dispositionsByName["State / Region"],
      needs_new_column: false,
    },
    {
      ta_concept: "Amenities tags",
      tier: "B",
      destination_after: "Amenities - Structured Tags (or HI if moved)",
      disposition_of_destination:
        dispositionsByName["Amenities - Structured Tags"],
      needs_new_column: false,
    },
    {
      ta_concept: "Email",
      tier: "B",
      destination_after:
        "DO NOT add Email census column yet — HI/evidence until lean schema approved; only then Phone-parity Email suite OR skip",
      disposition_of_destination: null,
      needs_new_column: false,
      note: "Prefer NOT adding during rationalization; store in evidence/HI",
    },
    {
      ta_concept: "Rooms / Keys",
      tier: "C",
      destination_after:
        "Authoritative Rooms / Keys unchanged; Tripadvisor rooms → evidence/candidate only",
      disposition_of_destination: dispositionsByName["Rooms / Keys"],
      needs_new_column: false,
    },
    {
      ta_concept: "Profile Pack dynamic fields",
      tier: "HI",
      destination_after: "Hotel Intelligence Profile Pack + evidence store",
      needs_new_column: false,
    },
  ];
  return fields;
}

async function main() {
  const args = parseArgs(process.argv);
  ensureDir(OUT_DIR);
  ensureDir(DATA_DIR);

  const token = (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!token || !baseId) throw new Error("Airtable credentials missing");

  console.error("Fetching live schema…");
  const schema = await fetchLiveSchema(token, baseId);
  writeJson(path.join(DATA_DIR, "schema-before.json"), schema);
  writeJson(path.join(OUT_DIR, "01-current-field-inventory.json"), {
    tableId: schema.tableId,
    tableName: schema.tableName,
    fieldCount: schema.fields.length,
    fields: schema.fields.map((f) => ({
      FIELD_NAME: f.name,
      FIELD_ID: f.id,
      FIELD_TYPE: f.type,
    })),
  });

  let population = { total: null, byField: {} };
  if (!args.skipPopulation) {
    console.error("Computing population across all records (read-only)…");
    population = await computePopulation(
      token,
      baseId,
      schema.fields.map((f) => f.name)
    );
    writeJson(path.join(DATA_DIR, "population-by-field.json"), population);
    writeJson(path.join(OUT_DIR, "02-population-completeness.json"), population);
  } else {
    const candidates = [
      path.join(DATA_DIR, "population-by-field.json"),
      path.join(OUT_DIR, "02-population-completeness.json"),
    ];
    for (const c of candidates) {
      if (fs.existsSync(c)) {
        population = JSON.parse(fs.readFileSync(c, "utf8"));
        writeJson(path.join(DATA_DIR, "population-by-field.json"), population);
        break;
      }
    }
  }

  const depAudit = [];
  if (!args.skipDeps) {
    console.error(
      `Dependency scan (single-pass) for ${schema.fields.length} fields…`
    );
    const scanned = scanRepoDependencies(schema.fields);
    for (const row of scanned) {
      const combinedFiles = [
        ...(row.by_name.files || []),
        ...(row.by_id.files || []),
      ];
      const buckets = classifyBucketRefs(combinedFiles);
      depAudit.push({
        FIELD_NAME: row.FIELD_NAME,
        FIELD_ID: row.FIELD_ID,
        by_name: row.by_name,
        by_id: row.by_id,
        ...buckets,
        REFERENCE_CONFIDENCE:
          row.by_name.matchSum + row.by_id.matchSum > 0
            ? "repo_search_hits"
            : "repo_search_zero_hits_airtable_views_unknown",
        AIRTABLE_VIEWS_AUTOMATIONS: "UNKNOWN_NOT_QUERIED",
        AIRTABLE_FORMULAS: "UNKNOWN_CHECK_SCHEMA_OPTIONS",
      });
    }
    writeJson(path.join(OUT_DIR, "03-dependency-audit.json"), {
      fields: depAudit,
    });
  } else if (fs.existsSync(path.join(OUT_DIR, "03-dependency-audit.json"))) {
    const j = JSON.parse(
      fs.readFileSync(path.join(OUT_DIR, "03-dependency-audit.json"), "utf8")
    );
    depAudit.push(...(j.fields || []));
  }

  const depByName = Object.fromEntries(depAudit.map((d) => [d.FIELD_NAME, d]));

  const inventory = [];
  for (const f of schema.fields) {
    const pop = population.byField?.[f.name] || null;
    const refs = depByName[f.name] || null;
    const classified = autoDisposition(f, pop, {
      byName: refs?.by_name,
      byId: refs?.by_id,
    });
    inventory.push({
      FIELD_NAME: f.name,
      FIELD_ID: f.id,
      FIELD_TYPE: f.type,
      PURPOSE: classified.purpose,
      POPULATED_COUNT: pop?.POPULATED_COUNT ?? null,
      NULL_COUNT: pop?.NULL_COUNT ?? null,
      COMPLETENESS_PERCENT: pop?.COMPLETENESS_PERCENT ?? null,
      UNIQUE_VALUE_COUNT: pop?.UNIQUE_VALUE_COUNT ?? null,
      REFERENCED_BY_CODE: refs?.REFERENCED_BY_CODE ?? null,
      REFERENCED_BY_BRAND_EXPLORER: refs?.REFERENCED_BY_BRAND_EXPLORER ?? null,
      REFERENCED_BY_HOTEL_INTELLIGENCE:
        refs?.REFERENCED_BY_HOTEL_INTELLIGENCE ?? null,
      REFERENCED_BY_UI: refs?.REFERENCED_BY_UI ?? null,
      REFERENCED_BY_MCP: refs?.REFERENCED_BY_MCP ?? null,
      REFERENCED_BY_TESTS: refs?.REFERENCED_BY_TESTS ?? null,
      REPO_MATCH_SUM:
        (refs?.by_name?.matchSum || 0) + (refs?.by_id?.matchSum || 0),
      REFERENCE_CONFIDENCE: refs?.REFERENCE_CONFIDENCE || "unknown",
      AIRTABLE_VIEWS_AUTOMATIONS: "UNKNOWN_NOT_QUERIED",
      DISPOSITION: classified.disposition,
      RATIONALE: classified.rationale,
      CONSOLIDATE_INTO: classified.consolidate_into || null,
      IN_MAP_CENSUS_FIELDS: Object.values(MAP_CENSUS_FIELDS).includes(f.name),
    });
  }

  writeJson(path.join(OUT_DIR, "04-field-disposition-matrix.json"), {
    dispositions_allowed: DISPOSITIONS,
    fields: inventory,
  });

  const counts = Object.fromEntries(DISPOSITIONS.map((d) => [d, 0]));
  for (const row of inventory) counts[row.DISPOSITION] += 1;

  const lean = idealLeanSchema();
  writeJson(path.join(OUT_DIR, "10-ideal-lean-census-schema.json"), lean);

  const leanNames = new Set(
    Object.values(lean.groups).flat()
  );
  const targetFields = [...leanNames];
  const targetCount =
    lean.target_field_count_estimate.with_optional_supporting;

  const dispositionsByName = Object.fromEntries(
    inventory.map((r) => [r.FIELD_NAME, r.DISPOSITION])
  );
  const taMap = tripadvisorRemap(dispositionsByName);
  writeJson(path.join(OUT_DIR, "16-tripadvisor-destination-mapping.json"), {
    tripadvisor_writes_paused: true,
    mapping: taMap,
  });

  // Buckets
  const bucketA = [];
  const bucketB = [];
  const bucketC = [];
  for (const row of inventory) {
    const entry = {
      FIELD_NAME: row.FIELD_NAME,
      FIELD_ID: row.FIELD_ID,
      DISPOSITION: row.DISPOSITION,
      POPULATED_COUNT: row.POPULATED_COUNT,
      REPO_MATCH_SUM: row.REPO_MATCH_SUM,
      RATIONALE: row.RATIONALE,
      CONSOLIDATE_INTO: row.CONSOLIDATE_INTO,
    };
    if (
      row.DISPOSITION === "DELETE_CANDIDATE" &&
      (row.POPULATED_COUNT || 0) === 0 &&
      (row.REPO_MATCH_SUM || 0) === 0
    ) {
      bucketA.push({
        ...entry,
        action: "SAFE_DELETE_CANDIDATE_AFTER_AIRTABLE_VIEW_CHECK",
        risk: "LOW",
        note: "Repo-clean and empty — still verify Airtable views/automations/interfaces before delete",
      });
    } else if (
      ["MOVE_TO_HOTEL_INTELLIGENCE", "MOVE_TO_EVIDENCE_STORE", "MOVE_TO_EXTERNAL_IDS", "MOVE_TO_OWNER_INTELLIGENCE", "CONSOLIDATE", "DEPRECATE", "REPURPOSE"].includes(
        row.DISPOSITION
      )
    ) {
      bucketB.push({
        ...entry,
        action: "MIGRATE_THEN_REMOVE_OR_RENAME",
        risk: (row.POPULATED_COUNT || 0) > 0 || (row.REPO_MATCH_SUM || 0) > 0 ? "HIGH" : "MEDIUM",
        data_migration_required: (row.POPULATED_COUNT || 0) > 0,
      });
    } else {
      bucketC.push({
        ...entry,
        action: "KEEP_OR_REVIEW",
        risk: row.DISPOSITION === "UNKNOWN_REQUIRES_REVIEW" ? "UNKNOWN" : "LOW",
      });
    }
  }

  writeJson(path.join(OUT_DIR, "12-execution-buckets.json"), {
    BUCKET_A_SAFE_NOW: bucketA,
    BUCKET_B_MIGRATE_THEN_REMOVE: bucketB,
    BUCKET_C_KEEP_OR_REVIEW: bucketC,
  });

  const manifest = inventory.map((row) => ({
    OLD_FIELD: row.FIELD_NAME,
    FIELD_ID: row.FIELD_ID,
    FIELD_TYPE: row.FIELD_TYPE,
    ACTION: row.DISPOSITION,
    NEW_FIELD_OR_DESTINATION:
      row.CONSOLIDATE_INTO?.join(" + ") ||
      (row.DISPOSITION.startsWith("MOVE_")
        ? row.DISPOSITION.replace("MOVE_TO_", "").toLowerCase()
        : row.DISPOSITION === "KEEP_CORE" || row.DISPOSITION === "KEEP_SUPPORTING"
          ? row.FIELD_NAME
          : null),
    POPULATED_RECORDS: row.POPULATED_COUNT,
    DEPENDENCIES_REPO_MATCH_SUM: row.REPO_MATCH_SUM,
    DEPENDENCIES_AIRTABLE: "UNKNOWN_NOT_QUERIED",
    DATA_MIGRATION_REQUIRED:
      (row.POPULATED_COUNT || 0) > 0 &&
      !["KEEP_CORE", "KEEP_SUPPORTING"].includes(row.DISPOSITION),
    RISK:
      row.DISPOSITION === "DELETE_CANDIDATE" && (row.POPULATED_COUNT || 0) === 0
        ? "LOW"
        : ["KEEP_CORE", "KEEP_SUPPORTING"].includes(row.DISPOSITION)
          ? "LOW"
          : "HIGH",
    RATIONALE: row.RATIONALE,
  }));
  writeJson(path.join(OUT_DIR, "15-schema-rationalization-manifest.json"), {
    status: "AWAITING_FOUNDER_REVIEW",
    production_schema_changes_executed: false,
    production_data_writes: 0,
    tripadvisor_tier_a_paused: true,
    rows: manifest,
  });

  // Duplicates / provider / bloat summaries
  const duplicates = inventory.filter((r) => r.DISPOSITION === "CONSOLIDATE");
  const provider = inventory.filter(
    (r) =>
      r.FIELD_NAME.startsWith("HBX ") ||
      /tripadvisor|giata|google|cvent/i.test(r.FIELD_NAME)
  );
  const moveOut = inventory.filter((r) =>
    String(r.DISPOSITION).startsWith("MOVE_")
  );
  const deleteCand = inventory.filter(
    (r) => r.DISPOSITION === "DELETE_CANDIDATE"
  );
  const repurpose = inventory.filter((r) => r.DISPOSITION === "REPURPOSE");

  writeJson(path.join(OUT_DIR, "05-duplicates-redundant.json"), { fields: duplicates });
  writeJson(path.join(OUT_DIR, "06-provider-specific-fields.json"), {
    fields: provider,
  });
  writeJson(path.join(OUT_DIR, "07-repurpose-candidates.json"), {
    fields: repurpose,
    note: "Auto-classifier found few pure REPURPOSE slots; prefer KEEP existing Official Property URL / Hotel Class rather than inventing renames. Email should NOT be added via repurpose until lean schema approved.",
  });
  writeJson(path.join(OUT_DIR, "08-move-outside-census.json"), { fields: moveOut });
  writeJson(path.join(OUT_DIR, "09-delete-candidates.json"), {
    fields: deleteCand,
  });

  const keepCore = counts.KEEP_CORE;
  const keepSupporting = counts.KEEP_SUPPORTING;
  const moveOutCount =
    counts.MOVE_TO_HOTEL_INTELLIGENCE +
    counts.MOVE_TO_EVIDENCE_STORE +
    counts.MOVE_TO_EXTERNAL_IDS +
    counts.MOVE_TO_OWNER_INTELLIGENCE;
  const reductionIfExecuted =
    schema.fields.length - targetCount;
  const reductionPct = pct(reductionIfExecuted, schema.fields.length);

  const summary = {
    CENSUS_SCHEMA_RATIONALIZATION_V1_COMPLETE: true,
    CURRENT_FIELDS: schema.fields.length,
    TARGET_FIELDS: targetCount,
    TARGET_FIELDS_STRETCH_CEILING:
      lean.target_field_count_estimate.stretch_ceiling,
    KEEP_CORE: keepCore,
    KEEP_SUPPORTING: keepSupporting,
    REPURPOSE: counts.REPURPOSE,
    CONSOLIDATE: counts.CONSOLIDATE,
    MOVE_OUT: moveOutCount,
    DEPRECATE: counts.DEPRECATE,
    DELETE_CANDIDATES: counts.DELETE_CANDIDATE,
    UNKNOWN_REQUIRES_REVIEW: counts.UNKNOWN_REQUIRES_REVIEW,
    SAFE_DELETE_OR_REPURPOSE_NOW: bucketA.length,
    MIGRATE_THEN_REMOVE: bucketB.length,
    KEEP_OR_REVIEW: bucketC.length,
    FIELD_REDUCTION_COUNT: reductionIfExecuted,
    FIELD_REDUCTION_PERCENT: reductionPct,
    TRIPADVISOR_FIELDS_USING_EXISTING_COLUMNS: taMap.filter(
      (t) => t.needs_new_column === false && t.tier !== "HI"
    ).length,
    TRIPADVISOR_FIELDS_REQUIRING_NEW_COLUMNS: taMap.filter(
      (t) => t.needs_new_column === true
    ).length,
    BACKUP_STATUS: "SCHEMA_BEFORE_SNAPSHOT_SAVED_DATA_EXPORT_PENDING_PER_AFFECTED_FIELD",
    DEPENDENCY_AUDIT_STATUS:
      "REPO_SEARCH_COMPLETE_AIRTABLE_VIEWS_AUTOMATIONS_UNKNOWN",
    PRODUCTION_SCHEMA_CHANGES_EXECUTED: 0,
    PRODUCTION_DATA_WRITES: 0,
    RECOMMENDED_EXECUTION_WAVE:
      "Wave 0: founder review manifest. Wave 1: Airtable view/automation check on Bucket A empties. Wave 2: export backups + migrate Bucket B (HBX→external-ids, owner→OI, narratives→HI). Wave 3: consolidate Market/Submarket + Continent. Wave 4: thin provenance companions optionally. Wave 5: resume Tripadvisor Tier A into retained columns only.",
    disposition_counts: counts,
    census_total_records: population.total,
  };
  writeJson(path.join(OUT_DIR, "00-summary.json"), summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
