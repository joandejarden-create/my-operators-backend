/**
 * Preflight Active Brand Setup discovery insert bundle (91).
 * Read-only vs Airtable except listing Census for duplicate checks.
 * Does not modify Brand Setup / Brand Explorer.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadDiscoveryInsertApprovalBundle,
  rededupeInsertsAgainstCensus,
} from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import {
  INSERT_FORBIDDEN_FIELDS,
  MATCH_CLASS,
  sanitizeInsertFields,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  buildActiveBrandSetupControlList,
  HELD_EXCLUDED_SLUGS,
} from "../lib/research-engine-v2/census-autopilot-active-brand-scope.js";
import { MAP_FIRST_PASS } from "../lib/research-engine-v2/production-census-first-pass-enrichment.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const RUN_DIR =
  "reports/research-engine-v2/autopilot/2026-08-05T23-52-53_CALA-source-discovery";
const BUNDLE_PATH = path.join(ROOT, RUN_DIR, "approval-bundle.json");

function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    return u.protocol === "https:" || u.protocol === "http:";
  } catch {
    return false;
  }
}

const PARENT_BY_SLUG = {
  "aloft-hotels": "Marriott",
  "autograph-collection": "Marriott",
  "ac-hotels-by-marriott": "Marriott",
  "city-express-by-marriott": "Marriott",
  "courtyard-by-marriott": "Marriott",
  "marriott-hotels": "Marriott",
  "moxy-hotels": "Marriott",
  "residence-inn-by-marriott": "Marriott",
  sheraton: "Marriott",
  "springhill-suites-by-marriott": "Marriott",
  studiores: "Marriott",
  "towneplace-suites-by-marriott": "Marriott",
  "tribute-portfolio": "Marriott",
  westin: "Marriott",
  "canopy-by-hilton": "Hilton",
  "curio-collection": "Hilton",
  "doubletree-by-hilton": "Hilton",
  "hampton-by-hilton": "Hilton",
  "hilton-garden-inn": "Hilton",
  "hilton-hotels-and-resorts": "Hilton",
  "home2-suites-by-hilton": "Hilton",
  "homewood-suites-by-hilton": "Hilton",
  "motto-by-hilton": "Hilton",
  "spark-by-hilton": "Hilton",
  "tempo-by-hilton": "Hilton",
  "tru-by-hilton": "Hilton",
  "tapestry-collection-by-hilton": "Hilton",
  ascend: "Choice",
  "comfort-inn-suites": "Choice",
  "country-inn-suites": "Choice",
  "quality-inn": "Choice",
  radisson: "Choice",
  "radisson-blu": "Choice",
  "radisson-red": "Choice",
  "radisson-individuals-by-choice": "Choice",
  "suburban-studios": "Choice",
  "avid-hotels": "IHG",
  "even-hotels": "IHG",
  "holiday-inn-express": "IHG",
  "hotel-indigo": "IHG",
  kimpton: "IHG",
  "voco-hotels": "IHG",
  "handwritten-collection": "IHG",
  "vignette-collection": "IHG",
};

const CLASS = {
  PASS: "pass_insert_preflight",
  DUP: "duplicate_risk",
  STEWARD: "steward_review_required",
  SOURCE: "source_insufficient",
  CONFLICT: "blocked_identity_conflict",
  WRONG: "blocked_wrong_census_target",
};

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeMd(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

async function listCensus(baseId, token, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(
        TABLE_IDS["Hotel Property Census"]
      )}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

function inferParent(row, controlByBrand) {
  const brand = String(row.brand || row.fields?.["Current Brand"] || "").trim();
  const family = String(
    row.source_family || row.fields?.["Brand Family"] || row.fields?.["Family / Source Family"] || ""
  ).trim();
  const hit = controlByBrand.get(norm(brand));
  if (hit?.parent_company) return { parent: hit.parent_company, how: "brand_setup_control_list" };
  if (hit?.brand_slug && PARENT_BY_SLUG[hit.brand_slug]) {
    return { parent: PARENT_BY_SLUG[hit.brand_slug], how: "slug_inference", slug: hit.brand_slug };
  }
  for (const [slug, parent] of Object.entries(PARENT_BY_SLUG)) {
    if (norm(brand).includes(slug.replace(/-/g, " ")) || norm(brand).includes(parent.toLowerCase())) {
      return { parent, how: "brand_name_heuristic", slug };
    }
  }
  if (/marriott|sheraton|westin|aloft|autograph|tribute|moxy|courtyard|residence inn|springhill|towneplace|studiores|city express/i.test(brand)) {
    return { parent: "Marriott", how: "brand_name_heuristic" };
  }
  if (/hilton|hampton|doubletree|canopy|curio|tapestry|homewood|home2|motto|tempo|tru|spark|garden inn/i.test(brand)) {
    return { parent: "Hilton", how: "brand_name_heuristic" };
  }
  if (/choice|ascend|comfort|quality|radisson|suburban|country inn/i.test(brand)) {
    return { parent: "Choice", how: "brand_name_heuristic" };
  }
  if (/ihg|holiday inn|indigo|kimpton|voco|avid|even|handwritten|vignette/i.test(brand)) {
    return { parent: "IHG", how: "brand_name_heuristic" };
  }
  if (family && ["Marriott", "Hilton", "Choice", "IHG"].includes(family)) {
    return { parent: family, how: "source_family" };
  }
  return { parent: null, how: "unresolved" };
}

async function main() {
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId,
  });
  if (!writeTarget.ok) {
    writeJson(path.join(ROOT, "reports/research-engine-v2/production-census-active-brand-insert-preflight.json"), {
      final_status: "production_census_active_brand_insert_apply_blocked",
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
    });
    process.exit(1);
  }

  const loaded = loadDiscoveryInsertApprovalBundle(BUNDLE_PATH);
  if (!loaded.ok) {
    console.error(loaded);
    process.exit(1);
  }

  const control = buildActiveBrandSetupControlList({ region: "CALA", includeHeldProbe: true });
  const controlByBrand = new Map();
  for (const b of control.brands || []) {
    controlByBrand.set(norm(b.brand_name), b);
    if (b.brand_slug) controlByBrand.set(norm(b.brand_slug.replace(/-/g, " ")), b);
    for (const a of b.census_matching_aliases || []) controlByBrand.set(norm(a), b);
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    throw new Error("missing_airtable_credentials");
  }
  const censusRecords = await listCensus(bases.target_base_id, token, [
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.propertyName,
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.country,
    MAP_FIRST_PASS.city,
    MAP_FIRST_PASS.address,
    MAP_FIRST_PASS.sourceUrl,
    MAP_FIRST_PASS.officialUrl,
  ]);

  const rededupe = rededupeInsertsAgainstCensus(loaded.inserts, censusRecords);
  const writableKeys = new Set(rededupe.writable.map((r) => r.identity_key));
  const blockedMap = new Map(rededupe.blocked.map((r) => [r.identity_key, r]));
  const stewardMap = new Map(rededupe.steward.map((r) => [r.identity_key, r]));

  const results = [];
  const pass = [];
  const inferredParents = [];

  for (const row of loaded.inserts) {
    const fields = row.fields || {};
    const brand = String(row.brand || fields["Current Brand"] || "").trim();
    const name = String(row.property_name || fields["Property Name"] || "").trim();
    const city = String(fields.City || row.discovery?.city || "").trim();
    const country = String(fields.Country || row.discovery?.country || "").trim();
    const sourceUrl = String(fields["Source URL"] || "").trim();
    const officialUrl = String(fields["Official Property URL"] || row.discovery?.official_property_url || "").trim();
    const conf = String(row.confidence || fields["Identity Confidence"] || "").trim();
    const parentInf = inferParent(row, controlByBrand);
    if (parentInf.how !== "brand_setup_control_list" && parentInf.parent) {
      inferredParents.push({
        identity_key: row.identity_key,
        brand,
        inferred_parent: parentInf.parent,
        how: parentInf.how,
      });
    }

    const controlHit = controlByBrand.get(norm(brand));
    const held =
      HELD_EXCLUDED_SLUGS.includes(controlHit?.brand_slug) ||
      /brand.?unconfirmed|four.?points.?flex|radisson.?collection/i.test(brand);

    let classification = CLASS.PASS;
    const reasons = [];

    if (held) {
      classification = CLASS.STEWARD;
      reasons.push("held_or_brand_unconfirmed");
    }
    if (!name || !brand || !country) {
      classification = CLASS.SOURCE;
      reasons.push("missing_name_brand_or_country");
    }
    if (conf !== "High") {
      classification = CLASS.STEWARD;
      reasons.push("identity_not_high");
    }
    if (!officialUrl && !sourceUrl) {
      classification = CLASS.SOURCE;
      reasons.push("missing_official_source_url");
    }
    if (officialUrl && !isSafeHttpUrl(officialUrl)) {
      classification = CLASS.SOURCE;
      reasons.push("unsafe_official_url");
    }
    if (!parentInf.parent) {
      classification = CLASS.STEWARD;
      reasons.push("parent_company_unresolved");
    }
    if (!controlHit && !parentInf.parent) {
      classification = CLASS.STEWARD;
      reasons.push("brand_not_in_active_control_list");
    }

    for (const k of Object.keys(fields)) {
      if (INSERT_FORBIDDEN_FIELDS.includes(k)) {
        classification = CLASS.CONFLICT;
        reasons.push(`forbidden_field:${k}`);
      }
    }

    const sanitized = sanitizeInsertFields(fields);
    if (sanitized.fatal?.length) {
      classification = CLASS.CONFLICT;
      reasons.push(...sanitized.fatal.map((f) => `fatal:${f}`));
    }

    if (blockedMap.has(row.identity_key)) {
      classification = CLASS.DUP;
      reasons.push("duplicate_on_live_census_rededupe");
    } else if (stewardMap.has(row.identity_key)) {
      classification = CLASS.STEWARD;
      reasons.push("probable_match_or_steward_on_rededupe");
    } else if (!writableKeys.has(row.identity_key) && classification === CLASS.PASS) {
      classification = CLASS.STEWARD;
      reasons.push("not_in_writable_rededupe_set");
    }

    // Soft-brand / marketing name check
    if (/member of|a member of|collection property|unnamed|tbd|n\/a/i.test(name)) {
      classification = CLASS.STEWARD;
      reasons.push("non_property_specific_name");
    }

    const item = {
      identity_key: row.identity_key,
      property_name: name,
      brand,
      city,
      country,
      parent_company: parentInf.parent,
      parent_inference: parentInf.how,
      brand_setup_slug: controlHit?.brand_slug || null,
      active_live: Boolean(controlHit),
      official_property_id: row.official_property_id || null,
      official_property_url: officialUrl || null,
      source_url: sourceUrl || null,
      confidence: conf,
      classification,
      reasons,
      production_use_status: fields["Production Use Status"] || null,
    };
    results.push(item);
    if (classification === CLASS.PASS) pass.push(row);
  }

  const counts = Object.fromEntries(
    Object.values(CLASS).map((c) => [c, results.filter((r) => r.classification === c).length])
  );

  const filteredBundle = {
    ...loaded.bundle,
    preflight_at: new Date().toISOString(),
    preflight_version: "active-brand-insert-preflight-v1",
    records_proposed_for_insert: pass.length,
    proposed_inserts: pass,
    preflight_counts: counts,
    source_bundle: RUN_DIR + "/approval-bundle.json",
    note: "Filtered to pass_insert_preflight only after live Census rededupe",
  };

  const filteredPath = path.join(ROOT, RUN_DIR, "approval-bundle.preflight-pass.json");
  writeJson(filteredPath, filteredBundle);

  const report = {
    run_type: "production_census_active_brand_insert_preflight",
    generated_at: new Date().toISOString(),
    selected_run_dir: RUN_DIR,
    scope: "active-brand-setup",
    queue: "source_discovery",
    region: "CALA",
    strategy: "fastest-safe",
    production_writes: false,
    airtable_writes: false,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    production_target: productionHotelPropertyCensus,
    write_target_ok: true,
    census_rows_indexed: censusRecords.length,
    bundle_inserts: loaded.inserts.length,
    counts,
    pass_count: pass.length,
    inferred_parents_count: inferredParents.length,
    inferred_parents_sample: inferredParents.slice(0, 20),
    filtered_approval_bundle: path.relative(ROOT, filteredPath).replace(/\\/g, "/"),
    results,
    recommended_apply:
      pass.length > 0
        ? "apply filtered preflight-pass bundle with founder confirms"
        : "blocked_no_pass_candidates",
  };

  writeJson(
    path.join(ROOT, "reports/research-engine-v2/production-census-active-brand-insert-preflight.json"),
    report
  );
  writeMd(
    path.join(ROOT, "reports/research-engine-v2/production-census-active-brand-insert-preflight.md"),
    [
      `# Active Brand Setup Insert Preflight`,
      ``,
      `Selected run: \`${RUN_DIR}\``,
      ``,
      `- Bundle inserts: **${loaded.inserts.length}**`,
      `- Live Census indexed: **${censusRecords.length}**`,
      `- Pass insert preflight: **${pass.length}**`,
      `- Duplicate risk: **${counts[CLASS.DUP]}**`,
      `- Steward review: **${counts[CLASS.STEWARD]}**`,
      `- Source insufficient: **${counts[CLASS.SOURCE]}**`,
      `- Identity conflict: **${counts[CLASS.CONFLICT]}**`,
      `- Parent inferences logged: **${inferredParents.length}** (Brand Setup not modified)`,
      ``,
      `Filtered bundle: \`${path.relative(ROOT, filteredPath).replace(/\\/g, "/")}\``,
      ``,
      `## Classification counts`,
      ``,
      ...Object.entries(counts).map(([k, v]) => `- ${k}: ${v}`),
      ``,
    ].join("\n")
  );

  writeJson(path.join(ROOT, RUN_DIR, "insert-preflight.json"), report);
  writeJson(
    path.join(ROOT, RUN_DIR, "duplicate-risk.json"),
    results.filter((r) => r.classification === CLASS.DUP)
  );
  writeJson(
    path.join(ROOT, RUN_DIR, "steward-review-queue.json"),
    results.filter((r) => r.classification === CLASS.STEWARD || r.classification === CLASS.SOURCE)
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        selected_run_dir: RUN_DIR,
        pass: pass.length,
        counts,
        filtered_bundle: path.relative(ROOT, filteredPath).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
