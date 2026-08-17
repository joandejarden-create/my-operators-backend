/**
 * Census Autopilot queue: property_name_cleanup
 * Official-source-only cleanup of marketing/tagline Property Name values.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  MAP_FIRST_PASS,
  loadActiveBrandUniverse,
  mapCensusBrand,
} from "./production-census-first-pass-enrichment.js";
import {
  PROPERTY_NAME_CLEANUP_EXTRACTOR_VERSION,
  classifyPropertyNameProblems,
  extractPropertyNamesFromOfficialHtml,
  selectBestPropertyNameHit,
  isMoreSpecificPropertyName,
  normalizeHotelName,
} from "./production-census-property-name-cleanup-extractor.js";

export const PROPERTY_NAME_CLEANUP_QUEUE_VERSION =
  "production-census-property-name-cleanup-queue-v1";

export const QUEUE_ID = "property_name_cleanup";

export const STATUS = Object.freeze({
  READY_FOR_APPLY: "production_census_property_name_cleanup_controlled_ready_for_apply",
  NEEDS_STEWARD: "production_census_property_name_cleanup_needs_steward_review",
  BLOCKED: "production_census_property_name_cleanup_blocked",
});

export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

export const MAP_NAME_CLEANUP = Object.freeze({
  propertyName: MAP_FIRST_PASS.propertyName,
  lastReviewed: MAP_FIRST_PASS.lastReviewed,
  enrichmentStatus: MAP_FIRST_PASS.enrichmentStatus,
  enrichmentPriority: MAP_FIRST_PASS.enrichmentPriority,
});

export const ALLOWED_NAME_CLEANUP_FIELDS = Object.freeze([
  MAP_NAME_CLEANUP.propertyName,
  MAP_NAME_CLEANUP.lastReviewed,
  MAP_NAME_CLEANUP.enrichmentStatus,
  MAP_NAME_CLEANUP.enrichmentPriority,
]);

const FETCH_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "accept-language": "en-US,en;q=0.9",
};

const READ_FIELDS = [
  MAP_FIRST_PASS.propertyName,
  MAP_FIRST_PASS.identityKey,
  MAP_FIRST_PASS.country,
  MAP_FIRST_PASS.city,
  MAP_FIRST_PASS.currentBrand,
  MAP_FIRST_PASS.brandSlug,
  MAP_FIRST_PASS.affiliationStatus,
  MAP_FIRST_PASS.sourceUrl,
  MAP_FIRST_PASS.officialUrl,
  MAP_FIRST_PASS.family,
  MAP_FIRST_PASS.humanReview,
  MAP_FIRST_PASS.dataEligible,
  MAP_FIRST_PASS.enrichmentStatus,
  MAP_FIRST_PASS.enrichmentPriority,
  MAP_FIRST_PASS.lastReviewed,
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

function isPropertyLevelUrl(url) {
  if (!url) return false;
  const s = String(url).toLowerCase();
  if (
    /sitemap|locations\/mexico\/[^/]*\/?$|\/mexico\/?$|choicehotels\.com\/(?:en-uk\/)?mexico(?:\/regional|\/?\?|$)|ihg\.com\/mexico$/i.test(
      s
    )
  ) {
    return false;
  }
  return (
    /hilton\.com\/en\/hotels\//i.test(s) ||
    /hoteldetail/i.test(s) ||
    /marriott\.com\/(?:en-us\/)?hotels\//i.test(s) ||
    /choicehotels\.com\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9]+/i.test(s) ||
    /ihg\.com\/[^/]+\/hotels\//i.test(s)
  );
}

function pickOfficialFetchUrl(fields) {
  const official = fields[MAP_FIRST_PASS.officialUrl];
  const source = fields[MAP_FIRST_PASS.sourceUrl];
  if (isPropertyLevelUrl(official)) return { url: official, kind: "official_property_url" };
  if (isPropertyLevelUrl(source)) return { url: source, kind: "source_url" };
  if (official) return { url: official, kind: "official_fallback_may_be_generic" };
  if (source) return { url: source, kind: "source_fallback_may_be_generic" };
  return { url: null, kind: "missing" };
}

function familyFromRecord(fields, identityKey) {
  const f = String(fields[MAP_FIRST_PASS.family] || "").trim();
  if (["Marriott", "IHG", "Hilton", "Choice"].includes(f)) return f;
  const id = String(identityKey || "");
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  return f || "Other";
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function fetchOfficialPage(url) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 25000);
  try {
    const res = await fetch(url, {
      headers: FETCH_HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /<title[^>]*>\s*access denied/i.test(text) ||
      /cf-challenge|attention required|akamai\s*block/i.test(text);
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
      length: text.length,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: err?.message || String(err),
      length: 0,
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Eligibility for property_name_cleanup queue.
 */
export function classifyPropertyNameCleanupEligibility(record, ctx = {}) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = Boolean(fields[MAP_FIRST_PASS.humanReview]);
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const family = familyFromRecord(fields, key);
  const fetchUrl = pickOfficialFetchUrl(fields);
  const currentName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const problems = classifyPropertyNameProblems(currentName);

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: currentName,
    brand: fields[MAP_FIRST_PASS.currentBrand],
    city: fields[MAP_FIRST_PASS.city],
    family,
    brand_mapping: brandMap,
    fetch_url: fetchUrl.url,
    fetch_url_kind: fetchUrl.kind,
    name_problems: problems,
    malformed: problems.malformed,
  };

  if (held) return { ...base, eligible: false, block_reason: "human_review_required" };
  if (brandUnconfirmed) return { ...base, eligible: false, block_reason: "brand_unconfirmed" };
  if (!brandMap.active) return { ...base, eligible: false, block_reason: "not_in_active_universe" };
  if (brandMap.classification === "uncertain") {
    return { ...base, eligible: false, block_reason: "uncertain_brand_mapping" };
  }
  if (!currentName) return { ...base, eligible: false, block_reason: "property_name_blank" };
  if (!problems.malformed) return { ...base, eligible: false, block_reason: "name_appears_valid_already" };
  if (!fetchUrl.url) return { ...base, eligible: false, block_reason: "missing_source_url" };
  if (!isPropertyLevelUrl(fetchUrl.url)) {
    return { ...base, eligible: false, block_reason: "generic_directory_url_not_property_page" };
  }
  return { ...base, eligible: true, block_reason: null };
}

function buildProposal(row, selection, sourceUrl, pageOk) {
  const confidence = selection.confidence;
  const proposed = selection.hit?.name || null;
  const canWriteHigh =
    confidence === "High" &&
    pageOk &&
    proposed &&
    isMoreSpecificPropertyName(row.property_name, proposed);

  /** @type {Record<string, unknown>} */
  const patch = {};
  if (canWriteHigh) {
    patch[MAP_NAME_CLEANUP.propertyName] = proposed;
    patch[MAP_NAME_CLEANUP.lastReviewed] = todayIsoDate();
    patch[MAP_NAME_CLEANUP.enrichmentStatus] = "Partial";
    patch[MAP_NAME_CLEANUP.enrichmentPriority] = "Medium";
  }

  return {
    ...row,
    queue: QUEUE_ID,
    action: canWriteHigh
      ? "propose_high_write"
      : confidence === "Medium"
        ? "medium_review"
        : confidence === "Hold"
          ? "hold"
          : "low_blocked",
    current_property_name: row.property_name,
    proposed_property_name: proposed,
    confidence,
    method: selection.hit?.method || null,
    source_url: sourceUrl,
    reason: selection.reason,
    alternatives: selection.alternatives || null,
    patch,
    patch_fields: Object.keys(patch),
    write_allowed_now: canWriteHigh,
    page_fetched: pageOk,
  };
}

/**
 * Dry-run property_name_cleanup queue.
 */
export async function runPropertyNameCleanupQueueDryRun(opts = {}) {
  const limit = opts.limit ?? 10000;
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const universe = loadActiveBrandUniverse();
  const censusRows = await listAllRecords(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    READ_FIELDS
  );

  const classified = censusRows.map((r) =>
    classifyPropertyNameCleanupEligibility(r, { universe })
  );
  const eligible = classified.filter((c) => c.eligible);
  const blocked = classified.filter((c) => !c.eligible);
  const work = eligible.slice(0, limit);
  const deferred = Math.max(0, eligible.length - work.length);

  const proposals = [];
  const fetchStats = { attempted: 0, ok: 0, blocked: 0, failed: 0 };

  for (const row of work) {
    fetchStats.attempted += 1;
    const page = await fetchOfficialPage(row.fetch_url);
    await sleep(250);

    if (!page.ok) {
      if (page.blocked) fetchStats.blocked += 1;
      else fetchStats.failed += 1;
      proposals.push({
        ...row,
        queue: QUEUE_ID,
        action: "blocked",
        blocked_reason: page.blocked ? "official_page_blocked" : `fetch_failed_${page.status || "err"}`,
        proposed_property_name: null,
        confidence: "Low",
        patch: {},
        write_allowed_now: false,
      });
      continue;
    }

    fetchStats.ok += 1;
    const extracted = extractPropertyNamesFromOfficialHtml(page.text, {
      url: page.url,
      brand: row.brand,
      city: row.city,
      propertyName: row.property_name,
    });
    const selection = selectBestPropertyNameHit(extracted.hits, {
      currentName: row.property_name,
      brand: row.brand,
      city: row.city,
    });

    // Fallback: brand + city synthesis only when marketing name and page clearly references brand+city
    if (
      (!selection.hit || selection.confidence === "Low") &&
      row.name_problems?.severity === "high" &&
      row.brand &&
      row.city
    ) {
      const synthetic = normalizeHotelName(`${row.brand} ${row.city}`);
      const pageMentions =
        page.text.toLowerCase().includes(String(row.brand).toLowerCase().split(/\s+/)[0]) &&
        page.text.toLowerCase().includes(String(row.city).toLowerCase());
      if (pageMentions && isMoreSpecificPropertyName(row.property_name, synthetic)) {
        proposals.push(
          buildProposal(
            row,
            {
              hit: { name: synthetic, method: "brand_city_from_official_page_context" },
              confidence: "High",
              reason: "marketing_phrase_replaced_with_brand_city_confirmed_on_official_page",
            },
            page.url,
            true
          )
        );
        continue;
      }
    }

    proposals.push(buildProposal(row, selection, page.url, true));
  }

  const high = proposals.filter((p) => p.action === "propose_high_write");
  const medium = proposals.filter((p) => p.action === "medium_review");
  const hold = proposals.filter((p) => p.action === "hold");
  const low = proposals.filter(
    (p) => p.action === "low_blocked" || p.action === "blocked" || p.action === "no_clean_name"
  );

  const blockReasons = {};
  for (const b of blocked) {
    blockReasons[b.block_reason] = (blockReasons[b.block_reason] || 0) + 1;
  }
  for (const p of proposals) {
    if (p.action === "propose_high_write") continue;
    const r = p.blocked_reason || p.reason || p.action;
    blockReasons[r] = (blockReasons[r] || 0) + 1;
  }

  const avidIncluded = proposals.filter(
    (p) =>
      /avid/i.test(String(p.brand || "")) ||
      /avid/i.test(String(p.property_name || "")) ||
      /avid/i.test(String(p.identity_key || ""))
  );

  let status = STATUS.BLOCKED;
  if (high.length > 0) status = STATUS.READY_FOR_APPLY;
  else if (medium.length > 0 || hold.length > 0) status = STATUS.NEEDS_STEWARD;
  else if (eligible.length === 0 && blocked.length > 0) status = STATUS.NEEDS_STEWARD;
  else status = STATUS.BLOCKED;

  const forAutopilot = Boolean(opts.forAutopilot);
  const idOut = (id) => (forAutopilot ? id : mask(id));

  const mapProposal = (p) => ({
    record_id: idOut(p.record_id),
    identity_key: p.identity_key,
    family: p.family,
    brand: p.brand,
    city: p.city,
    action: p.action,
    current_property_name: p.current_property_name || p.property_name,
    proposed_property_name: p.proposed_property_name ?? null,
    confidence: p.confidence || null,
    method: p.method || null,
    source_url: p.source_url || p.fetch_url || null,
    reason: p.reason || p.blocked_reason || null,
    name_problems: p.name_problems?.reasons || null,
    write_allowed_now: Boolean(p.write_allowed_now),
    patch_fields: p.patch_fields || [],
    ...(forAutopilot
      ? {
          patch: p.patch || {},
          queue: QUEUE_ID,
          current_fields: {},
        }
      : {}),
  });

  return {
    version: PROPERTY_NAME_CLEANUP_QUEUE_VERSION,
    extractor_version: PROPERTY_NAME_CLEANUP_EXTRACTOR_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    queue: QUEUE_ID,
    status,
    airtable_writes: false,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    allowed_fields: ALLOWED_NAME_CLEANUP_FIELDS,
    summary: {
      total_records_scanned: censusRows.length,
      records_flagged_malformed_eligible: eligible.length,
      records_blocked_prefilter: blocked.length,
      queue_limit: limit,
      processed: work.length,
      deferred_over_limit: deferred,
      official_sources_fetched: fetchStats.attempted,
      pages_ok: fetchStats.ok,
      pages_blocked: fetchStats.blocked,
      pages_failed: fetchStats.failed,
      high_confidence_proposals: high.length,
      medium_confidence_candidates: medium.length,
      hold_records: hold.length,
      low_confidence_blocked: low.length,
      exact_airtable_update_count_if_applied: high.length,
      avid_rows_in_proposals: avidIncluded.length,
      avid_high_writes: avidIncluded.filter((p) => p.action === "propose_high_write").length,
      block_reasons: blockReasons,
    },
    sample_high: high.slice(0, 15).map(mapProposal),
    sample_medium: medium.slice(0, 10).map(mapProposal),
    sample_hold: hold.slice(0, 10).map(mapProposal),
    proposals: proposals.map(mapProposal),
    next_step:
      status === STATUS.READY_FOR_APPLY
        ? "Founder review High proposals; apply approval-bundle-bound with Property Name + Last Reviewed only."
        : status === STATUS.NEEDS_STEWARD
          ? "Steward review Medium/Hold cases; no High writes ready."
          : "Investigate blocked status before apply.",
  };
}

export function renderPropertyNameCleanupMarkdown(report) {
  const s = report.summary || {};
  return `# Production Census Property Name Cleanup Queue

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Queue:** \`${report.queue}\`  
**Extractor:** ${report.extractor_version}  
**Airtable writes:** ${report.airtable_writes}

## Summary

| Metric | Value |
| --- | ---: |
| Records scanned | ${s.total_records_scanned ?? "—"} |
| Eligible (malformed + official URL) | ${s.records_flagged_malformed_eligible ?? "—"} |
| Processed | ${s.processed ?? "—"} |
| High proposals | ${s.high_confidence_proposals ?? "—"} |
| Medium review | ${s.medium_confidence_candidates ?? "—"} |
| Hold | ${s.hold_records ?? "—"} |
| Low/blocked | ${s.low_confidence_blocked ?? "—"} |
| Exact writes if applied | ${s.exact_airtable_update_count_if_applied ?? "—"} |
| Avid rows in proposals | ${s.avid_rows_in_proposals ?? "—"} |
| Avid High writes | ${s.avid_high_writes ?? "—"} |

## Sample High

\`\`\`json
${JSON.stringify(report.sample_high || [], null, 2)}
\`\`\`

## Allowed fields

${(report.allowed_fields || []).map((f) => `- ${f}`).join("\n")}

## Next

${report.next_step || ""}
`;
}
