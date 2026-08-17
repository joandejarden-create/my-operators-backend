/**
 * Lane 2 — post-draft integrity checks for true-incomplete brands.
 * Read-only; writes reports only.
 */
import path from "path";
import {
  LANE2_VERSION,
  MIN_CONTENT_PACK_ROWS,
  resolveFullBuildSlug,
  getFullBuildContent,
  resolveLane2BrandIdentity,
  refuseProtectedOrOutOfCohort,
  listPresentationRowsLight,
  writeLane2Reports,
  EXPECTED_PARENT_COMPANY_RE,
  LANE2_ROOT,
} from "./brand-explorer-lane2-common.js";
import { FULL_BUILD_SLUG_ALIASES } from "./brand-explorer-full-build-content.js";
import { shouldRenderFullBrandExplorerProfile } from "./brand-explorer-display-state.js";

export const REPORT_JSON = "brand-explorer-lane2-post-draft-integrity.json";
export const REPORT_MD = "brand-explorer-lane2-post-draft-integrity.md";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function checkSlugAliases(slug) {
  const canonical = resolveFullBuildSlug(slug);
  const aliases = Object.entries(FULL_BUILD_SLUG_ALIASES)
    .filter(([, v]) => v === canonical)
    .map(([k]) => k);
  const resolved = aliases.map((a) => ({
    alias: a,
    resolvesTo: resolveFullBuildSlug(a),
    ok: resolveFullBuildSlug(a) === canonical,
  }));
  return { canonical, aliases: resolved, pass: resolved.every((r) => r.ok) };
}

function findDuplicateRowFamilies(rows) {
  const map = new Map();
  for (const r of rows || []) {
    if (r.active === false) continue;
    if (/do not display|internal only/i.test(nz(r.externalDisplayStatus))) continue;
    const key = `${nz(r.slotKey)}::${nz(r.title)}`;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r.recordId);
  }
  return [...map.entries()]
    .filter(([, ids]) => ids.length > 1)
    .map(([key, recordIds]) => ({ key, count: recordIds.length, recordIds }));
}

async function evaluatePublicLock(brandSlug) {
  try {
    const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
    const res = {
      statusCode: 200,
      payload: null,
      setHeader() {},
      status(c) {
        this.statusCode = c;
        return this;
      },
      json(p) {
        this.payload = p;
      },
    };
    await getBrandLibraryBrandById({ query: { brandId: brandSlug }, headers: {} }, res);
    const brand = res.payload?.brand;
    if (!brand) throw new Error("brand_api_missing");
    const fullPublic = shouldRenderFullBrandExplorerProfile(brand) === true;
    const held = brand?.brandExplorerDisplayCompleteness?.legacyVisibilityUnlockHeld === true;
    return {
      checked: true,
      shouldRenderFullProfile: fullPublic,
      legacyVisibilityUnlockHeld: held,
      displayState: brand.brandExplorerDisplayState,
      founderPreviewOnly: !fullPublic,
      pass: !fullPublic,
      blockers: fullPublic ? ["unexpected_public_full_profile"] : [],
    };
  } catch (err) {
    return {
      checked: false,
      pass: null,
      note: "brand_api_unavailable",
      error: err.message,
    };
  }
}

export async function auditLane2PostDraftIntegrityBrand(brandSlug, { skipPublicLock = false } = {}) {
  const refuse = refuseProtectedOrOutOfCohort(brandSlug);
  if (refuse.refused) {
    return {
      brandSlug: resolveFullBuildSlug(brandSlug),
      refused: true,
      pass: false,
      blockers: [refuse.reason],
    };
  }

  const slug = refuse.brandSlug;
  const identity = resolveLane2BrandIdentity(slug);
  const checks = [];
  const blockers = [];

  if (!identity.recordId) {
    blockers.push("missing_brand_basics_record_id");
  }
  checks.push({
    id: "brand_basics_record",
    pass: Boolean(identity.recordId),
    recordId: identity.recordId,
    name: identity.name,
  });

  const aliasCheck = checkSlugAliases(slug);
  if (!aliasCheck.pass) blockers.push("slug_alias_resolution_failed");
  checks.push({ id: "slug_aliases", ...aliasCheck });

  let pack = null;
  let packModuleCount = 0;
  try {
    pack = getFullBuildContent(slug);
    packModuleCount = pack?.presentation?.length ?? 0;
  } catch (err) {
    blockers.push(`content_pack_load_error:${err.message}`);
  }
  if (!pack?.presentation?.length) blockers.push("content_pack_missing");
  else if (packModuleCount < MIN_CONTENT_PACK_ROWS) {
    blockers.push(`content_pack_thin_${packModuleCount}_lt_${MIN_CONTENT_PACK_ROWS}`);
  }
  checks.push({
    id: "content_pack",
    pass: packModuleCount >= MIN_CONTENT_PACK_ROWS,
    moduleCount: packModuleCount,
    minExpected: MIN_CONTENT_PACK_ROWS,
  });

  let presentationRows = [];
  let presentationFetchNote = null;
  try {
    const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
    presentationRows = fetch.rows;
    presentationFetchNote = fetch.skipped;
  } catch (err) {
    blockers.push(`presentation_fetch_error:${err.message}`);
  }

  const rowCount = presentationRows.length;
  if (presentationFetchNote === "missing_airtable_credentials") {
    checks.push({
      id: "presentation_rows",
      pass: null,
      rowCount,
      packSize: packModuleCount,
      note: "skipped_no_airtable_credentials",
    });
  } else if (!rowCount) {
    blockers.push("no_presentation_rows");
    checks.push({ id: "presentation_rows", pass: false, rowCount, packSize: packModuleCount });
  } else {
    const ratio = packModuleCount ? rowCount / packModuleCount : 0;
    const thinDraft = ratio < 0.7;
    if (thinDraft) blockers.push(`presentation_row_count_low_${rowCount}_vs_pack_${packModuleCount}`);
    checks.push({
      id: "presentation_rows",
      pass: !thinDraft,
      rowCount,
      packSize: packModuleCount,
      ratio: Number(ratio.toFixed(3)),
      thinDraft,
    });
  }

  const dupes = findDuplicateRowFamilies(presentationRows);
  if (dupes.length) blockers.push(`duplicate_row_families_${dupes.length}`);
  checks.push({ id: "duplicate_row_families", pass: dupes.length === 0, duplicates: dupes });

  const wrongBrandRows = presentationRows.filter(
    (r) => r.brandName && identity.name && nz(r.brandName) !== nz(identity.name)
  );
  if (wrongBrandRows.length) blockers.push(`orphan_wrong_brand_rows_${wrongBrandRows.length}`);
  checks.push({
    id: "orphan_brand_rows",
    pass: wrongBrandRows.length === 0,
    count: wrongBrandRows.length,
  });

  const parentRe = EXPECTED_PARENT_COMPANY_RE[slug];
  const parentOk = parentRe ? parentRe.test(nz(identity.parentCompany)) : false;
  if (!parentOk) blockers.push("parent_company_mismatch");
  checks.push({
    id: "parent_company",
    pass: parentOk,
    expectedPattern: String(parentRe),
    actual: identity.parentCompany,
  });

  const publicLock = skipPublicLock
    ? { checked: false, pass: null, note: "skipped_by_flag" }
    : await evaluatePublicLock(slug);
  if (publicLock.checked && publicLock.pass === false) {
    blockers.push(...(publicLock.blockers || []));
  }
  checks.push({ id: "public_profile_lock", ...publicLock });

  return {
    brandSlug: slug,
    reportSlug: identity.reportSlug,
    refused: false,
    pass: blockers.length === 0,
    blockers,
    checks,
    identity: {
      recordId: identity.recordId,
      name: identity.name,
      parentCompany: identity.parentCompany,
    },
    summary: {
      packModuleCount,
      presentationRowCount: rowCount,
      duplicateFamilies: dupes.length,
    },
  };
}

export async function runLane2PostDraftIntegrity({
  brands = [],
  reportsDir = path.join(LANE2_ROOT, "reports"),
  skipPublicLock = false,
} = {}) {
  const brandResults = [];
  for (const raw of brands) {
    brandResults.push(await auditLane2PostDraftIntegrityBrand(raw, { skipPublicLock }));
  }

  const result = {
    version: LANE2_VERSION,
    lane: "post-draft-integrity",
    generatedAt: new Date().toISOString(),
    brands: brandResults.map((b) => b.brandSlug),
    brandResults,
    summary: {
      brandCount: brandResults.length,
      passCount: brandResults.filter((b) => b.pass).length,
      failCount: brandResults.filter((b) => !b.pass && !b.refused).length,
      refusedCount: brandResults.filter((b) => b.refused).length,
    },
  };

  const md = [
    `# Lane 2 — Post-draft integrity`,
    ``,
    `- Generated: ${result.generatedAt}`,
    `- Brands: ${result.brands.join(", ") || "—"}`,
    `- Pass: **${result.summary.passCount}/${result.summary.brandCount}**`,
    ``,
    `## Per brand`,
    ``,
    ...brandResults.map(
      (b) =>
        `- **${b.brandSlug}**: ${b.pass ? "PASS" : "FAIL"}${b.blockers?.length ? ` — ${b.blockers.join("; ")}` : ""}`
    ),
    ``,
  ];

  writeLane2Reports({
    jsonPath: path.join(reportsDir, REPORT_JSON),
    mdPath: path.join(reportsDir, REPORT_MD),
    json: result,
    mdLines: md,
  });

  return result;
}
