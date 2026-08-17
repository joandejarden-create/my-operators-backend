/**
 * Brand source auto-resolver (reusable).
 *
 * Removes the pipeline friction where allowlistedSourceIds had to be hand-copied
 * into batch config after Source Library rows were registered. Given a brand
 * record id and its Source Library rows, this helper:
 *   1. Filters to approved, linked, company-controlled sources.
 *   2. Classifies each source by role (local PDF / consumer / development /
 *      press / PR-opening / image / other).
 *   3. Selects extraction-eligible sources automatically after stewardship.
 *   4. De-duplicates so a re-run does not double-register or double-extract.
 *   5. Generates a strict allowlist from live approved sources (no manual IDs).
 *
 * Pure functions accept already-fetched sources; `fetchAndResolveApprovedBrandSources`
 * is an async convenience that reads from the Source Library.
 *
 * This module does NOT write to Airtable, approve facts, or publish governance.
 */
import { listPartnerSources } from "./airtable-source.js";

export const RESOLVER_VERSION = "1";

/** Generic source roles (company-materials taxonomy). */
export const SOURCE_ROLE = {
  LOCAL_PDF: "local_pdf",
  CONSUMER_PAGE: "consumer_page",
  DEVELOPMENT_PAGE: "development_page",
  PRESS_PAGE: "press_page",
  PR_OPENING: "pr_opening",
  IMAGE_ASSET: "image_asset",
  OTHER: "other",
};

/**
 * Roles that carry extractable company-materials text. Development pages are
 * excluded by default (frequently JS-shell / provenance-only); image assets
 * carry no extractable brand-fact text.
 */
export const EXTRACTABLE_ROLES = new Set([
  SOURCE_ROLE.LOCAL_PDF,
  SOURCE_ROLE.CONSUMER_PAGE,
  SOURCE_ROLE.PRESS_PAGE,
  SOURCE_ROLE.PR_OPENING,
]);

/** Choice pipeline role names → generic roles (backward compatibility). */
const CHOICE_ROLE_ALIASES = {
  mini_batch_primary_pdf: SOURCE_ROLE.LOCAL_PDF,
  local_development_pdf: SOURCE_ROLE.LOCAL_PDF,
  consumer_page: SOURCE_ROLE.CONSUMER_PAGE,
  development_provenance: SOURCE_ROLE.DEVELOPMENT_PAGE,
  press_kit: SOURCE_ROLE.PRESS_PAGE,
};

const IMAGE_EXT_RE = /\.(png|jpe?g|gif|webp|svg|avif|tiff?)$/i;
const PDF_DECK_EXT_RE = /\.(pdf|pptx?|key|docx?)$/i;
const PR_OPENING_RE =
  /\b(opens|opening|opened|debut|unveil|announc\w*|celebrat\w*|now open|milestone|expansion|grand opening)\b/i;

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function lc(v) {
  return nz(v).toLowerCase();
}

/** Source Library rows use `id`; stewardship sourceRows use `sourceId`. */
function getSourceId(source) {
  return nz(source.id) || nz(source.sourceId) || null;
}

/**
 * Is the source company-controlled (brand/operator/official web/FDD) rather
 * than a third-party page? `companyDomains` is a list of substrings/RegExp that
 * mark the brand's official web properties.
 */
export function isCompanyControlledSource(source, { companyDomains = [] } = {}) {
  const origin = nz(source.sourceOrigin);
  if (
    origin === "Brand Provided" ||
    origin === "Operator Provided" ||
    origin === "Internal Upload" ||
    origin === "FDD Library" ||
    origin === "Press Release"
  ) {
    return true;
  }
  if (nz(source.localFilePath)) return true;

  const url = lc(source.sourceUrl);
  if (url) {
    for (const pat of companyDomains) {
      if (pat instanceof RegExp) {
        if (pat.test(url)) return true;
      } else if (url.includes(lc(pat))) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Classify a Source Library row into a generic role. If the row already carries
 * a pipeline `role` (e.g. Choice stewardship rows), that is honored first.
 */
export function classifyBrandSourceRole(source, { companyDomains = [] } = {}) {
  const existing = nz(source.role);
  if (existing && CHOICE_ROLE_ALIASES[existing]) return CHOICE_ROLE_ALIASES[existing];
  if (existing && Object.values(SOURCE_ROLE).includes(existing)) return existing;

  const type = nz(source.sourceType);
  const url = lc(source.sourceUrl);
  const local = lc(source.localFilePath);
  const title = lc(source.sourceTitle);
  const fileType = lc(source.fileType);

  const looksImage =
    IMAGE_EXT_RE.test(local) ||
    IMAGE_EXT_RE.test(url) ||
    /logo|image|photo|media asset|gallery|icon/.test(type) ||
    /image|logo/.test(fileType);
  if (looksImage) return SOURCE_ROLE.IMAGE_ASSET;

  const looksLocalDoc = Boolean(local) && (PDF_DECK_EXT_RE.test(local) || fileType === "pdf");
  if (
    looksLocalDoc &&
    (type === "Development Brochure" ||
      type === "Prototype / Layout" ||
      type === "Operator Deck" ||
      type === "FDD" ||
      type === "Case Study" ||
      type === "Other" ||
      !type)
  ) {
    return SOURCE_ROLE.LOCAL_PDF;
  }

  const isPress =
    type === "Press Release" ||
    /\/(press|media|news|newsroom)\b/.test(url) ||
    /media\.|news\.|press\./.test(url);
  if (isPress) {
    return PR_OPENING_RE.test(title) || PR_OPENING_RE.test(url)
      ? SOURCE_ROLE.PR_OPENING
      : SOURCE_ROLE.PRESS_PAGE;
  }

  const isDevelopment =
    type === "Development Page" ||
    /develop(ment)?\./.test(url) ||
    /\/development\b/.test(url) ||
    /development|franchis|for-owners|for-developers/.test(url);
  if (isDevelopment) return SOURCE_ROLE.DEVELOPMENT_PAGE;

  if (type === "Brand Page" || type === "Website Capture" || url) {
    return SOURCE_ROLE.CONSUMER_PAGE;
  }

  if (looksLocalDoc) return SOURCE_ROLE.LOCAL_PDF;
  return SOURCE_ROLE.OTHER;
}

/** Linked to the target brand record. */
export function isLinkedToBrand(source, recordId) {
  return nz(source.brandId) === nz(recordId);
}

/** Approved for Explorer use (stewardship complete) and linked to the brand. */
export function isApprovedLinkedSource(source, recordId) {
  if (recordId && !isLinkedToBrand(source, recordId)) return false;
  const status = nz(source.status);
  const explorerApproved = nz(source.approvedForExplorerUse) === "Yes";
  return explorerApproved || status === "Approved";
}

/** Approved for extraction AND in an extractable role. */
export function isExtractionEligible(source, role) {
  if (nz(source.approvedForExtraction) !== "Yes") return false;
  return EXTRACTABLE_ROLES.has(role);
}

/**
 * Filter to approved, linked sources; optionally require company-controlled.
 */
export function resolveApprovedBrandSources(
  sources = [],
  { recordId = null, companyDomains = [], requireCompanyControlled = false } = {}
) {
  return (sources || []).filter((s) => {
    if (!isApprovedLinkedSource(s, recordId)) return false;
    if (requireCompanyControlled && !isCompanyControlledSource(s, { companyDomains })) return false;
    return true;
  });
}

function dedupeKey(source, role) {
  const url = lc(source.sourceUrl);
  const local = lc(source.localFilePath).replace(/\\/g, "/");
  return `${role}::${local || url || getSourceId(source)}`;
}

/**
 * Build a strict, de-duplicated allowlist from approved live sources.
 *
 * @param {object[]} sources — Source Library rows (or stewardship sourceRows).
 * @param {object} opts
 * @param {string} [opts.recordId] — brand record id (link check).
 * @param {(string|RegExp)[]} [opts.companyDomains] — official web patterns.
 * @param {boolean} [opts.requireApproved=true] — restrict to approved sources.
 * @returns {{
 *   allowlistedSourceIds: string[],
 *   extractionEligibleIds: string[],
 *   primaryPdfSourceId: string|null,
 *   consumerSourceId: string|null,
 *   pressSourceId: string|null,
 *   developmentSourceId: string|null,
 *   byRole: Record<string,string[]>,
 *   duplicatesSkipped: {id:string, role:string, key:string}[],
 *   resolved: {id:string, role:string, extractionEligible:boolean, companyControlled:boolean}[]
 * }}
 */
export function buildBrandSourceAllowlist(sources = [], opts = {}) {
  const { recordId = null, companyDomains = [], requireApproved = true } = opts;
  const pool = requireApproved
    ? resolveApprovedBrandSources(sources, { recordId, companyDomains })
    : (sources || []).filter((s) => (recordId ? isLinkedToBrand(s, recordId) : true));

  const byRole = {};
  const resolved = [];
  const duplicatesSkipped = [];
  const seen = new Set();

  for (const source of pool) {
    const id = getSourceId(source);
    if (!id) continue;
    const role = classifyBrandSourceRole(source, { companyDomains });
    const key = dedupeKey(source, role);
    if (seen.has(key)) {
      duplicatesSkipped.push({ id, role, key });
      continue;
    }
    seen.add(key);
    const extractionEligible = isExtractionEligible(source, role);
    const companyControlled = isCompanyControlledSource(source, { companyDomains });
    resolved.push({ id, role, extractionEligible, companyControlled });
    (byRole[role] ||= []).push(id);
  }

  const firstOf = (role) => (byRole[role]?.length ? byRole[role][0] : null);
  const primaryPdfSourceId = firstOf(SOURCE_ROLE.LOCAL_PDF);
  const consumerSourceId = firstOf(SOURCE_ROLE.CONSUMER_PAGE);
  const pressSourceId = firstOf(SOURCE_ROLE.PRESS_PAGE) || firstOf(SOURCE_ROLE.PR_OPENING);
  const developmentSourceId = firstOf(SOURCE_ROLE.DEVELOPMENT_PAGE);

  // Strict allowlist: primary local PDF + consumer + press (parity with the
  // proven Choice pipeline). Additional extractable sources are surfaced via
  // extractionEligibleIds for callers that want a wider net.
  const allowlistedSourceIds = [primaryPdfSourceId, consumerSourceId, pressSourceId].filter(Boolean);

  const extractionEligibleIds = resolved
    .filter((r) => r.extractionEligible)
    .map((r) => r.id);

  return {
    allowlistedSourceIds: [...new Set(allowlistedSourceIds)],
    extractionEligibleIds: [...new Set(extractionEligibleIds)],
    primaryPdfSourceId,
    consumerSourceId,
    pressSourceId,
    developmentSourceId,
    byRole,
    duplicatesSkipped,
    resolved,
  };
}

/**
 * Adapter for batch pipelines: resolve an extraction config from already-fetched
 * stewardship source rows (which may carry pre-classified pipeline roles), merged
 * onto a manifest base. Prefer a non-empty manifest allowlist so shipped batches
 * stay byte-for-byte stable; auto-resolve only when the manifest is empty.
 */
export function resolveBatchExtractConfig({ base = {}, sourceRows = [], companyDomains = [] } = {}) {
  if (Array.isArray(base.allowlistedSourceIds) && base.allowlistedSourceIds.length) {
    return { ...base, autoResolved: false };
  }
  const resolvedAllowlist = buildBrandSourceAllowlist(sourceRows, {
    // Stewardship rows are already brand-scoped and approval-gated; do not
    // re-filter by brand link (rows may not carry brandId) or approval flags.
    recordId: null,
    companyDomains,
    requireApproved: false,
  });
  return {
    ...base,
    allowlistedSourceIds: resolvedAllowlist.allowlistedSourceIds,
    primaryPdfSourceId: resolvedAllowlist.primaryPdfSourceId || base.primaryPdfSourceId || null,
    consumerSourceId: resolvedAllowlist.consumerSourceId || base.consumerSourceId || null,
    pressSourceId: resolvedAllowlist.pressSourceId || base.pressSourceId || null,
    autoResolved: true,
    autoResolvedByRole: resolvedAllowlist.byRole,
    autoResolvedDuplicatesSkipped: resolvedAllowlist.duplicatesSkipped,
  };
}

/**
 * Async convenience — fetch a brand's Source Library rows and resolve the
 * allowlist directly. Intended for future (non-Choice) pipelines so they never
 * need hand-maintained source-id lists.
 */
export async function fetchAndResolveApprovedBrandSources({
  recordId,
  companyDomains = [],
  requireCompanyControlled = false,
} = {}) {
  if (!recordId) throw new Error("recordId is required");
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId: recordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);

  const approved = resolveApprovedBrandSources(all, {
    recordId,
    companyDomains,
    requireCompanyControlled,
  });
  const allowlist = buildBrandSourceAllowlist(approved, {
    recordId,
    companyDomains,
    requireApproved: false,
  });

  return {
    recordId,
    totalSources: all.length,
    approvedSourceCount: approved.length,
    ...allowlist,
    sources: all,
  };
}
