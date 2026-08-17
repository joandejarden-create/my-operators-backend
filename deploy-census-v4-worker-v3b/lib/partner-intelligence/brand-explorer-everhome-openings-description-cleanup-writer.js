/**
 * Brand Explorer Everhome Opening Description Full Cleanup v32E-R1.
 *
 * Everhome-only: rewrites footprint.openings teaser/situation copy that still
 * contains source-metadata language. Preserves chips, labels, images, URLs, visibility.
 *
 * @see docs/data-intelligence/brand-explorer-everhome-openings-description-cleanup-writer-v32E-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { parseFootprintOpeningLocation } from "./brand-explorer-openings-ui-quarantine-governance.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { TARGET_BRAND as EVERHOME_TARGET } from "./brand-explorer-everhome-openings-momentum-rebuild-writer.js";

export const WRITER_VERSION = "v32E-R1";
export const REPORT_JSON_NAME = "brand-explorer-everhome-openings-description-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-everhome-openings-description-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-everhome-openings-description-cleanup-writer-v32E-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32E-R1-everhome-openings-description-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_SOURCE_URLS = "--confirm-no-source-url-changes";
export const APPLY_FLAG_NO_OPENING_LABELS = "--confirm-no-opening-label-changes";
export const APPLY_FLAG_NO_VISIBILITY = "--confirm-no-visibility-changes";
export const APPLY_FLAG_EVERHOME_ONLY = "--confirm-everhome-only";

export const TARGET_BRAND = EVERHOME_TARGET;

const OPENINGS_SLOT = "footprint.openings";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const BLOCKED_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Attachments",
  "Scenario Image",
  "External Display Status",
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "Source URL",
  "Source Page URL",
]);

export const SOURCE_METADATA_PATTERNS = [
  { id: "listed_choicehotels", re: /listed on choicehotels\.com/i },
  { id: "featured_development_site", re: /featured on choice hotels'? development site/i },
  { id: "featured_chd_list", re: /featured on the chd everhome recent-openings list/i },
  { id: "featured_recent_openings", re: /featured among recent everhome openings/i },
  { id: "chd_token", re: /\bchd\b/i },
  { id: "recent_openings_list", re: /recent-openings list/i },
  { id: "active_property_page", re: /active property page/i },
  { id: "consumer_page", re: /consumer page/i },
  { id: "consumer_path", re: /consumer path/i },
  { id: "property_listing_page", re: /property listing page/i },
  { id: "development_site_phrase", re: /development site/i },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "internal", re: /\binternal\b/i },
  { id: "extraction", re: /\bextraction\b/i },
  { id: "fdd", re: /\bfdd\b/i },
  { id: "item_19", re: /\bitem\s*19\b/i },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i },
  { id: "confirm_fees", re: /\bconfirm fees\b/i },
  { id: "performance_representation", re: /\bperformance representation\b/i },
];

const BLOCKED_OWNER_FACING_RE =
  /\bfdd\b|\bitem\s*19\b|franchise disclosure|confirm fees|confirm flag|performance representation|internal|extraction/i;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-everhome-openings-momentum-rebuild-writer.json",
  "reports/brand-explorer-everhome-presentation-cleanup-writer.json",
  "reports/brand-explorer-everhome-image-governance-recognition-writer.json",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Everhome Brand Explorer Presentation / API",
  "Tribute Portfolio + Radisson Individuals footprint.openings reference",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-everhome-openings-description-cleanup-writer.js",
  "scripts/brand-explorer-everhome-openings-description-cleanup-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function isSafeHttpUrl(s) {
  const u = nz(s);
  return /^https?:\/\//i.test(u) && !/\s/.test(u);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v32eR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-everhome-openings-description-cleanup-writer.js"
    )
  );
}

/** Mirrors public/js/brand-explorer-atelier-from-api.js parseFootprintOpeningParas */
export function parseFootprintOpeningParas(bodyRaw) {
  let paras = String(bodyRaw || "")
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  let summaryHref = "";
  if (paras.length && isSafeHttpUrl(paras[paras.length - 1])) {
    summaryHref = paras[paras.length - 1];
    paras = paras.slice(0, -1);
  }
  let chips = "";
  let loc = "";
  let asset = "";
  let scenario = "";
  let situation = "";
  let why = "";
  let takeaway = "";
  let parseMode = "short_fallback";
  if (paras.length >= 6) {
    [chips, loc, asset, situation, why, takeaway] = paras;
    parseMode = "full_6_block";
  } else if (paras.length === 5) {
    [chips, loc, asset, scenario, situation] = paras;
    parseMode = "voco_5_block";
  } else if (paras.length === 4) {
    [chips, loc, asset, situation] = paras;
    parseMode = "standard_4_block";
  } else if (paras.length === 3) {
    [chips, loc, situation] = paras;
    parseMode = "compressed_3_block";
  } else if (paras.length === 2) {
    chips = paras[0];
    situation = paras[1];
    parseMode = "compressed_2_block";
  } else if (paras.length === 1) {
    chips = paras[0];
    parseMode = "chips_only";
  }
  return {
    summaryHref,
    chips,
    loc,
    asset,
    scenario,
    situation,
    why,
    takeaway,
    parseMode,
  };
}

export function detectSourceMetadataPhrases(text) {
  const hay = nz(text);
  if (!hay) return [];
  return SOURCE_METADATA_PATTERNS.filter((p) => p.re.test(hay)).map((p) => p.id);
}

export function containsSourceMetadataLanguage(text) {
  return detectSourceMetadataPhrases(text).length > 0;
}

function supportedDemandDriversFromContext(parsed, title) {
  const hay = `${parsed.chips}\n${parsed.loc}\n${parsed.asset}\n${parsed.scenario}\n${title}`.toLowerCase();
  const drivers = [];
  if (/\bcorporate\b/.test(hay)) drivers.push("corporate");
  if (/\brelocation\b/.test(hay)) drivers.push("relocation");
  if (/\bproject\b/.test(hay)) drivers.push("project-based");
  if (/\bhealthcare\b|\bhospital\b/.test(hay)) drivers.push("healthcare");
  if (/\binfrastructure\b/.test(hay)) drivers.push("infrastructure");
  if (/\bsuburban\b/.test(hay)) drivers.push("suburban");
  return drivers;
}

function marketContextLabel(parsed, title) {
  const hay = `${parsed.chips}\n${parsed.loc}\n${parsed.scenario}\n${title}`.toLowerCase();
  if (/coastal|beach|gulf|panama city/i.test(hay)) return "coastal";
  if (/california|inland empire|ontario|corona/i.test(hay)) return "california";
  if (/texas|austin|georgetown/i.test(hay)) return "texas";
  if (/new jersey|nyc|somerset|brunswick|metro/i.test(hay)) return "suburban_metro";
  if (/florida/i.test(hay)) return "florida";
  return "general";
}

export function buildOwnerFacingOpeningTeaser(row, parsed) {
  const market = marketContextLabel(parsed, row.title);
  const drivers = supportedDemandDriversFromContext(parsed, row.title);
  const location =
    nz(parsed.loc) ||
    parseFootprintOpeningLocation(row.title, row.body) ||
    "the local market";

  if (market === "coastal" || market === "florida") {
    return "An Everhome extended-stay example in a coastal market, illustrating the brand's kitchen-equipped suite model for owners evaluating longer-stay positioning.";
  }
  if (market === "california") {
    return "A California extended-stay example that helps owners evaluate Everhome's apartment-style prototype in markets with suburban and longer-stay demand potential.";
  }
  if (market === "texas") {
    if (drivers.length) {
      return `A Central Texas extended-stay example that shows how Everhome can support longer-stay demand through kitchen-equipped suites and apartment-style layouts.`;
    }
    return "A Central Texas extended-stay example that illustrates Everhome's apartment-style suite model for owners evaluating new-construction extended-stay positioning.";
  }
  if (market === "suburban_metro") {
    if (drivers.length) {
      return `An Everhome extended-stay example in a suburban metro market, illustrating the brand's apartment-style prototype for owners evaluating new-construction positioning for ${drivers.join(", ")} demand.`;
    }
    return "An Everhome extended-stay example in a suburban metro market, illustrating the brand's apartment-style suite model for owners evaluating new-construction extended-stay positioning.";
  }
  if (drivers.length) {
    return `A market example for owners evaluating extended-stay demand linked to ${drivers.join(", ")} demand drivers, illustrating Everhome's apartment-style suite model in ${location}.`;
  }
  return `An Everhome extended-stay example in ${location}, illustrating the brand's apartment-style suite model for owners evaluating new-construction extended-stay positioning.`;
}

export function rebuildOpeningBody(parsed, newSituation) {
  const parts = [];
  if (parsed.chips) parts.push(parsed.chips);
  if (parsed.loc) parts.push(parsed.loc);
  if (parsed.asset) parts.push(parsed.asset);
  if (parsed.scenario) parts.push(parsed.scenario);
  if (parsed.parseMode === "full_6_block") {
    parts.push(newSituation);
    if (parsed.why) parts.push(parsed.why);
    if (parsed.takeaway) parts.push(parsed.takeaway);
  } else if (parsed.parseMode === "compressed_2_block" || parsed.parseMode === "compressed_3_block") {
    parts.push(newSituation);
  } else {
    parts.push(newSituation);
  }
  let body = parts.filter((p) => nz(p)).join("\n\n");
  if (parsed.summaryHref) body += `\n\n${parsed.summaryHref}`;
  return body;
}

export function proposeOpeningDescriptionUpdate(row) {
  const parsed = parseFootprintOpeningParas(row.body);
  const teaser = nz(parsed.situation);
  const metadataHits = detectSourceMetadataPhrases(teaser);
  const blockedHits = BLOCKED_OWNER_FACING_RE.test(teaser);
  const needsUpdate =
    metadataHits.length > 0 || (blockedHits && teaser.length > 0);

  if (!needsUpdate) {
    const ownerFacing =
      teaser.length > 40 &&
      !containsSourceMetadataLanguage(teaser) &&
      !BLOCKED_OWNER_FACING_RE.test(teaser);
    return {
      recordId: row.recordId,
      shouldUpdate: false,
      reason: ownerFacing ? "already_owner_facing" : "no_teaser_or_already_clean",
      parsed,
      metadataHitsBefore: metadataHits,
      metadataHitsAfter: metadataHits,
    };
  }

  const newTeaser = buildOwnerFacingOpeningTeaser(row, parsed);
  const afterBody = rebuildOpeningBody(parsed, newTeaser);
  const metadataHitsAfter = detectSourceMetadataPhrases(newTeaser);

  return {
    recordId: row.recordId,
    slotKey: row.slotKey,
    title: row.title,
    shouldUpdate: true,
    reason: metadataHits.length ? "source_metadata_in_teaser" : "blocked_owner_facing_language",
    before: row.body,
    after: afterBody,
    parsed,
    teaserBefore: teaser,
    teaserAfter: newTeaser,
    metadataHitsBefore: metadataHits,
    metadataHitsAfter,
    preservedChips: parsed.chips,
    preservedSourceUrl: parsed.summaryHref || row.summaryUrl || "",
    ownerFacingBefore: false,
    ownerFacingAfter: !containsSourceMetadataLanguage(newTeaser),
  };
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function firstAttachmentUrl(fields) {
  for (const key of ["Image", "Images", "Scenario Image", "Attachments"]) {
    const att = fields?.[key];
    if (Array.isArray(att) && att[0]?.url) return nz(att[0].url);
  }
  return "";
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params.toString()}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return rows.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      summaryUrl: nz(f["Summary URL"] || f["View Summary URL"] || f["Case summary URL"]),
      sortOrder: f["Sort Order"],
      active: f.Active,
      externalDisplayStatus: nz(f["External Display Status"]),
      imageUrl: firstAttachmentUrl(f),
      hasImage: Boolean(firstAttachmentUrl(f)),
    };
  });
}

function validatePatchFields(fields) {
  const errs = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PATCH_FIELDS.has(key)) errs.push(`blocked_field:${key}`);
  }
  const txt = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (scanCopySafety(txt).length) errs.push("copy_safety_fail");
  if (BLOCKED_OWNER_FACING_RE.test(txt)) errs.push("blocked_owner_facing_language");
  const afterParsed = parseFootprintOpeningParas(fields.Body || "");
  if (containsSourceMetadataLanguage(afterParsed.situation)) {
    errs.push("source_metadata_remains_in_teaser");
  }
  return errs;
}

function buildPatchFields(row, updatedBody) {
  return {
    "Slot Key": row.slotKey,
    Title: row.title,
    Body: updatedBody,
    "Sort Order": row.sortOrder ?? 0,
    Active: row.active !== false,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-everhome-openings-description-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_SOURCE_URLS,
    APPLY_FLAG_NO_OPENING_LABELS,
    APPLY_FLAG_NO_VISIBILITY,
    APPLY_FLAG_EVERHOME_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Everhome Openings Description Cleanup v32E-R1");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32E-R1 exists: **${report.v32eR1WriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Rows updated: **${report.rowsUpdated.length}**`);
  lines.push(`- Source-metadata phrases before: **${report.sourceMetadataPhrasesFoundBefore.length}**`);
  lines.push(`- Source-metadata phrases after (proposed): **${report.sourceMetadataPhrasesFoundAfter.length}**`);
  lines.push("");
  lines.push("## Opening Descriptions Before/After");
  for (const row of report.openingsDescriptionsBeforeAfter) {
    lines.push(`### ${row.propertyTitle} (\`${row.recordId}\`)`);
    lines.push(`- Teaser before: ${row.teaserBefore || "(empty)"}`);
    lines.push(`- Teaser after: ${row.teaserAfter || "(unchanged)"}`);
    lines.push(`- Metadata hits before: ${row.metadataHitsBefore.join(", ") || "none"}`);
  }
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply Command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerEverhomeOpeningsDescriptionCleanupWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFields = false,
  noSourceUrlChanges = false,
  noOpeningLabelChanges = false,
  noVisibility = false,
  everhomeOnly = false,
} = {}) {
  if (nz(brandArg).toLowerCase() !== TARGET_BRAND.slug) {
    throw new Error(`v32E-R1 is Everhome-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load Everhome API shape");

  const presentationRows = await listPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const openingsRows = presentationRows.filter((r) => r.slotKey === OPENINGS_SLOT);
  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const openingsAudit = [];
  const proposals = [];
  const rowsLeftUnchanged = [];
  const applyBlockers = [];

  for (const row of openingsRows) {
    const parsed = parseFootprintOpeningParas(row.body);
    const apiBlock = apiById.get(row.recordId);
    const proposal = proposeOpeningDescriptionUpdate(row);
    const metadataInTeaser = detectSourceMetadataPhrases(parsed.situation);

    openingsAudit.push({
      recordId: row.recordId,
      propertyTitle: row.title,
      location: parsed.loc || parseFootprintOpeningLocation(row.title, row.body),
      currentBody: row.body,
      parsedLabelsChips: parsed.chips,
      parsedDescriptionTeaser: parsed.situation,
      imageStatus: row.hasImage ? "has_attachment" : "missing",
      imageLoadingInApi: Boolean(apiBlock?.imageUrl),
      sourceUrl: parsed.summaryHref || row.summaryUrl || "",
      visibility: row.externalDisplayStatus || "visible",
      visibleInApi: apiBlocks.some((b) => b.recordId === row.recordId),
      containsSourceMetadataLanguage: metadataInTeaser.length > 0,
      ownerFacingDescription:
        !metadataInTeaser.length &&
        !BLOCKED_OWNER_FACING_RE.test(parsed.situation) &&
        nz(parsed.situation).length > 40,
      shouldUpdate: proposal.shouldUpdate,
      metadataHits: metadataInTeaser,
      parseMode: parsed.parseMode,
    });

    if (proposal.shouldUpdate) {
      proposals.push(proposal);
    } else {
      rowsLeftUnchanged.push({
        recordId: row.recordId,
        propertyTitle: row.title,
        reason: proposal.reason,
      });
    }
  }

  const sourceMetadataPhrasesFoundBefore = [
    ...new Set(openingsAudit.flatMap((r) => r.metadataHits)),
  ];
  const sourceMetadataPhrasesFoundAfter = [
    ...new Set(
      proposals.flatMap((p) => p.metadataHitsAfter).filter((id) => id)
    ),
  ];

  for (const proposal of proposals) {
    const beforeParsed = parseFootprintOpeningParas(proposal.before);
    const afterParsed = parseFootprintOpeningParas(proposal.after);
    if (beforeParsed.chips !== afterParsed.chips) {
      applyBlockers.push(`opening_chips_changed:${proposal.recordId}`);
    }
    const beforeUrl = beforeParsed.summaryHref || "";
    const afterUrl = afterParsed.summaryHref || "";
    if (beforeUrl !== afterUrl) {
      applyBlockers.push(`source_url_changed:${proposal.recordId}`);
    }
    const patchErrs = validatePatchFields(buildPatchFields(
      openingsRows.find((r) => r.recordId === proposal.recordId),
      proposal.after
    ));
    if (patchErrs.length) {
      applyBlockers.push(...patchErrs.map((e) => `${proposal.recordId}:${e}`));
    }
  }

  const applyGatesReady =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFields &&
    noSourceUrlChanges &&
    noOpeningLabelChanges &&
    noVisibility &&
    everhomeOnly;

  const dryRunClean = applyBlockers.length === 0 && proposals.length > 0;
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  const applyResults = { updated: [], errors: [] };

  if (canApply) {
    for (const proposal of proposals) {
      const row = openingsRows.find((r) => r.recordId === proposal.recordId);
      try {
        const fields = buildPatchFields(row, proposal.after);
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) },
          proposal.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.updated.push(proposal.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: proposal.recordId, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    writerVersion: WRITER_VERSION,
    v32eR1WriterExists: v32eR1WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    openingsAudit,
    openingsDescriptionsBeforeAfter: proposals.map((p) => ({
      recordId: p.recordId,
      propertyTitle: p.title,
      teaserBefore: p.teaserBefore,
      teaserAfter: p.teaserAfter,
      before: p.before,
      after: p.after,
      metadataHitsBefore: p.metadataHitsBefore,
      metadataHitsAfter: p.metadataHitsAfter,
      preservedChips: p.preservedChips,
      preservedSourceUrl: p.preservedSourceUrl,
    })),
    sourceMetadataPhrasesFoundBefore: sourceMetadataPhrasesFoundBefore,
    sourceMetadataPhrasesFoundAfter: sourceMetadataPhrasesFoundAfter,
    rowsUpdated: proposals.map((p) => ({ recordId: p.recordId, title: p.title })),
    rowsLeftUnchanged,
    imagesUntouched: true,
    labelsChipsUntouched: !applyBlockers.some((b) => b.includes("opening_chips_changed")),
    sourceUrlsUntouched: !applyBlockers.some((b) => b.includes("source_url_changed")),
    imageFieldsChanged: false,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaImpact:
      "Should reduce source-metadata language in footprint.openings teasers; image/registry gates unchanged.",
    expectedCompleteBuildImpact:
      "Copy clarity improves; active-profile still blocked on image governance until founder approvals.",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand:
      "npm run brand-explorer-everhome-openings-description-cleanup-writer -- --brand everhome-suites --dry-run",
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
