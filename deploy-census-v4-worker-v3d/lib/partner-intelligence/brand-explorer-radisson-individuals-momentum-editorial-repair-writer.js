/**
 * Brand Explorer Radisson Individuals Recent Momentum Editorial + Source Link Repair v31F.
 *
 * Repairs footprint.momentum presentation rows: proper-case headings, polished
 * announcement-style descriptions, and source links aligned to official Choice
 * press materials. Text/source editorial only — never touches images or facts.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-momentum-editorial-repair-writer-v31F.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  findInternalLanguageInRow,
} from "./brand-explorer-openings-ui-quarantine-governance.js";
import {
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
  isPresentationRowVisibleInExplorer,
  TARGET_BRAND as SUPPRESSION_TARGET,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";

export const WRITER_VERSION = "31F";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-momentum-editorial-repair-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-editorial-repair-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-editorial-repair-writer-v31F.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31F-radisson-individuals-momentum-editorial-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-momentum-copy";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const MOMENTUM_SLOT = "footprint.momentum";
export const TARGET_BRAND = SUPPRESSION_TARGET;

export const RADISSON_INDIVIDUALS_PRESS_KIT_URL =
  "https://media.choicehotels.com/Radisson-Individuals-press-kit";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "ascend",
  "radisson-blu",
  "kimpton",
  "curio-collection",
  "suburban-studios",
  "woodspring-suites",
  "everhome-suites",
  ...WAVE1_EXPANSION_SLUGS.filter((s) => s !== TARGET_BRAND.slug),
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PR_URL_RE =
  /newsroom|press-release|press_release|\/news\/|media\.choicehotels\.com|ihgplc\.com\/news/i;

const GENERIC_INTERNAL_TITLE_RE =
  /\b(cala portfolio expansion|pipeline\s*&\s*system scale|census property url|source-backed opening|individuals collection\s*—|individuals\s*—\s*panama and regional)\b/i;

const FORBIDDEN_BODY_RE =
  /\b(census url|census property url|directory anchor|item\s*19|consumer paths?|not a comp-set guarantee|confirm flag, fees|source data|metadata\b|internal extraction|extraction\b|dealality census|underwriting conversion economics|owners should confirm individuals qa)\b/i;

const WEAK_SOURCE_RE =
  /\b(census|dealality|internal|localhost|airtable\.com)\b/i;

/** Founder-reviewed editorial packages — Tribute-quality style, Radisson-specific content. */
export const MOMENTUM_EDITORIAL_PACKAGES = Object.freeze([
  {
    recordId: "rec0an5blfW4FtMfE",
    sort: 0,
    dateLine: "2024",
    polishedTitle: "Radisson Individuals Expands in CALA",
    polishedSummary:
      "Choice Hotels highlighted new Radisson Individuals properties across Colombia and Panama, reinforcing the brand's role as a hand-selected soft-collection path for independent hotels that want Choice Privileges distribution while preserving local identity.",
    sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
    sourceBasis:
      "Choice Hotels official Radisson Individuals press kit (media.choicehotels.com) — portfolio CALA expansion context; not a single-property listing.",
  },
  {
    recordId: "recb0WzRRu6jrev4c",
    sort: 1,
    dateLine: "2024",
    polishedTitle: "Medellín and Cartagena Join the Individuals Collection",
    polishedSummary:
      "Choice Hotels added Radisson Individuals properties in Medellín and Cartagena, showing how the collection positions boutique and independent hotels within Choice's upper-upscale CALA footprint while keeping property-specific character.",
    sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
    sourceBasis:
      "Choice Hotels official Radisson Individuals press kit — Colombia market examples referenced in brand media materials.",
  },
  {
    recordId: "recpIgmBNBEMXVEda",
    sort: 2,
    dateLine: "2024",
    polishedTitle: "Panama Adds Regional Growth Context",
    polishedSummary:
      "Radisson Individuals growth in Panama City and surrounding markets extends Choice's hand-selected upper-upscale presence in Central America—illustrating regional expansion for owners evaluating soft-brand affiliation in gateway corridors.",
    sourceUrl: RADISSON_INDIVIDUALS_PRESS_KIT_URL,
    sourceBasis:
      "Choice Hotels official Radisson Individuals press kit — Panama and regional CALA growth context.",
  },
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md",
  "reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-suppression-writer.json",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.md",
  "reports/brand-explorer-brand-asset-registry-discovery-writer.json",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.md",
  "reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.md",
  "reports/brand-explorer-complete-build-radisson-individuals-by-choice.json",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/brand-explorer-momentum-editorial-link-repair-writer.js",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "live Radisson Individuals Brand Explorer Presentation rows",
  "live Source Library records",
  "live Partner Facts",
  "Tribute footprint.momentum rows (quality reference only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js",
  "scripts/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.mjs",
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

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v31fWriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31F`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31F supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
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

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function parseMomentumBody(body) {
  const paras = normalizeBody(body)
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  const dateLine = paras[0] || "";
  let sourceUrl = "";
  const descParts = [];
  for (let i = 1; i < paras.length; i++) {
    if (/^https?:\/\//i.test(paras[i])) sourceUrl = paras[i];
    else descParts.push(paras[i]);
  }
  return { dateLine, description: descParts.join("\n\n"), sourceUrl };
}

export function classifyMomentumSourceUrl(url) {
  const u = nz(url).toLowerCase();
  if (!u) return { sourceType: "missing", isPressRelease: false, isOfficial: false, isWeak: true };
  if (WEAK_SOURCE_RE.test(u) || /choicehotels\.com\/[^/]+\/[^/]+\/[^/]+\/radisson-individuals-hotels\//.test(u)) {
    return {
      sourceType: "property_listing",
      isPressRelease: false,
      isOfficial: false,
      isWeak: true,
    };
  }
  if (PR_URL_RE.test(u) && /press-kit|press_kit/.test(u)) {
    return {
      sourceType: "official_press_kit",
      isPressRelease: false,
      isOfficial: true,
      isWeak: false,
    };
  }
  if (PR_URL_RE.test(u) || /\/news\//.test(u)) {
    return {
      sourceType: "press_release",
      isPressRelease: true,
      isOfficial: true,
      isWeak: false,
    };
  }
  if (u.includes("media.choicehotels.com")) {
    return {
      sourceType: "official_announcement_hub",
      isPressRelease: false,
      isOfficial: true,
      isWeak: false,
    };
  }
  if (u.includes("choicehotels.com") && !/\/hotels\//.test(u)) {
    return {
      sourceType: "consumer_brand_page",
      isPressRelease: false,
      isOfficial: true,
      isWeak: true,
    };
  }
  if (u.includes("choicehotels.com")) {
    return {
      sourceType: "property_listing",
      isPressRelease: false,
      isOfficial: false,
      isWeak: true,
    };
  }
  return { sourceType: "other", isPressRelease: false, isOfficial: false, isWeak: true };
}

export function proposedLinkLabelForSource(sourceClassification) {
  const type = sourceClassification?.sourceType || "missing";
  if (type === "press_release") return "View Press Release";
  if (type === "official_press_kit" || type === "official_announcement_hub") {
    return "View Official Announcement";
  }
  if (type === "consumer_brand_page") return "View Choice Hotels Brand Page";
  if (type === "property_listing") return "View Property Listing";
  return "View Source";
}

export function legacyFrontendLinkLabel(url) {
  const u = nz(url).toLowerCase();
  if (u.includes("media.choicehotels.com") || u.includes("choicehotels.com")) {
    return "View Choice Hotels announcement";
  }
  return "View announcement";
}

function buildMomentumBody(pkg) {
  return normalizeBody([pkg.dateLine, pkg.polishedSummary, pkg.sourceUrl].join("\n\n"));
}

export function assessTitleQuality(title) {
  const t = nz(title);
  const issues = [];
  if (!t) issues.push("missing_title");
  if (GENERIC_INTERNAL_TITLE_RE.test(t)) issues.push("generic_or_internal_title");
  if (/^[a-z]/.test(t) && !/^[a-z]{2,3}\b/.test(t)) issues.push("improper_case_lead");
  if (/portfolio expansion$/i.test(t) && !/^Radisson Individuals Expands/i.test(t)) {
    issues.push("lowercase_expansion_label");
  }
  if (t === t.toLowerCase() && t.length > 12) issues.push("all_lowercase_heading");
  return {
    activeProfileQuality: issues.length === 0,
    issues,
  };
}

export function assessBodyQuality(body) {
  const b = normalizeBody(body);
  const issues = [];
  if (!b) issues.push("missing_body");
  if (FORBIDDEN_BODY_RE.test(b)) issues.push("source_capture_or_internal_language");
  for (const hit of findInternalLanguageInRow({ body: b })) {
    if (["high", "critical"].includes(hit.severity)) {
      issues.push(`internal_language:${hit.markerId}`);
    }
  }
  if (/\bpress release\b/i.test(b) && !PR_URL_RE.test(b)) {
    issues.push("press_claim_without_pr_url");
  }
  return {
    activeProfileQuality: issues.length === 0,
    issues,
  };
}

export function assessLinkLabelQuality(url, proposedLabel) {
  const classification = classifyMomentumSourceUrl(url);
  const legacy = legacyFrontendLinkLabel(url);
  const issues = [];
  if (classification.isWeak && /announcement|press release/i.test(proposedLabel)) {
    issues.push("link_overstates_weak_source");
  }
  if (
    classification.sourceType === "property_listing" &&
    /announcement|press release/i.test(legacy)
  ) {
    issues.push("frontend_overstates_property_listing");
  }
  if (!url) issues.push("missing_source_url");
  return {
    classification,
    currentFrontendLabel: legacy,
    proposedLabel,
    activeProfileQuality: issues.length === 0,
    issues,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-momentum-editorial-repair-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsMomentumEditorialRepairWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listPresentationRowsRaw(
    baseId,
    apiKey,
    target.recordId,
    target.name
  );

  const allRows = presentationRaw.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: normalizeBody(f.Body),
      sortOrder: f["Sort Order"],
      externalDisplayStatus: nz(f["External Display Status"]),
      visibleInExplorer: isPresentationRowVisibleInExplorer(f),
      quarantined: HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(nz(f["External Display Status"])),
      hasImage: Array.isArray(f.Image) && f.Image.length > 0,
      imageCount: Array.isArray(f.Image) ? f.Image.length : 0,
    };
  });

  const momentumRows = allRows.filter((r) => r.slotKey === MOMENTUM_SLOT);
  const openingsRows = allRows.filter((r) => r.slotKey === "footprint.openings");

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));
  const qaBrand = finalQaBefore?.brandReports?.[0] || {};

  const momentumRowDiagnosis = [];
  const sourceLinkValidation = [];
  const rowsWouldUpdate = [];
  const rowsToSuppress = [];
  const beforeAfterCopy = [];
  const applyBlockers = [];

  if (momentumRows.length !== MOMENTUM_EDITORIAL_PACKAGES.length) {
    applyBlockers.push(
      `momentum_row_count_mismatch:live=${momentumRows.length},expected=${MOMENTUM_EDITORIAL_PACKAGES.length}`
    );
  }

  for (const pkg of MOMENTUM_EDITORIAL_PACKAGES) {
    const live =
      momentumRows.find((r) => r.recordId === pkg.recordId) ||
      momentumRows.find((r) => Number(r.sortOrder ?? -1) === Number(pkg.sort));

    const parsed = live ? parseMomentumBody(live.body) : { dateLine: "", description: "", sourceUrl: "" };
    const sourceClass = classifyMomentumSourceUrl(parsed.sourceUrl || pkg.sourceUrl);
    const proposedSourceClass = classifyMomentumSourceUrl(pkg.sourceUrl);
    const proposedLinkLabel = proposedLinkLabelForSource(proposedSourceClass);
    const proposedBody = buildMomentumBody(pkg);

    const titleBefore = live?.title || "";
    const bodyBefore = live?.body || "";
    const titleQualityBefore = assessTitleQuality(titleBefore);
    const bodyQualityBefore = assessBodyQuality(parsed.description || bodyBefore);
    const linkQualityBefore = assessLinkLabelQuality(
      parsed.sourceUrl,
      legacyFrontendLinkLabel(parsed.sourceUrl)
    );
    const titleQualityAfter = assessTitleQuality(pkg.polishedTitle);
    const bodyQualityAfter = assessBodyQuality(pkg.polishedSummary);
    const linkQualityAfter = assessLinkLabelQuality(pkg.sourceUrl, proposedLinkLabel);

    const diagnosis = {
      recordId: live?.recordId || pkg.recordId,
      sort: pkg.sort,
      title: titleBefore,
      yearDate: parsed.dateLine || pkg.dateLine,
      body: bodyBefore,
      parsedDescription: parsed.description,
      linkLabel: linkQualityBefore.currentFrontendLabel,
      sourceUrl: parsed.sourceUrl || null,
      sourceType: sourceClass.sourceType,
      isPressRelease: sourceClass.isPressRelease,
      isOfficialAnnouncement: sourceClass.isOfficial,
      isWeakSource: sourceClass.isWeak,
      titleActiveProfileQuality: titleQualityBefore.activeProfileQuality,
      bodyActiveProfileQuality: bodyQualityBefore.activeProfileQuality,
      linkActiveProfileQuality: linkQualityBefore.activeProfileQuality,
      titleIssues: titleQualityBefore.issues,
      bodyIssues: bodyQualityBefore.issues,
      linkIssues: linkQualityBefore.issues,
      visibleInExplorer: live?.visibleInExplorer ?? false,
      quarantined: live?.quarantined ?? false,
      recommendation: "repair_editorial",
    };

    if (!live) {
      diagnosis.recommendation = "missing_live_row";
      applyBlockers.push(`missing_momentum_row:${pkg.recordId}`);
      momentumRowDiagnosis.push(diagnosis);
      continue;
    }

    if (proposedSourceClass.isWeak) {
      diagnosis.recommendation = "pending_source_review";
      rowsToSuppress.push({
        recordId: live.recordId,
        slotKey: MOMENTUM_SLOT,
        title: live.title,
        reason: "weak_source_url",
        sourceUrl: pkg.sourceUrl,
        action: "pending_source_review",
      });
      applyBlockers.push(`weak_source_for_momentum:${pkg.recordId}`);
    }

    if (
      bodyQualityBefore.issues.includes("press_claim_without_pr_url") ||
      (linkQualityBefore.issues.includes("frontend_overstates_property_listing") &&
        sourceClass.isWeak)
    ) {
      applyBlockers.push(`false_announcement_claim:${pkg.recordId}`);
    }

    if (FORBIDDEN_BODY_RE.test(pkg.polishedSummary) || GENERIC_INTERNAL_TITLE_RE.test(pkg.polishedTitle)) {
      applyBlockers.push(`forbidden_language_in_proposal:${pkg.recordId}`);
    }

    if (linkQualityAfter.issues.includes("link_overstates_weak_source")) {
      applyBlockers.push(`link_label_overstates_source:${pkg.recordId}`);
    }

    momentumRowDiagnosis.push(diagnosis);

    sourceLinkValidation.push({
      recordId: live.recordId,
      title: pkg.polishedTitle,
      currentSourceUrl: parsed.sourceUrl,
      proposedSourceUrl: pkg.sourceUrl,
      currentSourceType: sourceClass.sourceType,
      proposedSourceType: proposedSourceClass.sourceType,
      currentLinkLabel: linkQualityBefore.currentFrontendLabel,
      proposedLinkLabel,
      sourceBasis: pkg.sourceBasis,
      validationPassed:
        proposedSourceClass.isOfficial && !proposedSourceClass.isWeak && linkQualityAfter.activeProfileQuality,
      issues: [...linkQualityBefore.issues, ...linkQualityAfter.issues],
    });

    const needsUpdate =
      nz(live.title) !== pkg.polishedTitle || normalizeBody(live.body) !== proposedBody;

    beforeAfterCopy.push({
      recordId: live.recordId,
      sort: pkg.sort,
      before: {
        title: titleBefore,
        body: bodyBefore,
        sourceUrl: parsed.sourceUrl,
        linkLabel: linkQualityBefore.currentFrontendLabel,
      },
      after: {
        title: pkg.polishedTitle,
        body: proposedBody,
        sourceUrl: pkg.sourceUrl,
        linkLabel: proposedLinkLabel,
      },
    });

    if (needsUpdate && diagnosis.recommendation === "repair_editorial") {
      rowsWouldUpdate.push({
        action: "momentum_editorial_repair",
        recordId: live.recordId,
        slotKey: MOMENTUM_SLOT,
        fixReason: "momentum_title_body_source_editorial",
        proposedTitle: pkg.polishedTitle,
        proposedBody,
        proposedSourceUrl: pkg.sourceUrl,
        proposedLinkLabel,
        fields: {
          Title: pkg.polishedTitle,
          Body: proposedBody,
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
        before: {
          title: titleBefore,
          body: bodyBefore.slice(0, 320),
        },
        after: {
          title: pkg.polishedTitle,
          body: proposedBody.slice(0, 320),
        },
        imageUntouched: true,
      });
    }
  }

  for (const row of momentumRows) {
    if (!MOMENTUM_EDITORIAL_PACKAGES.some((p) => p.recordId === row.recordId)) {
      applyBlockers.push(`unexpected_momentum_row:${row.recordId}`);
      momentumRowDiagnosis.push({
        recordId: row.recordId,
        title: row.title,
        recommendation: "unexpected_row_review",
        titleActiveProfileQuality: assessTitleQuality(row.title).activeProfileQuality,
        bodyActiveProfileQuality: assessBodyQuality(row.body).activeProfileQuality,
      });
    }
  }

  const postRepairInternal = rowsWouldUpdate.filter((u) => {
    const afterIssues = assessBodyQuality(u.proposedBody);
    const titleIssues = assessTitleQuality(u.proposedTitle);
    return !afterIssues.activeProfileQuality || !titleIssues.activeProfileQuality;
  });
  if (postRepairInternal.length) {
    applyBlockers.push("internal_language_would_remain_on_momentum_rows");
  }

  const openingsTouched = rowsWouldUpdate.some((u) => u.slotKey === "footprint.openings");
  if (openingsTouched) applyBlockers.push("openings_rows_would_be_modified");

  const quarantinedReactivated = rowsWouldUpdate.some((u) => {
    const row = allRows.find((r) => r.recordId === u.recordId);
    return row?.quarantined && u.fields?.["External Display Status"];
  });
  if (quarantinedReactivated) applyBlockers.push("quarantined_row_reactivation_risk");

  const hasWork = rowsWouldUpdate.length > 0;
  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0;
  const canApply = applyGatesReady && dryRunClean && hasWork;

  let airtableModified = false;
  let applyResults = {
    updated: [],
    errors: [],
    imagesTouched: false,
    factsApproved: false,
    openingsRowsModified: false,
  };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of rowsWouldUpdate) {
      if (update.slotKey !== MOMENTUM_SLOT) {
        applyResults.errors.push({ recordId: update.recordId, error: "non_momentum_slot_blocked" });
        continue;
      }
      const liveRec = presentationRaw.find((r) => r.id === update.recordId);
      if (!liveRec) {
        applyResults.errors.push({ recordId: update.recordId, error: "record_not_found" });
        continue;
      }
      const patchFields = { ...update.fields };
      delete patchFields.Image;
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: patchFields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: json.error?.message || `PATCH failed ${res.status}`,
        });
        continue;
      }
      applyResults.updated.push({
        recordId: update.recordId,
        title: update.proposedTitle,
      });
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
    if (!hasWork) applyResults.note = "momentum_rows_already_match_proposals";
  }

  const expectedFinalQaNumeric = Math.min(
    88,
    (qaBrand.scores?.overallNumeric || 80) + (hasWork ? 4 : 0)
  );

  const report = {
    writerVersion: WRITER_VERSION,
    v31FWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    momentumRowDiagnosis,
    sourceLinkValidation,
    rowsWouldUpdate,
    rowsToSuppress,
    beforeAfterCopy,
    openingsRowsUntouched: !openingsTouched,
    openingsRowCount: openingsRows.length,
    quarantinedOpeningsUntouched: true,
    imagesTouched: false,
    imagesApproved: false,
    factsApproved: false,
    currentReadinessDiagnosis: {
      finalQaScore: qaBrand.scores?.overallNumeric ?? null,
      finalQaReadiness: qaBrand.scores?.overallActiveProfileReadiness ?? null,
      momentumRowCount: momentumRows.length,
    },
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyBlockers,
    dryRunClean,
    canApply,
    hasWork,
    applyResults,
    expectedFinalQaAfterApply: {
      overallNumeric: expectedFinalQaNumeric,
      overallActiveProfileReadiness: "almost_ready",
      note: hasWork
        ? "Momentum editorial polish improves owner-facing Recent Momentum; image approval still required for active-profile."
        : "Momentum copy already matches v31F proposals or no changes needed.",
    },
    exactDryRunCommand: `npm run brand-explorer-radisson-individuals-momentum-editorial-repair-writer -- --brand ${target.slug} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
    governanceNote:
      "v31F repairs footprint.momentum title/body/source editorial only — never approves images/facts, never reactivates quarantined openings rows, never modifies Company Validated.",
  };
  report.markdown = buildMarkdown(report);
  return report;
}

export function buildMarkdown(report) {
  const d = report.currentReadinessDiagnosis || {};
  const lines = [];
  lines.push(
    `# Brand Explorer Radisson Individuals Momentum Editorial Repair v${report.writerVersion}`
  );
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v31F exists: **${report.v31FWriterExists ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Momentum row diagnosis");
  lines.push(`- Rows audited: **${report.momentumRowDiagnosis?.length ?? 0}**`);
  for (const row of report.momentumRowDiagnosis || []) {
    lines.push(
      `- \`${row.recordId}\` **${row.title || "—"}** — title QA: ${row.titleActiveProfileQuality ? "ok" : "needs repair"} · body QA: ${row.bodyActiveProfileQuality ? "ok" : "needs repair"} · source: ${row.sourceType || "—"}`
    );
  }
  lines.push("");
  lines.push("## Source link validation");
  for (const v of report.sourceLinkValidation || []) {
    lines.push(
      `- **${v.title}**: ${v.proposedSourceType} → label **${v.proposedLinkLabel}** (${v.validationPassed ? "pass" : "fail"})`
    );
  }
  lines.push("");
  lines.push("## Updates");
  lines.push(`- Rows to update: **${report.rowsWouldUpdate?.length ?? 0}**`);
  lines.push(`- Rows to suppress/pending: **${report.rowsToSuppress?.length ?? 0}**`);
  lines.push(`- Images touched: **no**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Before / after (titles)");
  for (const row of report.beforeAfterCopy || []) {
    lines.push(`- ${row.before.title} → **${row.after.title}**`);
  }
  lines.push("");
  lines.push("## Expected after apply");
  lines.push(
    `- Final QA (est.): **${report.expectedFinalQaAfterApply?.overallNumeric ?? "—"}** (${report.expectedFinalQaAfterApply?.overallActiveProfileReadiness ?? "—"})`
  );
  lines.push("");
  lines.push("## Apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — resolve blockers first)");
  return lines.join("\n");
}
