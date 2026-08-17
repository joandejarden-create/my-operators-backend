/**
 * Sync rendered field completeness evaluation (no factory / Airtable loaders).
 * Safe to import from OS + factory rules without circular dependencies.
 */
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { ALL_INVENTORY_FIELDS } from "./brand-explorer-rendered-field-completeness-inventory.js";
import { RENDERED_FIELD_REMEDIATION_CONTENT } from "./brand-explorer-rendered-field-completeness-remediation-content.js";
import { TAB_FACTORY_REMEDIATION_CONTENT } from "./brand-explorer-tab-factory-remediation-content.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

function getByPath(obj, dotted) {
  if (!dotted) return undefined;
  return dotted.split(".").reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

function findSlot(rows, slotKey) {
  return (rows || []).find(
    (r) =>
      nz(r.slotKey) === slotKey &&
      r.active !== false &&
      r.visible !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
}

function findSlots(rows, slotKey) {
  return (rows || []).filter(
    (r) =>
      nz(r.slotKey) === slotKey &&
      r.active !== false &&
      r.visible !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
}

function stableImageKey(url) {
  const u = nz(url);
  if (!u) return "";
  try {
    const parsed = new URL(u);
    return `${parsed.origin}${parsed.pathname}`.toLowerCase();
  } catch {
    return u.split("?")[0].toLowerCase();
  }
}

function typicalKeys(brand) {
  const pp = brand.portfolioPerformance || {};
  const minK = pp.minPropertySize;
  const maxK = pp.maxPropertySize;
  if (minK != null && maxK != null && String(minK) && String(maxK)) {
    return `${minK}–${maxK} rooms`;
  }
  if (minK != null && String(minK)) return `${minK}+ rooms (minimum)`;
  if (maxK != null && String(maxK)) return `Up to ${maxK} rooms`;
  return "";
}

function primaryRegionsValue(brand) {
  const regions = brand.regionOffered;
  if (Array.isArray(regions) && regions.length) return regions.join("; ");
  return nz(regions);
}

function htmlHasBlankKv(html, label) {
  const esc = label.replace(/&/g, "&amp;").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`<dt>${esc}<\\/dt>\\s*<dd class="oe-dd oe-dd--empty">`, "i");
  return re.test(html);
}

function htmlCardBodyEmpty(html, label) {
  const esc = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(
    `<h3 class="[^"]*(?:brand-position-card__label|explorer-detail-card__label)[^"]*">${esc}<\\/h3>[\\s\\S]{0,280}?oe-dd--empty`,
    "i"
  );
  return re.test(html);
}

function stripHtml(s) {
  return nz(s)
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function resolveFieldValue(field, brand, rows) {
  if (field.derived === "typical_keys") {
    return { text: typicalKeys(brand), rows: [], recordId: brand.id || null, imageUrl: null };
  }
  if (field.derived === "primary_regions") {
    const fp = brand.footprint || {};
    const fv = fp.formValues || {};
    const reg = fp.regionalDistribution && typeof fp.regionalDistribution === "object" ? fp.regionalDistribution : {};
    const regionKeys = Object.keys(reg);
    const text =
      regionKeys.length > 0
        ? regionKeys.slice(0, 8).join(" · ")
        : nz(fv.specificMarkets) || primaryRegionsValue(brand);
    return { text, rows: [], recordId: brand.id || null, imageUrl: null };
  }
  if (field.slotKey) {
    if (field.componentType === "table_rows") {
      const list = findSlots(rows, field.slotKey);
      return {
        text: list.map((r) => nz(r.title) || nz(r.body)).filter(Boolean).join(" | "),
        rows: list,
        recordId: list[0]?.recordId || null,
        imageUrl: null,
        caseSummaryOverview: null,
      };
    }
    const row = findSlot(rows, field.slotKey);
    return {
      text: nz(row?.body) || nz(row?.title),
      rows: row ? [row] : [],
      recordId: row?.recordId || null,
      imageUrl: nz(row?.imageUrl),
      caseSummaryOverview: nz(row?.caseSummaryOverview),
      title: nz(row?.title),
    };
  }
  if (field.apiPath) {
    const raw = getByPath(brand, field.apiPath);
    const text = Array.isArray(raw) ? raw.filter(Boolean).join("; ") : nz(raw);
    return { text, rows: [], recordId: brand.id || brand.brandId || null, imageUrl: null };
  }
  return { text: "", rows: [], recordId: null, imageUrl: null };
}

function evaluateInventoryField(field, brand, rows, html, brandSlug) {
  const resolved = resolveFieldValue(field, brand, rows);
  const value = resolved.text;
  const w = words(value);
  let status = "pass";
  let recommendedAction = "no_action";
  let reason = "Rendered value meets minimum useful depth.";
  const proposedPatch = null;

  const blankInHtml =
    field.componentType === "kv"
      ? htmlHasBlankKv(html, field.componentLabel)
      : field.componentType === "card"
        ? htmlCardBodyEmpty(html, field.componentLabel)
        : false;

  if (field.componentType === "table_rows") {
    const count = resolved.rows?.length || 0;
    if (count < (field.minRows || 1)) {
      status = count === 0 ? "missing" : "too_thin";
      recommendedAction = "add_body";
      reason = `Owner checklist rows=${count}; need >= ${field.minRows}`;
    }
  } else if (!nz(value) || blankInHtml) {
    status = blankInHtml || !nz(value) ? "blank" : "missing";
    if (field.suppressible) {
      status = "should_suppress";
      recommendedAction = "suppress_component";
      reason = "Optional/suppressible field is blank — suppress empty shell rather than leave visible blank.";
    } else if (field.derived === "primary_regions" || field.derived === "typical_keys" || !field.slotKey) {
      status = "cleanly_unavailable";
      recommendedAction = "replace_with_not_publicly_disclosed";
      reason =
        "No steward-verified public value — treat as Not publicly disclosed / suppress empty shell; do not invent metrics.";
    } else {
      recommendedAction = "add_body";
      reason = blankInHtml
        ? "Visible blank rendered in UI (oe-dd--empty)."
        : "No owner-facing value available for rendered field.";
    }
  } else if (field.minWords && w < field.minWords) {
    status = "too_thin";
    recommendedAction = "rewrite_owner_copy";
    reason = `Only ${w} words; minimum useful depth is ${field.minWords}.`;
  }

  if (status === "pass" && field.minBullets) {
    const bullets = nz(value)
      .split(/\n+/)
      .map((l) => l.replace(/^[-•*]\s*/, "").trim())
      .filter(Boolean);
    if (bullets.length < field.minBullets) {
      status = "too_thin";
      recommendedAction = "rewrite_owner_copy";
      reason = `Bullet count ${bullets.length} < ${field.minBullets}.`;
    } else if (bullets.some((b) => words(b) <= 4)) {
      status = "too_thin";
      recommendedAction = "rewrite_owner_copy";
      reason = "Contains bullet-only / too-short lines.";
    }
  }

  if (status === "pass" && field.minChips) {
    const chips = nz(value)
      .split(/[\n;,]+/)
      .map((t) => t.trim())
      .filter(Boolean);
    if (chips.length < field.minChips) {
      status = "too_thin";
      recommendedAction = "rewrite_owner_copy";
      reason = `Chip count ${chips.length} < ${field.minChips}.`;
    }
  }

  if (field.requireImage && status === "pass" && !nz(resolved.imageUrl)) {
    status = "missing";
    recommendedAction = "reassign_existing_image";
    reason = "Scenario card missing image.";
  }

  if (field.requireCaseSummary && status === "pass" && !nz(resolved.caseSummaryOverview)) {
    status = "needs_owner_copy";
    recommendedAction = "add_case_summary";
    reason = "Featured application missing Case Summary overview.";
  }

  // SLH geography mislabels on openings are handled in openings scan
  if (
    brandSlug === "small-luxury-hotels-of-the-world" &&
    field.slotKey === "overview.relative_positioning" &&
    /franchise|chain conversion required/i.test(value)
  ) {
    status = "misleading";
    recommendedAction = "rewrite_owner_copy";
    reason = "SLH copy forces franchise/chain conversion logic.";
  }

  const packRow = (RENDERED_FIELD_REMEDIATION_CONTENT[brandSlug]?.presentation || []).find(
    (p) => p.slotKey === field.slotKey
  );

  return {
    brandSlug,
    tabName: field.tabName,
    sectionName: field.sectionName,
    componentType: field.componentType,
    componentLabel: field.componentLabel,
    fieldName: field.fieldId,
    fieldId: field.fieldId,
    currentRenderedValue: nz(value).slice(0, 400) || "(blank)",
    sourceRecordType: field.slotKey
      ? "Brand Explorer Presentation"
      : field.basicsField
        ? "Brand Basics"
        : "Brand Library API",
    sourceRecordId: resolved.recordId,
    sourceFieldName: field.slotKey || field.basicsField || field.apiPath || field.derived || null,
    status,
    recommendedAction: status === "pass" ? "no_action" : recommendedAction,
    proposedPatch: status === "pass" ? null : packRow
      ? {
          table: "Brand Setup - Brand Explorer Presentation",
          action: resolved.recordId ? "PATCH" : "POST",
          recordId: resolved.recordId,
          slotKey: field.slotKey,
          fields: {
            Title: packRow.title || "",
            Body: packRow.body,
            ...(packRow.sortOrder != null ? { "Sort Order": packRow.sortOrder } : {}),
          },
        }
      : null,
    reason,
    wordCount: w,
    blankInHtml,
  };
}

function scanOpenings(brandSlug, rows, html) {
  const findings = [];
  const openings = findSlots(rows, "footprint.openings");
  if (!openings.length) {
    findings.push({
      brandSlug,
      tabName: "Footprint & Growth",
      sectionName: "Openings / Examples / Properties",
      componentType: "property_card",
      componentLabel: "Property examples",
      fieldName: "footprint.openings",
      currentRenderedValue: "(missing)",
      sourceRecordType: "Brand Explorer Presentation",
      sourceRecordId: null,
      sourceFieldName: "footprint.openings",
      status: "missing",
      recommendedAction: "add_body",
      proposedPatch: null,
      reason: "No footprint.openings rows visible.",
    });
    return findings;
  }
  for (const o of openings) {
    const bodyW = words(o.body);
    let status = "pass";
    let recommendedAction = "no_action";
    let reason = "Property example has useful body.";
    if (bodyW < 30) {
      status = "too_thin";
      recommendedAction = "rewrite_owner_copy";
      reason = `Opening body only ${bodyW} words.`;
    }
    if (!nz(o.caseSummaryOverview)) {
      status = status === "pass" ? "needs_owner_copy" : status;
      recommendedAction = "add_case_summary";
      reason = "Opening modal missing Case Summary Overview.";
    }
    if (
      brandSlug === "small-luxury-hotels-of-the-world" &&
      /cala property example/i.test(nz(o.title)) &&
      /(san r[eé]gis|quinta da comporta)/i.test(nz(o.title))
    ) {
      status = "wrong_geography_label";
      recommendedAction = "fix_label";
      reason = "Non-CALA property labeled as CALA Property Example.";
    }
    findings.push({
      brandSlug,
      tabName: "Footprint & Growth",
      sectionName: "Openings / Examples / Properties",
      componentType: "property_card",
      componentLabel: nz(o.title) || "Opening",
      fieldName: `footprint.openings:${o.recordId}`,
      currentRenderedValue: nz(o.body).slice(0, 400),
      sourceRecordType: "Brand Explorer Presentation",
      sourceRecordId: o.recordId,
      sourceFieldName: "Body / Case Summary",
      status,
      recommendedAction: status === "pass" ? "no_action" : recommendedAction,
      proposedPatch: null,
      reason,
    });
  }
  // HTML empty property shells
  const emptyShells = (html.match(/property-example-card__meta oe-dd--empty/g) || []).length;
  if (emptyShells > 0) {
    findings.push({
      brandSlug,
      tabName: "Footprint & Growth",
      sectionName: "Openings / Examples / Properties",
      componentType: "property_card",
      componentLabel: "Property card shell",
      fieldName: "footprint.openings.html_empty_shell",
      currentRenderedValue: `(${emptyShells} empty shells)`,
      sourceRecordType: "Rendered HTML",
      sourceRecordId: null,
      sourceFieldName: null,
      status: "blank",
      recommendedAction: "add_body",
      proposedPatch: null,
      reason: "Rendered property card shells still show empty meta/body markers.",
    });
  }
  return findings;
}

function scanScenarioImageDistinctiveness(brandSlug, rows) {
  const urls = [1, 2, 3]
    .map((i) => stableImageKey(findSlot(rows, `overview.scenario.${i}`)?.imageUrl))
    .filter(Boolean);
  if (urls.length >= 2 && new Set(urls).size < urls.length) {
    return [
      {
        brandSlug,
        tabName: "Overview",
        sectionName: "Where This Brand Creates the Most Value",
        componentType: "scenario_card",
        componentLabel: "Scenario images",
        fieldName: "overview.scenario.images",
        currentRenderedValue: urls.join(" | "),
        sourceRecordType: "Brand Explorer Presentation",
        sourceRecordId: null,
        sourceFieldName: "Image",
        status: "duplicate",
        recommendedAction: "reassign_existing_image",
        proposedPatch: null,
        reason: "Scenario cards reuse the same image path.",
      },
    ];
  }
  return [];
}

function scanMisleadingZeros(brandSlug, html) {
  if (/\b0\s+hotels\b|\b0\s+rooms\b|Hotels\s*:\s*0\b/i.test(stripHtml(html))) {
    return [
      {
        brandSlug,
        tabName: "Footprint & Growth",
        sectionName: "Presence Intelligence",
        componentType: "metric_card",
        componentLabel: "Zero metrics",
        fieldName: "metrics.zero",
        currentRenderedValue: "0 hotels/rooms visible",
        sourceRecordType: "Rendered HTML",
        sourceRecordId: null,
        sourceFieldName: null,
        status: "unsupported_metric",
        recommendedAction: "suppress_component",
        proposedPatch: null,
        reason: "Visible 0 metric without confirmed official zero.",
      },
    ];
  }
  return [];
}

function scanEmptyBullets(brandSlug, html) {
  const n = (html.match(/<li>\s*(?:&nbsp;)?\s*<\/li>/gi) || []).length;
  if (!n) return [];
  return [
    {
      brandSlug,
      tabName: "Overview",
      sectionName: "Rendered lists",
      componentType: "bullet_list",
      componentLabel: "Empty bullets",
      fieldName: "html.empty_bullets",
      currentRenderedValue: `${n} empty <li>`,
      sourceRecordType: "Rendered HTML",
      sourceRecordId: null,
      sourceFieldName: null,
      status: "blank",
      recommendedAction: "rewrite_owner_copy",
      proposedPatch: null,
      reason: "Visible empty bullet rows in rendered HTML.",
    },
  ];
}

function summarizeFindings(findings) {
  const isResolved = (f) =>
    f.status === "pass" ||
    f.status === "should_suppress" ||
    f.status === "cleanly_unavailable" ||
    f.recommendedAction === "suppress_component";
  const fail = findings.filter((f) => !isResolved(f));
  const byStatus = {};
  for (const f of findings) byStatus[f.status] = (byStatus[f.status] || 0) + 1;
  return {
    totalVisibleFieldsAudited: findings.length,
    totalPass: findings.filter(isResolved).length,
    totalFail: fail.length,
    totalSuppressionNeeded: findings.filter((f) => f.recommendedAction === "suppress_component").length,
    totalRewriteNeeded: findings.filter((f) =>
      ["rewrite_owner_copy", "add_body", "add_case_summary", "fill_from_source"].includes(f.recommendedAction)
    ).length,
    totalMetricHandlingDefects: findings.filter((f) =>
      ["unsupported_metric", "misleading"].includes(f.status)
    ).length,
    totalImageDistinctivenessDefects: findings.filter((f) => f.status === "duplicate").length,
    totalMissingModalBodyDefects: findings.filter((f) =>
      ["add_case_summary", "add_body"].includes(f.recommendedAction)
    ).length,
    byStatus,
  };
}

function findingHasResolutionPath(f) {
  if (f.status === "pass") return true;
  if (f.proposedPatch) return true;
  if (["intentionally_suppressed", "cleanly_unavailable", "should_suppress"].includes(f.status)) return true;
  if (["suppress_component", "mark_unavailable"].includes(f.recommendedAction)) return true;
  return false;
}

/**
 * Release-quality decision for a brand audit result.
 * - field_complete: failFindings === 0
 * - field_complete_after_patch: fails remain but every fail has a resolution/patch path
 * - not_field_complete: fails without a resolution path
 *
 * Note: field_complete_after_patch is NOT an auditPass.
 */
function releaseDecision(summary, findings = []) {
  if (summary.totalFail === 0) return "field_complete";
  const fail = findings.filter((f) => f.status !== "pass");
  const allResolved = fail.every(findingHasResolutionPath);
  return allResolved ? "field_complete_after_patch" : "not_field_complete";
}

function completenessFlags(findings = [], summary) {
  const fail = findings.filter((f) => f.status !== "pass");
  const patchPlanComplete = fail.every(findingHasResolutionPath);
  return {
    auditComplete: true,
    patchPlanComplete,
    /** Strict release gate: zero fail findings in the rendered payload. */
    auditPass: summary.totalFail === 0,
    failFindings: summary.totalFail,
  };
}

function attachProposedPatches(findings, brandSlug, rows) {
  const packRows = [
    ...(RENDERED_FIELD_REMEDIATION_CONTENT[brandSlug]?.presentation || []),
    ...(TAB_FACTORY_REMEDIATION_CONTENT[brandSlug]?.presentation || []),
  ];
  for (const f of findings) {
    if (f.status === "pass" || f.proposedPatch) continue;
    const packRow = packRows.find(
      (p) => p.slotKey === f.sourceFieldName || p.slotKey === f.fieldId
    );
    if (packRow) {
      const existing = findSlot(rows, packRow.slotKey);
      f.proposedPatch = {
        table: "Brand Setup - Brand Explorer Presentation",
        action: existing?.recordId ? "PATCH" : "POST",
        recordId: existing?.recordId || null,
        slotKey: packRow.slotKey,
        fields: {
          Title: packRow.title || "",
          Body: packRow.body,
          "Sort Order": packRow.sortOrder ?? 0,
          ...(existing?.recordId
            ? {}
            : {
                "Slot Key": packRow.slotKey,
                Active: true,
              }),
        },
      };
      if (!f.recommendedAction || f.recommendedAction === "no_action") {
        f.recommendedAction = "add_body";
      }
    } else if (
      ["replace_with_not_publicly_disclosed", "suppress_component", "mark_unavailable"].includes(
        f.recommendedAction
      )
    ) {
      f.proposedPatch = {
        handling: f.recommendedAction,
        note:
          f.recommendedAction === "suppress_component"
            ? "Suppress this rendered component until steward-verified data exists."
            : "Show Not publicly disclosed (or equivalent) where the UI cannot suppress the empty card.",
        fieldId: f.fieldId,
      };
    } else if (f.status === "wrong_geography_label" && f.sourceRecordId) {
      const opening = rows.find((r) => r.recordId === f.sourceRecordId);
      if (opening) {
        const fixedTitle = nz(opening.title).replace(/CALA Property Example/gi, "International Reference Example");
        f.proposedPatch = {
          table: "Brand Setup - Brand Explorer Presentation",
          action: "PATCH",
          recordId: opening.recordId,
          slotKey: "footprint.openings",
          fields: { Title: fixedTitle },
        };
      }
    } else if (f.fieldId === "overview.featured_application") {
      const pack = RENDERED_FIELD_REMEDIATION_CONTENT[brandSlug];
      const featuredExtra = pack?.featuredApplication;
      if (featuredExtra) {
        const existing = findSlot(rows, "overview.featured_application");
        f.proposedPatch = {
          table: "Brand Setup - Brand Explorer Presentation",
          action: existing?.recordId ? "PATCH" : "POST",
          recordId: existing?.recordId || null,
          slotKey: "overview.featured_application",
          fields: { Title: featuredExtra.title || existing?.title || "", Body: featuredExtra.body },
        };
      }
    }
  }
}

/**
 * Sync completeness evaluation from an already-loaded Brand Library payload.
 * Used by OS / factory gates so every setup runs the same checks.
 */
export function evaluateRenderedFieldCompletenessFromPayload(
  brand,
  rows = [],
  html = "",
  brandSlug = null,
  { includeHtmlScans = true } = {}
) {
  const slug = brandSlug || brand?.slug || brand?.brandSlug || "";
  const findings = [];

  for (const field of ALL_INVENTORY_FIELDS) {
    findings.push(evaluateInventoryField(field, brand, rows, html, slug));
  }
  findings.push(...scanOpenings(slug, rows, html));
  findings.push(...scanScenarioImageDistinctiveness(slug, rows));
  if (includeHtmlScans) {
    findings.push(...scanMisleadingZeros(slug, html));
    findings.push(...scanEmptyBullets(slug, html));
  }

  attachProposedPatches(findings, slug, rows);

  const golden = evaluateGoldenContentQuality(brand, rows, html, { brandSlug: slug });
  const summary = summarizeFindings(findings);
  const flags = completenessFlags(findings, summary);
  const decision = releaseDecision(summary, findings);

  return {
    brandSlug: slug,
    brandName: brand?.name || slug,
    liveState: {
      displayState: brand?.brandExplorerDisplayState,
      shouldRenderFullProfile: brand?.shouldRenderFullProfile === true,
    },
    summary,
    ...flags,
    releaseQualityDecision: decision,
    goldenContentQualityPass: golden.pass === true,
    goldenFailures: golden.failures || [],
    findings,
    patchPlan: findings.filter((f) => f.proposedPatch).map((f) => f.proposedPatch),
  };
}

export function evaluateRenderedFieldCompletenessForTest(brandResult) {
  const failures = [];
  for (const f of brandResult.findings || []) {
    if (f.status !== "pass") {
      failures.push(`${f.fieldName}:${f.status}`);
    }
  }
  if (brandResult.auditPass === false && failures.length === 0) {
    failures.push("auditPass_false");
  }
  return {
    pass: failures.length === 0 && brandResult.auditPass !== false,
    failures,
    auditComplete: brandResult.auditComplete === true,
    patchPlanComplete: brandResult.patchPlanComplete === true,
    auditPass: brandResult.auditPass === true,
    failFindings: brandResult.failFindings ?? failures.length,
  };
}
