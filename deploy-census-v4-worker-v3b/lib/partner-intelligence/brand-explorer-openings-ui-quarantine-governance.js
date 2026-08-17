/**
 * Brand Explorer Openings / Examples UI quarantine governance v31C.
 *
 * Detects internal source-capture language and unsafe active display for
 * expansion-brand footprint openings, momentum, and related scenario slots.
 */

export const QUARANTINE_GOVERNANCE_VERSION = "31C";

/** Slots audited for Openings / Examples / Conversion Example surfaces. */
export const OPENINGS_EVIDENCE_SLOT_RE =
  /^(footprint\.openings|footprint\.momentum|overview\.scenario\.\d|valueOwners\.scenario\.\d)$/;

export const INTERNAL_UI_LANGUAGE_MARKERS = [
  { id: "census_property_url", re: /\bcensus property url\b/i, severity: "high" },
  { id: "census_url_extract", re: /\bcensus url extract\b|\bdealality census\b/i, severity: "high" },
  { id: "item_19_performance", re: /\bitem\s*19\b/i, severity: "high" },
  { id: "source_data_label", re: /\bsource data\b/i, severity: "high" },
  { id: "metadata_label", re: /\bmetadata\b/i, severity: "medium" },
  { id: "consumer_site_label", re: /\bconsumer site\b|\bconsumer-site\b/i, severity: "medium" },
  { id: "active_property_page", re: /\bactive property page\b|\bactive choice hotels property page\b/i, severity: "high" },
  { id: "confirm_fees_fdd", re: /\bconfirm flag, fees, and opening status\b|\bconfirm flag, fees\b|\bin your loi and fdd\b/i, severity: "high" },
  { id: "internal_label", re: /\binternal extraction\b|\binternal\b/i, severity: "medium" },
  { id: "extraction_label", re: /\bextraction\b/i, severity: "medium" },
  { id: "fdd_label", re: /\bfdd\b|\bfranchise disclosure document\b/i, severity: "high" },
  {
    id: "gateway_cala_capture",
    re: /\bcala footprint\s*·\s*census property url\b|\bgateway cala\b.*\bcensus\b/i,
    severity: "medium",
  },
  { id: "listed_on_choicehotels", re: /\blisted on choicehotels\.com\b/i, severity: "low" },
  { id: "choice_affiliated_listed", re: /\bchoice-affiliated\s*·\s*listed on choicehotels/i, severity: "high" },
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

export function isOpeningsEvidenceSlot(slotKey) {
  return OPENINGS_EVIDENCE_SLOT_RE.test(nz(slotKey));
}

export function collectRowCopySurfaces(row) {
  const fields = row?.fields || {};
  return [
    { field: "Title", value: nz(row?.title ?? fields.Title) },
    { field: "Body", value: nz(row?.body ?? fields.Body) },
    { field: "Case Summary Overview", value: nz(row?.caseSummaryOverview ?? fields["Case Summary Overview"]) },
    {
      field: "Case Summary Owner Objective",
      value: nz(row?.caseSummaryOwnerObjective ?? fields["Case Summary Owner Objective"]),
    },
    {
      field: "Case Summary Brand Relevance",
      value: nz(row?.caseSummaryBrandRelevance ?? fields["Case Summary Brand Relevance"]),
    },
    {
      field: "Case Summary Interpretation",
      value: nz(row?.caseSummaryInterpretation ?? fields["Case Summary Interpretation"]),
    },
    { field: "Case Summary Tags", value: nz(row?.caseSummaryTags ?? fields["Case Summary Tags"]) },
    { field: "Summary URL", value: nz(row?.summaryUrl ?? fields["Summary URL"]) },
  ].filter((s) => hasVal(s.value));
}

export function detectInternalUiLanguage(haystack) {
  const text = nz(haystack);
  if (!text) return [];
  const hits = [];
  for (const marker of INTERNAL_UI_LANGUAGE_MARKERS) {
    if (marker.re.test(text)) {
      hits.push({
        markerId: marker.id,
        severity: marker.severity || "high",
        excerpt: text.slice(0, 160),
      });
    }
  }
  return hits;
}

export function findInternalLanguageInRow(row) {
  const surfaces = collectRowCopySurfaces(row);
  const findings = [];
  for (const surface of surfaces) {
    for (const hit of detectInternalUiLanguage(surface.value)) {
      findings.push({ ...hit, field: surface.field });
    }
  }
  return findings;
}

export function parseFootprintOpeningLocation(title, body) {
  const titleMatch = nz(title).match(/—\s*([^,]+(?:,\s*[^,]+)?)\s*$/);
  if (titleMatch) return titleMatch[1].trim();
  const paras = nz(body).split(/\n\n+/).map((p) => p.trim()).filter(Boolean);
  if (paras.length >= 2) return paras[1];
  return "";
}

export function proposeOwnerFacingOpeningsCopy({ title, body, summaryUrl }) {
  const location = parseFootprintOpeningLocation(title, body);
  const city = location.split(",")[0]?.trim() || "this market";
  const country = location.includes(",") ? location.split(",").slice(-1)[0].trim() : "";
  const locLine = country ? `${city}, ${country}` : city;
  const propertyName = nz(title).replace(/^Radisson Individuals?\s*—\s*/i, "").trim() || locLine;

  const tags = "CALA, Soft collection, Portfolio example";
  const meta = "Radisson Individuals · Choice-family listing";
  const scenario = country ? `${country.toUpperCase()} MARKET EXAMPLE` : "CALA MARKET EXAMPLE";
  const teaser = `${propertyName} illustrates how Radisson Individuals positions a hand-selected independent or boutique asset within Choice-family distribution—use as a market context example, not a performance guarantee.`;
  const repairedBody = [tags, locLine, meta, scenario, teaser, summaryUrl].filter(Boolean).join("\n\n");

  const caseSummaryOverview = `${locLine}: portfolio example for Radisson Individuals by Choice within the Choice Hotels CALA corridor.`;
  const caseSummaryOwnerObjective =
    "Owner consideration: confirm current flag, standards, commercial terms, and operating status directly before underwriting.";
  const caseSummaryBrandRelevance =
    "Shows published Choice-family Individuals inventory in this market—useful for collection-tier positioning context, not comp-set proof.";
  const caseSummaryInterpretation =
    "Treat as a market and brand-fit reference only; validate economics, PIP scope, and member terms locally.";
  const caseSummaryTags = `CALA, ${city}, Individuals, Portfolio example`;

  for (const text of [
    repairedBody,
    caseSummaryOverview,
    caseSummaryOwnerObjective,
    caseSummaryBrandRelevance,
    caseSummaryInterpretation,
  ]) {
    if (detectInternalUiLanguage(text).length) {
      throw new Error("Proposed owner-facing copy failed internal-language guardrail");
    }
  }

  return {
    body: repairedBody,
    caseSummaryOverview,
    caseSummaryOwnerObjective,
    caseSummaryBrandRelevance,
    caseSummaryInterpretation,
    caseSummaryTags,
  };
}

/**
 * @param {object} row - presentation row (API block or Airtable row shape)
 * @param {object} imageAssessment - from brand-explorer-brand-asset-image-governance
 * @param {object|null} registryMatch - matched registry asset
 */
export function assessOpeningsRowQuarantine(row, imageAssessment, registryMatch) {
  const slotKey = nz(row?.slotKey);
  if (!isOpeningsEvidenceSlot(slotKey)) return null;

  const internalLanguage = findInternalLanguageInRow(row);
  const hasImage = Boolean(imageAssessment?.hasImage ?? row?.hasImage ?? row?.imageUrl);
  const registryDoNotUse =
    nz(registryMatch?.explorerUsePermission) === "Do Not Use" ||
    nz(registryMatch?.assetStatus) === "Do Not Use";
  const registryPending =
    !registryMatch ||
    nz(registryMatch?.explorerUsePermission) === "Candidate Only" ||
    nz(registryMatch?.usageReviewStatus) === "Pending Review" ||
    nz(registryMatch?.usageReviewStatus) === "Not Reviewed";
  const wrongBrand = Boolean(imageAssessment?.wrongBrandRisk);
  const imageUnsafe =
    wrongBrand ||
    registryDoNotUse ||
    (hasImage && registryPending) ||
    (/footprint\.openings/.test(slotKey) && !hasImage);

  let recommendation = "remain_active";
  if (imageUnsafe || internalLanguage.length > 0) {
    if (imageUnsafe) recommendation = "suppress_and_quarantine";
    else if (/footprint\.openings/.test(slotKey)) recommendation = "suppress_or_repair_copy";
    else recommendation = "suppress_and_quarantine";
  }

  const safeForActiveDisplay =
    recommendation === "remain_active";

  return {
    recordId: row?.recordId || row?.presentationRowId || null,
    slotKey,
    title: nz(row?.title),
    visibleHeading: nz(row?.title),
    location: parseFootprintOpeningLocation(row?.title, row?.body),
    imageStatus: hasImage ? (registryDoNotUse || wrongBrand ? "unsafe" : "unapproved") : "missing",
    registryRecordId: registryMatch?.id || imageAssessment?.registryRecordId || null,
    registryApproved: Boolean(imageAssessment?.registryApproved),
    internalLanguageHits: internalLanguage,
    wrongBrandRisk: imageAssessment?.wrongBrandRisk || null,
    imageUnsafe,
    safeForActiveDisplay,
    recommendation,
    suppressionAction:
      imageUnsafe || internalLanguage.length > 0 || !safeForActiveDisplay
        ? "set_display_status_do_not_display"
        : null,
    clearImage: Boolean(hasImage && (registryDoNotUse || wrongBrand)),
    copyRepairEligible:
      !imageUnsafe && internalLanguage.length > 0 && /footprint\.openings/.test(slotKey),
  };
}

export function detectOpeningsUiQuarantineDefects(rows, assessments, brandTarget) {
  if (nz(brandTarget?.resolution?.resolutionSource) !== "expansion_backlog") return [];
  const defects = [];
  for (const assessment of assessments) {
    if (!assessment || assessment.safeForActiveDisplay) continue;
    const severity =
      assessment.imageUnsafe && assessment.wrongBrandRisk
        ? "critical"
        : assessment.imageUnsafe
          ? "high"
          : "high";
    defects.push({
      type: assessment.imageUnsafe ? "openings_unsafe_image" : "openings_internal_language",
      severity,
      category: "data",
      surface: `presentation.${assessment.slotKey}`,
      recordId: assessment.recordId,
      slotKey: assessment.slotKey,
      message: assessment.imageUnsafe
        ? "Openings / Examples row has unapproved, missing, or wrong-brand image — must be quarantined from active UI."
        : "Openings / Examples row contains internal source-capture language visible to owners.",
      recommendedFixBatch: "v31C_radisson_individuals_openings_suppression",
      internalMarkers: (assessment.internalLanguageHits || []).map((h) => h.markerId),
    });
  }
  return defects;
}

export function openingsFailActiveProfileQuarantineGate(assessments) {
  return (assessments || []).some((a) => a && !a.safeForActiveDisplay && isOpeningsEvidenceSlot(a.slotKey));
}
