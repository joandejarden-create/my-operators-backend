/**
 * Packet 2.7-R2 — Property-neutral Full Hotel Intelligence Investigation quality contract.
 * Never requires a specific hotel name (no KGPV identity guard).
 */

export const FULL_HI_REQUIRED_CHAPTER_IDS = Object.freeze([
  "property_identity",
  "ownership_chain_propco",
  "operator_management",
  "brand_reflag",
  "property_history",
  "organization_portfolio",
  "people_decision_authority",
  "transactions_capital",
  "commercial_pursuit",
  "open_questions",
  "sources_evidence",
]);

export const FULL_HI_QUALITY_DEFAULTS = Object.freeze({
  minSourceCount: 12,
  minKeyFindings: 4,
  minSections: 11,
  /** Soft floor — multidimensional evaluator, not a magic pass alone. */
  minSubstantiveWords: 3500,
  /** Target band for Golden Demo full investigations when corpus supports it. */
  targetSubstantiveWordsMin: 4000,
  targetSubstantiveWordsMax: 7000,
  minChapterWordFloor: 40,
});

function countWords(text) {
  return String(text || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

export function countDossierSubstantiveWords(dossier) {
  if (!dossier) return 0;
  if (Number(dossier.substantive_word_count) > 0) return Number(dossier.substantive_word_count);
  const blob = JSON.stringify({
    executive_summary: dossier.executive_summary,
    key_findings: dossier.key_findings,
    sections: dossier.sections,
    appendices: dossier.appendices,
  });
  return countWords(blob);
}

function sectionWordCount(sec) {
  let n = 0;
  for (const b of sec?.blocks || []) {
    if (b.type === "paragraphs") n += countWords((b.paragraphs || []).join(" "));
    else if (b.type === "list") n += countWords((b.items || []).join(" "));
    else if (b.type === "heading") n += countWords(b.text);
    else if (b.type === "table") {
      n += countWords((b.headers || []).join(" "));
      for (const row of b.rows || []) n += countWords((row || []).join(" "));
    } else n += countWords(JSON.stringify(b));
  }
  return n;
}

/**
 * Hotel-match guard: requested ids must match research/report identity.
 * @param {object} dossier
 * @param {object} [expected] { hotelIds: string[], hotelNames?: RegExp|string }
 */
export function assertReportHotelMatch(dossier, expected = {}) {
  const errors = [];
  const ids = new Set(
    [dossier?.hotel_id, dossier?.hotel_airtable_record_id, dossier?.canonical_hotel_id]
      .filter(Boolean)
      .map((x) => String(x).trim())
  );
  const expectedIds = (expected.hotelIds || expected.expectedHotelIds || [])
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  if (expectedIds.length) {
    const hit = expectedIds.some((id) => ids.has(id));
    if (!hit) {
      errors.push(
        `report_hotel_mismatch:expected=${expectedIds.join("|")};got=${[...ids].join("|") || "none"}`
      );
    }
  }
  if (expected.forbidHotelIds?.length) {
    for (const bad of expected.forbidHotelIds) {
      if (ids.has(String(bad))) errors.push(`forbidden_hotel_id_present:${bad}`);
    }
  }
  const name = String(dossier?.hotel_name || "").trim();
  if (expected.hotelNameIncludes) {
    const re =
      expected.hotelNameIncludes instanceof RegExp
        ? expected.hotelNameIncludes
        : new RegExp(String(expected.hotelNameIncludes), "i");
    if (!re.test(name)) errors.push("report_hotel_name_mismatch");
  }
  if (expected.forbidNamePatterns?.length) {
    for (const pat of expected.forbidNamePatterns) {
      const re = pat instanceof RegExp ? pat : new RegExp(String(pat), "i");
      if (re.test(name)) errors.push(`forbidden_hotel_name_pattern:${re}`);
    }
  }
  return { ok: errors.length === 0, errors, hotelIds: [...ids], hotelName: name };
}

/**
 * Multidimensional Full Investigation quality evaluation (property-neutral).
 */
export function evaluateFullInvestigationQuality(dossier, options = {}) {
  const cfg = { ...FULL_HI_QUALITY_DEFAULTS, ...(options.thresholds || {}) };
  const errors = [];
  const dimensions = {};

  if (!dossier || typeof dossier !== "object") {
    return { ok: false, errors: ["dossier_missing"], dimensions: {}, wordCount: 0 };
  }

  dimensions.IDENTITY_PRESENT = Boolean(
    dossier.hotel_name &&
      dossier.hotel_name !== "Hotel" &&
      (dossier.hotel_id || dossier.hotel_airtable_record_id)
  );
  if (!dimensions.IDENTITY_PRESENT) errors.push("identity_incomplete");

  if (dossier.dossier_type !== "FULL_HOTEL_INTELLIGENCE_INVESTIGATION") {
    errors.push("dossier_type_invalid");
    dimensions.DOSSIER_TYPE_VALID = false;
  } else {
    dimensions.DOSSIER_TYPE_VALID = true;
  }

  const match = assertReportHotelMatch(dossier, options);
  dimensions.REPORT_HOTEL_MATCHES_REQUEST = match.ok || !options.hotelIds?.length;
  if (options.hotelIds?.length || options.expectedHotelIds?.length) {
    if (!match.ok) errors.push(...match.errors);
  }

  const exec = dossier.executive_summary?.paragraphs || [];
  dimensions.EXECUTIVE_SUMMARY_PRESENT = exec.length >= 3;
  if (!dimensions.EXECUTIVE_SUMMARY_PRESENT) errors.push("executive_summary_thin");

  const findings = dossier.key_findings || [];
  dimensions.SUBSTANTIVE_FINDINGS_PRESENT = findings.length >= cfg.minKeyFindings;
  if (!dimensions.SUBSTANTIVE_FINDINGS_PRESENT) errors.push("key_findings_low");

  const sourceCount = Number(dossier.source_count || (dossier.sources || []).length || 0);
  dimensions.SOURCE_COUNT_MINIMUM = sourceCount >= cfg.minSourceCount;
  if (!dimensions.SOURCE_COUNT_MINIMUM) errors.push(`source_count_low:${sourceCount}`);

  const sections = dossier.sections || [];
  dimensions.SECTIONS_PRESENT = sections.length >= cfg.minSections;
  if (!dimensions.SECTIONS_PRESENT) errors.push("sections_incomplete");

  const chapterCoverage = {};
  for (const id of FULL_HI_REQUIRED_CHAPTER_IDS) {
    const sec = sections.find((s) => s && s.id === id);
    const words = sec ? sectionWordCount(sec) : 0;
    chapterCoverage[id] = { present: Boolean(sec), words, nonEmpty: words >= cfg.minChapterWordFloor };
    if (!sec) errors.push(`missing_section:${id}`);
    else if (words < cfg.minChapterWordFloor) errors.push(`thin_section:${id}:${words}`);
  }
  dimensions.OWNERSHIP_SECTION_PRESENT = chapterCoverage.ownership_chain_propco?.nonEmpty;
  dimensions.OPERATOR_SECTION_PRESENT = chapterCoverage.operator_management?.nonEmpty;
  dimensions.DEVELOPMENT_SECTION_PRESENT = chapterCoverage.property_history?.nonEmpty;
  dimensions.PEOPLE_SECTION_PRESENT = chapterCoverage.people_decision_authority?.nonEmpty;
  dimensions.SOURCES_PRESENT = chapterCoverage.sources_evidence?.nonEmpty;
  dimensions.OPEN_QUESTIONS_PRESENT_OR_EXPLICIT_NONE =
    chapterCoverage.open_questions?.present &&
    ((dossier.open_questions || []).length > 0 || chapterCoverage.open_questions.words >= 20);

  const words = countDossierSubstantiveWords(dossier);
  dimensions.MINIMUM_SUBSTANTIVE_DEPTH = words >= cfg.minSubstantiveWords;
  if (!dimensions.MINIMUM_SUBSTANTIVE_DEPTH) {
    errors.push(`substantive_word_count_below_${cfg.minSubstantiveWords}:${words}`);
  }

  // Citation integrity: only treat small integer cite markers as bibliography refs
  const citeNums = new Set();
  for (const kf of findings) {
    for (const n of kf.citations || []) {
      const num = Number(n);
      if (Number.isFinite(num) && num > 0 && num <= 500) citeNums.add(num);
    }
  }
  const maxCite = citeNums.size ? Math.max(...citeNums) : 0;
  dimensions.CITATION_INTEGRITY = maxCite === 0 || maxCite <= Math.max(sourceCount, 1);
  if (!dimensions.CITATION_INTEGRITY) errors.push(`citation_out_of_range:max=${maxCite};sources=${sourceCount}`);

  // Contaminant scan (optional)
  if (options.forbidCustomerContentPatterns?.length) {
    const blob = JSON.stringify({
      hotel_name: dossier.hotel_name,
      executive_summary: dossier.executive_summary,
      key_findings: dossier.key_findings,
      sections: dossier.sections,
    });
    for (const pat of options.forbidCustomerContentPatterns) {
      const re = pat instanceof RegExp ? pat : new RegExp(String(pat), "i");
      if (re.test(blob)) errors.push(`forbidden_content_pattern:${re}`);
    }
  }

  const criticalFail = errors.some((e) =>
    /dossier_missing|dossier_type_invalid|identity_incomplete|report_hotel_mismatch|forbidden_hotel|missing_section|substantive_word_count|forbidden_content/.test(
      e
    )
  );

  return {
    ok: errors.length === 0,
    soft_ok: !criticalFail && dimensions.MINIMUM_SUBSTANTIVE_DEPTH && dimensions.OWNERSHIP_SECTION_PRESENT,
    errors,
    dimensions,
    chapterCoverage,
    wordCount: words,
    sourceCount,
    hotelName: dossier.hotel_name,
    thresholds: cfg,
  };
}
