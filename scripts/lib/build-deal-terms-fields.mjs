/**
 * Build Airtable Deal Terms field payload from a profile + live Meta choices.
 */
import { DEAL_TERMS_FORM_SELECT_OPTIONS } from "./deal-terms-field-contract.mjs";
import {
  readFddText,
  parseChoiceFddDealTerms,
} from "./parse-choice-fdd-item17-deal-terms.mjs";

export function pickSelect(choices, preferred, formFallbackList, proposals, brandName, column) {
  if (preferred === null || preferred === undefined || preferred === "") return null;
  const allowed = Array.isArray(choices) && choices.length ? choices : formFallbackList || [];
  const exact = allowed.find(
    (c) => String(c).trim().toLowerCase() === String(preferred).trim().toLowerCase()
  );
  if (exact) return exact;
  // Allow Meta-only choices (e.g. Kimpton renewal wording) when preferred matches Meta.
  if (Array.isArray(choices) && choices.length) {
    const metaExact = choices.find(
      (c) => String(c).trim().toLowerCase() === String(preferred).trim().toLowerCase()
    );
    if (metaExact) return metaExact;
  }
  const formOpts = formFallbackList || [];
  const formExact = formOpts.find(
    (c) => String(c).trim().toLowerCase() === String(preferred).trim().toLowerCase()
  );
  if (formExact && allowed.some((c) => String(c).trim().toLowerCase() === formExact.toLowerCase())) {
    return allowed.find((c) => String(c).trim().toLowerCase() === formExact.toLowerCase());
  }
  if (Array.isArray(proposals)) {
    proposals.push({
      brandName,
      column,
      preferred,
      fallback: null,
      note: "Preferred value not in Meta choices; field omitted.",
    });
  }
  return null;
}

/**
 * @param {object} profile
 * @param {Record<string, string[]>} metaChoices
 * @param {Array<object>} [proposals]
 * @param {string} brandName
 */
export function buildDealTermsFieldsFromProfile(profile, metaChoices, proposals = [], brandName = "") {
  const sel = (col, want) =>
    pickSelect(metaChoices[col], want, DEAL_TERMS_FORM_SELECT_OPTIONS[col], proposals, brandName, col);

  let initialYears = profile.initialYears ?? 20;
  let noRenewal = Boolean(profile.noRenewal);

  if (profile.fddFile) {
    const text = readFddText(profile.fddFile);
    if (text) {
      const parsed = parseChoiceFddDealTerms(text);
      if (parsed.initialYears != null) initialYears = parsed.initialYears;
      if (parsed.noRenewal) noRenewal = true;
    }
  }

  const fields = {
    "Quantity - Typical Minimum Initial Term": "1",
    "Length - Typical Minimum Initial Term": String(initialYears),
    "Duration - Typical Minimum Initial Term": sel(
      "Duration - Typical Minimum Initial Term",
      "Year(s)"
    ),
    "Renewal Structure": sel("Renewal Structure", profile.renewalStructure),
    "Renewal Notice Responsibility": sel(
      "Renewal Notice Responsibility",
      profile.renewalNoticeResponsibility
    ),
    "Typical Renewal Conditions": profile.renewalConditions,
    "Length - Typical Renewal Notice Period": String(profile.noticeMonths ?? 12),
    "Quantity - Typical Renewal Notice Period": sel(
      "Quantity - Typical Renewal Notice Period",
      "Month(s)"
    ),
    "Performance Test Requirement": sel(
      "Performance Test Requirement",
      profile.performanceTestRequirement
    ),
    "Typical Cure Period for Performance Test Failure": profile.curePeriodText,
    "Duration - Typical Cure Period for Performance Test Failure": sel(
      "Duration - Typical Cure Period for Performance Test Failure",
      profile.curePeriodDuration
    ),
    "Typical QA": sel("Typical QA", profile.qa),
    "Mandatory PIP at Renewal": sel("Mandatory PIP at Renewal", profile.pipAtRenewal),
    "Mandatory PIP for Conversions": sel(
      "Mandatory PIP for Conversions",
      profile.pipForConversions
    ),
    "Typical Termination Fee Structure (if any)": sel(
      "Typical Termination Fee Structure (if any)",
      profile.terminationFeeStructure
    ),
    "Typical Termination Fee Structure (if any) Text": profile.terminationFeeNotes,
    "Who Can Exercise Termination Right After Failed Test?": sel(
      "Who Can Exercise Termination Right After Failed Test?",
      profile.performanceTerminationRights
    ),
  };

  if (profile.pipUsdPerRoom != null) {
    fields["Typical Mandatory PIP for Conversions ($/room)"] = profile.pipUsdPerRoom;
  } else if (
    profile.cohort === "membership" ||
    profile.pipForConversions === "No"
  ) {
    // Explicit clear — leftover franchise PIP estimates are wrong for membership networks.
    fields["Typical Mandatory PIP for Conversions ($/room)"] = null;
  }

  if (profile.conversionMaxMonths != null) {
    fields["Conversion - Typical max time allowed for completion"] = String(
      profile.conversionMaxMonths
    );
    fields["Conversion - Typical max time allowed for completion -Duration"] = sel(
      "Conversion - Typical max time allowed for completion -Duration",
      "Month(s)"
    );
  }

  if (profile.renewalMaxMonths != null) {
    fields["Renewal - Typical max time allowed for completion"] = String(profile.renewalMaxMonths);
    fields["Renewal - Typical max time allowed for completion -Duration"] = sel(
      "Renewal - Typical max time allowed for completion -Duration",
      "Month(s)"
    );
  }

  if (!noRenewal && (profile.renewalOptionQty == null || profile.renewalOptionQty > 0)) {
    fields["Quantity - Typical Renewal Option"] = String(profile.renewalOptionQty ?? 1);
    fields["Length - Typical Renewal Option"] = String(profile.renewalYears ?? 10);
    fields["Duration - Typical Renewal Option"] = sel(
      "Duration - Typical Renewal Option",
      "Year(s)"
    );
  } else if (noRenewal || profile.renewalOptionQty === 0) {
    fields["Quantity - Typical Renewal Option"] = "0";
    fields["Length - Typical Renewal Option"] = "0";
    fields["Duration - Typical Renewal Option"] = sel(
      "Duration - Typical Renewal Option",
      "Year(s)"
    );
  }

  for (const k of Object.keys(fields)) {
    if (fields[k] === undefined || fields[k] === "") delete fields[k];
    // keep null — clears Airtable cell
  }

  return {
    fields,
    resolved: {
      initialYears,
      noRenewal,
      sourceTier: profile.sourceTier,
      cohort: profile.cohort,
    },
  };
}

/** Normalize for equality compare. */
export function normalizeDealTermsCompare(v) {
  if (v === undefined || v === null) return "";
  if (typeof v === "number") return String(v);
  return String(v).replace(/\s+/g, " ").trim();
}

/**
 * Diff expected vs existing. Returns mismatch list.
 * Soft-compare long text: if existing contains key FDD facts, may still flag full-string diffs.
 */
export function diffDealTermsFields(expected, existingFields) {
  const mismatches = [];
  for (const [col, want] of Object.entries(expected)) {
    const cur = existingFields?.[col];
    if (want === null) {
      if (cur !== undefined && cur !== null && cur !== "") {
        mismatches.push({ column: col, expected: null, actual: cur });
      }
      continue;
    }
    const a = normalizeDealTermsCompare(want);
    const b = normalizeDealTermsCompare(cur);
    if (a === b) continue;
    if (col.includes("$/room") && a !== "" && b !== "") {
      const na = Number(a);
      const nb = Number(b);
      if (Number.isFinite(na) && Number.isFinite(nb) && Math.abs(na - nb) < 0.5) continue;
    }
    mismatches.push({ column: col, expected: want, actual: cur ?? null });
  }
  return mismatches;
}
