/**
 * DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1
 *
 * Presentation-only telephone formatting. Never mutates storage, providers,
 * verification, or contact identity. Uses libphonenumber-js exclusively.
 *
 * Docs: https://github.com/catamphetamine/libphonenumber-js
 * Library: libphonenumber-js@1.13.17
 */
import { parsePhoneNumberFromString } from "libphonenumber-js";

export const CONTRACT_ID = "DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1";
export const LIBRARY_ID = "libphonenumber-js";
export const LIBRARY_VERSION = "1.13.17";

/**
 * @typedef {object} DealalityPhoneFormatResult
 * @property {string|null} originalRaw
 * @property {string} displayValue
 * @property {string|null} e164
 * @property {string|null} telHref
 * @property {string|null} extension
 * @property {string|null} country
 * @property {string|null} countryCallingCode
 * @property {boolean} isValid
 * @property {boolean} isPossible
 * @property {'FORMATTED'|'PRESERVED_ORIGINAL'|'EMPTY'} formattingOutcome
 * @property {'PARSED'|'PARSE_FAILED'|'EMPTY'} parsingOutcome
 * @property {string|null} parseError
 */

function text(v) {
  if (v == null) return "";
  return String(v).trim();
}

/**
 * NANP (+1): "+1 " + national format → "+1 (267) 475-4267"
 * Other plans: library international format.
 * @param {import('libphonenumber-js').PhoneNumber} phone
 */
export function formatDisplayFromParsed(phone) {
  if (!phone) return "";
  const cc = String(phone.countryCallingCode || "");
  if (cc === "1") {
    const national = phone.formatNational();
    return national ? `+1 ${national}` : phone.formatInternational();
  }
  return phone.formatInternational();
}

/**
 * Format a telephone value for Dealality customer display.
 *
 * @param {unknown} raw
 * @param {{ defaultCountry?: string|null, contactCountry?: string|null }=} opts
 *   defaultCountry / contactCountry — only used when raw has no explicit +
 *   calling code. Never invent from hotel market. Prefer contact-level country.
 * @returns {DealalityPhoneFormatResult}
 */
export function formatDealalityPhone(raw, opts = {}) {
  const originalRaw = raw == null ? null : String(raw);
  const trimmed = text(raw);
  if (!trimmed) {
    return {
      originalRaw: originalRaw === "" ? "" : null,
      displayValue: "",
      e164: null,
      telHref: null,
      extension: null,
      country: null,
      countryCallingCode: null,
      isValid: false,
      isPossible: false,
      formattingOutcome: "EMPTY",
      parsingOutcome: "EMPTY",
      parseError: null,
    };
  }

  const defaultCountry =
    text(opts.contactCountry || opts.defaultCountry || "").toUpperCase() ||
    undefined;

  // Never pass a default country when the value already has an explicit +.
  const parseOpts =
    trimmed.charAt(0) === "+"
      ? undefined
      : defaultCountry
        ? { defaultCountry }
        : undefined;

  let phone = null;
  let parseError = null;
  try {
    phone = parsePhoneNumberFromString(trimmed, parseOpts);
  } catch (err) {
    parseError = String(err && err.message ? err.message : err);
    phone = null;
  }

  if (!phone) {
    return {
      originalRaw,
      displayValue: trimmed,
      e164: null,
      telHref: null,
      extension: null,
      country: null,
      countryCallingCode: null,
      isValid: false,
      isPossible: false,
      formattingOutcome: "PRESERVED_ORIGINAL",
      parsingOutcome: "PARSE_FAILED",
      parseError: parseError || "UNPARSED",
    };
  }

  const isValid = typeof phone.isValid === "function" ? phone.isValid() : false;
  const isPossible =
    typeof phone.isPossible === "function" ? phone.isPossible() : false;

  // Only emit dialing links for valid (or at least possible) numbers.
  const canDial = isValid || isPossible;
  const e164 = canDial && phone.number ? String(phone.number) : null;
  let telHref = null;
  if (canDial) {
    try {
      telHref =
        typeof phone.getURI === "function"
          ? phone.getURI()
          : e164
            ? `tel:${e164}`
            : null;
    } catch (_e) {
      telHref = e164 ? `tel:${e164}` : null;
    }
  }

  const extension =
    phone.ext != null && String(phone.ext).trim()
      ? String(phone.ext).trim()
      : null;

  let displayValue = trimmed;
  let formattingOutcome = "PRESERVED_ORIGINAL";
  if (isValid || isPossible) {
    try {
      displayValue = formatDisplayFromParsed(phone) || trimmed;
      formattingOutcome = "FORMATTED";
    } catch (_e) {
      displayValue = trimmed;
      formattingOutcome = "PRESERVED_ORIGINAL";
    }
  }

  if (extension && formattingOutcome === "FORMATTED") {
    // Keep extension visible in human display; URI carries RFC3966 ext.
    if (!/ext\.?\s*\d+/i.test(displayValue)) {
      displayValue = `${displayValue} ext. ${extension}`;
    }
  }

  return {
    originalRaw,
    displayValue,
    e164,
    telHref,
    extension,
    country: phone.country || null,
    countryCallingCode: phone.countryCallingCode
      ? String(phone.countryCallingCode)
      : null,
    isValid,
    isPossible,
    formattingOutcome,
    parsingOutcome: "PARSED",
    parseError: null,
  };
}

/** Convenience: display string only (falls back to original). */
export function formatDealalityPhoneDisplay(raw, opts) {
  return formatDealalityPhone(raw, opts).displayValue;
}

/** Convenience: tel: href or empty string when unsafe. */
export function formatDealalityPhoneTelHref(raw, opts) {
  return formatDealalityPhone(raw, opts).telHref || "";
}

export default {
  CONTRACT_ID,
  LIBRARY_ID,
  LIBRARY_VERSION,
  formatDealalityPhone,
  formatDealalityPhoneDisplay,
  formatDealalityPhoneTelHref,
  formatDisplayFromParsed,
};
