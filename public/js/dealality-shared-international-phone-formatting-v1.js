/**
 * DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1 — browser facade.
 * Requires /vendor/libphonenumber-js.min.js (exposes global `libphonenumber`).
 * Logic mirrors lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js.
 */
(function (global) {
  "use strict";

  var CONTRACT_ID = "DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1";
  var LIBRARY_ID = "libphonenumber-js";
  var LIBRARY_VERSION = "1.13.17";

  function text(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function getParse() {
    var lib = global.libphonenumber || {};
    return (
      lib.parsePhoneNumberFromString ||
      lib.parsePhoneNumber ||
      null
    );
  }

  function formatDisplayFromParsed(phone) {
    if (!phone) return "";
    var cc = String(phone.countryCallingCode || "");
    if (cc === "1") {
      var national =
        typeof phone.formatNational === "function" ? phone.formatNational() : "";
      return national ? "+1 " + national : phone.formatInternational();
    }
    return phone.formatInternational();
  }

  function emptyResult(originalRaw) {
    return {
      originalRaw: originalRaw === "" ? "" : originalRaw == null ? null : originalRaw,
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

  function formatDealalityPhone(raw, opts) {
    opts = opts || {};
    var originalRaw = raw == null ? null : String(raw);
    var trimmed = text(raw);
    if (!trimmed) return emptyResult(originalRaw);

    var parsePhoneNumberFromString = getParse();
    if (typeof parsePhoneNumberFromString !== "function") {
      return {
        originalRaw: originalRaw,
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
        parseError: "LIBPHONENUMBER_UNAVAILABLE",
      };
    }

    var defaultCountry =
      text(opts.contactCountry || opts.defaultCountry || "").toUpperCase() ||
      undefined;
    var parseOpts =
      trimmed.charAt(0) === "+"
        ? undefined
        : defaultCountry
          ? { defaultCountry: defaultCountry }
          : undefined;

    var phone = null;
    var parseError = null;
    try {
      phone = parsePhoneNumberFromString(trimmed, parseOpts);
    } catch (err) {
      parseError = String(err && err.message ? err.message : err);
      phone = null;
    }

    if (!phone) {
      return {
        originalRaw: originalRaw,
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

    var isValid = typeof phone.isValid === "function" ? phone.isValid() : false;
    var isPossible =
      typeof phone.isPossible === "function" ? phone.isPossible() : false;
    var canDial = isValid || isPossible;
    var e164 = canDial && phone.number ? String(phone.number) : null;
    var telHref = null;
    if (canDial) {
      try {
        telHref =
          typeof phone.getURI === "function"
            ? phone.getURI()
            : e164
              ? "tel:" + e164
              : null;
      } catch (_e) {
        telHref = e164 ? "tel:" + e164 : null;
      }
    }

    var extension =
      phone.ext != null && String(phone.ext).trim()
        ? String(phone.ext).trim()
        : null;

    var displayValue = trimmed;
    var formattingOutcome = "PRESERVED_ORIGINAL";
    if (isValid || isPossible) {
      try {
        displayValue = formatDisplayFromParsed(phone) || trimmed;
        formattingOutcome = "FORMATTED";
      } catch (_e2) {
        displayValue = trimmed;
        formattingOutcome = "PRESERVED_ORIGINAL";
      }
    }

    if (extension && formattingOutcome === "FORMATTED") {
      if (!/ext\.?\s*\d+/i.test(displayValue)) {
        displayValue = displayValue + " ext. " + extension;
      }
    }

    return {
      originalRaw: originalRaw,
      displayValue: displayValue,
      e164: e164,
      telHref: telHref,
      extension: extension,
      country: phone.country || null,
      countryCallingCode: phone.countryCallingCode
        ? String(phone.countryCallingCode)
        : null,
      isValid: isValid,
      isPossible: isPossible,
      formattingOutcome: formattingOutcome,
      parsingOutcome: "PARSED",
      parseError: null,
    };
  }

  function formatDealalityPhoneDisplay(raw, opts) {
    return formatDealalityPhone(raw, opts).displayValue;
  }

  function formatDealalityPhoneTelHref(raw, opts) {
    return formatDealalityPhone(raw, opts).telHref || "";
  }

  var api = {
    CONTRACT_ID: CONTRACT_ID,
    LIBRARY_ID: LIBRARY_ID,
    LIBRARY_VERSION: LIBRARY_VERSION,
    formatDealalityPhone: formatDealalityPhone,
    formatDealalityPhoneDisplay: formatDealalityPhoneDisplay,
    formatDealalityPhoneTelHref: formatDealalityPhoneTelHref,
    formatDisplayFromParsed: formatDisplayFromParsed,
  };

  global.DealalitySharedPhoneFormattingV1 = api;
  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
})(typeof window !== "undefined" ? window : globalThis);
