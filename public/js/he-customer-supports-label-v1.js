/**
 * Browser adapter for Hotel Explorer customer-facing SUPPORTS labels.
 * Mirrors lib/hotel-intelligence/sources/he-customer-supports-label-v1.js
 * (kept in sync for non-module IIFE workspace).
 */
(function (global) {
  "use strict";

  var SUPPORTS_CODE_LABELS = {
    ownership: "Hotel Ownership Structure",
    history: "Property Acquisition",
    operator: "Hotel Management",
    brand: "Brand Affiliation",
    brand_census: "Brand Affiliation",
    contact_research: "Hotel Leadership",
    market_alerts_hotel_fact: "Property News & Developments",
    guest_sentiment: "Guest Reputation",
    meeting_space: "Meeting Space & Facilities",
    rooms: "Guest Rooms & Facilities",
  };

  var FIELD_CODE_LABELS = [
    { re: /\b(legal[_ ]?owner|property[_ ]?company|propco|legal[_ ]?property)\b/i, label: "Legal Property Ownership" },
    { re: /\b(economic[_ ]?owner|economic[_ ]?sponsor|sponsor[_ ]?attribution)\b/i, label: "Economic Sponsorship" },
    { re: /\b(operator|managed[_ ]?by|management[_ ]?company|asset_agreement_profile|ops\s*&\s*investment)\b/i, label: "Hotel Management" },
    { re: /\b(brand|affiliation|franchise|star[_ ]?rating|starrating|reflag|breathless|hyatt|marriott|hilton)\b/i, label: "Brand Affiliation" },
    { re: /\b(property[_ ]?history|property[_ ]?acquisition|sale announced|sold to|acquisition financing)\b/i, label: "Property Acquisition" },
    { re: /\b(cmbs|financing|loan|mortgage|debt)\b/i, label: "Property Financing" },
    { re: /\b(meeting[_ ]?space|ballroom|meeting[_ ]?room)\b/i, label: "Meeting Space & Facilities" },
    { re: /\b(guest[_ ]?room|guestrooms?|keys|rooms?)\b/i, label: "Guest Rooms & Facilities" },
    { re: /\b(leadership|general[_ ]?manager|\bgm\b|director(?:a)? de ventas|director of sales|linkedin|hotel leadership)\b/i, label: "Hotel Leadership" },
    { re: /\b(ownership[_ ]?structure|ownership[_ ]?control|third-party owned|reporte anual|issuer profile|ownership)\b/i, label: "Hotel Ownership Structure" },
    { re: /\b(renovation|ampliaci[oó]n|expansion)\b/i, label: "Property News & Developments" },
  ];

  var FORBIDDEN = [
    /\brec[a-zA-Z0-9]{10,}\b/i,
    /\bHI\s*graph\b/i,
    /\bHI Graph Evidence\b/i,
    /\bownership edges?\b/i,
    /\brelationship edges?\b/i,
    /\bValidated HI\b/i,
    /\bAirtable\b/i,
    /\bUBO\b/,
    /\bVERIFIED_HI\b/i,
    /\bHI Hotel–Org\b/i,
    /\bHI Hotel-Org\b/i,
  ];

  function firstNonEmpty() {
    for (var i = 0; i < arguments.length; i += 1) {
      var v = arguments[i];
      if (v == null) continue;
      var s = String(v).trim();
      if (s) return s;
    }
    return "";
  }

  function containsForbidden(text) {
    var s = String(text || "");
    if (!s) return false;
    for (var i = 0; i < FORBIDDEN.length; i += 1) {
      if (FORBIDDEN[i].test(s)) return true;
    }
    return false;
  }

  function labelsFromFieldBlob(blob) {
    var text = String(blob || "");
    if (!text) return [];
    var out = [];
    var seen = {};
    for (var i = 0; i < FIELD_CODE_LABELS.length; i += 1) {
      if (!FIELD_CODE_LABELS[i].re.test(text)) continue;
      var lab = FIELD_CODE_LABELS[i].label;
      if (seen[lab]) continue;
      seen[lab] = true;
      out.push(lab);
    }
    return out;
  }

  function labelsFromClaims(claims) {
    var out = [];
    var seen = {};
    for (var i = 0; i < (claims || []).length; i += 1) {
      var blob = String(claims[i] || "").trim();
      if (!blob) continue;
      var fieldKey = blob.indexOf(":") >= 0 ? blob.slice(0, blob.indexOf(":")) : blob;
      var found = labelsFromFieldBlob(fieldKey).concat(labelsFromFieldBlob(blob));
      for (var j = 0; j < found.length; j += 1) {
        var label = found[j];
        if (seen[label]) continue;
        seen[label] = true;
        out.push(label);
      }
    }
    return out;
  }

  function customerSupportsLabel(citation) {
    citation = citation || {};
    var supportsCode = String(citation.supports || citation.support || "").trim();
    var perSourceClaim = firstNonEmpty(
      citation.source_claim,
      Array.isArray(citation.claims) ? citation.claims.join(" | ") : "",
      citation.fieldName,
      citation.field_name,
      citation.claim && !containsForbidden(citation.claim) ? citation.claim : "",
      citation.proves && !containsForbidden(citation.proves) ? citation.proves : "",
      citation.evidence_excerpt,
      citation.excerpt,
      citation.title
    );
    var claimList =
      Array.isArray(citation.claims) && citation.claims.length
        ? citation.claims
        : perSourceClaim
          ? [perSourceClaim]
          : [];
    var labels = labelsFromClaims(claimList);
    if (!labels.length) {
      var fromTitle = labelsFromFieldBlob(
        [
          citation.title,
          citation.provider,
          citation.publisher,
          citation.source_type,
          citation.category,
          citation.excerpt,
          citation.evidence_excerpt,
        ]
          .filter(Boolean)
          .join(" ")
      );
      if (fromTitle.length) labels = fromTitle;
    }
    if (!labels.length && supportsCode && SUPPORTS_CODE_LABELS[supportsCode]) {
      labels = [SUPPORTS_CODE_LABELS[supportsCode]];
    }
    if (!labels.length) labels = ["Property Research Evidence"];
    var out = labels.join("; ");
    if (containsForbidden(out)) {
      out = SUPPORTS_CODE_LABELS[supportsCode] || "Property Research Evidence";
    }
    out = out.replace(/\brec[a-zA-Z0-9]{10,}\b/gi, "").replace(/\s{2,}/g, " ").trim();
    if (!out || containsForbidden(out)) return "Property Research Evidence";
    return out;
  }

  function isGuestSentimentOnlyCitation(citation) {
    citation = citation || {};
    var supports = String(citation.supports || "").toLowerCase();
    var category = String(citation.category || "").toLowerCase();
    var sourceType = String(citation.source_type || "").toLowerCase();
    if (supports === "guest_sentiment" || category === "sentiment") return true;
    if (/guest reputation|guest sentiment/.test(sourceType)) return true;
    return false;
  }

  global.HeCustomerSupportsLabel = {
    VERSION: "he-customer-supports-label-v1",
    containsForbiddenSupportsTerminology: containsForbidden,
    customerSupportsLabel: customerSupportsLabel,
    isGuestSentimentOnlyCitation: isGuestSentimentOnlyCitation,
  };
})(typeof window !== "undefined" ? window : globalThis);
