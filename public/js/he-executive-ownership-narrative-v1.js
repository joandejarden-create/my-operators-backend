/**
 * Browser adapter for he-executive-ownership-narrative-v1 (customer-facing copy).
 * Keep logic aligned with lib/hotel-intelligence/ownership/he-executive-ownership-narrative-v1.js.
 */
(function (global) {
  "use strict";

  var INTERNAL_TERM_RE =
    /\b(HI\s+graph|HI\s+Organizations|Hotel[–-]Org|live\s+Airtable|Airtable|source[- ]of[- ]truth|research\s+posture|edges?\b|validated\s+relationship|PropCo\b|UBO\b|Phase\s*3\s+Proposed|not\s+Airtable-persisted)/i;

  function sanitizeCustomerPartyName(value) {
    if (value == null) return null;
    var s = String(value).trim();
    if (!s || s === "—") return null;
    s = s
      .replace(
        /\s*\((?:managed|management|operator|brand|franchisor|franchisee|owner|propco|ubo|sponsor|proposed|verified)\)\s*$/i,
        ""
      )
      .replace(/\s+/g, " ")
      .trim();
    return s || null;
  }

  function classifyOwnershipParty(party) {
    var p = party && typeof party === "object" ? party : {};
    var name = sanitizeCustomerPartyName(p.name);
    var known = Boolean(name) && p.known !== false;
    var status = String(p.status || p.verification_bucket || p.confidence || "").toUpperCase();
    var note = String(p.note || "");
    var proposed =
      known &&
      (/PROPOSED|MEDIUM|LOW|PARTIAL|SPONSOR/i.test(status) ||
        /sponsor attribution|not (a )?certified|proposed|ubo unresolved|ultimate ownership/i.test(
          note
        ));
    return { name: name, known: known, proposed: proposed, status: status };
  }

  function containsInternalOwnershipTerminology(text) {
    return INTERNAL_TERM_RE.test(String(text || ""));
  }

  function buildExecutiveOwnershipNarrative(input) {
    var opts = input || {};
    var hotel = sanitizeCustomerPartyName(opts.hotelName) || "This hotel";
    var legal = classifyOwnershipParty(opts.legalOwner);
    var economic = classifyOwnershipParty(opts.economicOwner);
    var brand = sanitizeCustomerPartyName(opts.brand);
    var operator = sanitizeCustomerPartyName(opts.operator);

    var sentences = [];
    if (legal.known) {
      sentences.push(hotel + "'s legal property company is " + legal.name + ".");
    }
    if (economic.known) {
      if (economic.proposed) {
        sentences.push(
          economic.name +
            " has been identified as a potential economic sponsor, although its ultimate ownership interest has not been independently confirmed."
        );
      } else {
        sentences.push("Economic ownership is attributed to " + economic.name + ".");
      }
    } else if (!legal.known) {
      sentences.push(
        "Legal ownership and economic sponsorship have not been independently confirmed for this property."
      );
    }
    if (brand && operator) {
      sentences.push(
        "The hotel operates under the " + brand + " brand and is managed by " + operator + "."
      );
    } else if (operator) {
      sentences.push("The hotel is managed by " + operator + ".");
    } else if (brand) {
      sentences.push("The hotel operates under the " + brand + " brand.");
    }

    var trimmed = sentences.slice(0, 3);
    return {
      text: trimmed.join(" ").replace(/\s+/g, " ").trim(),
      sentences: trimmed,
      classification: {
        legalVerified: legal.known && !legal.proposed,
        legalKnown: legal.known,
        economicProposed: economic.known && economic.proposed,
        economicVerified: economic.known && !economic.proposed,
        hasBrand: Boolean(brand),
        hasOperator: Boolean(operator),
      },
    };
  }

  function ownershipMethodologyNote() {
    return {
      summary: "About This Ownership Summary",
      coverage: [
        "This summary distinguishes verified legal property companies from proposed economic sponsors.",
        "Brand and management relationships are operating facts and are not treated as ownership.",
      ],
      limitations: [
        "Proposed economic sponsors are not confirmed ultimate beneficial owners.",
        "Supporting evidence and relationship records are available in Sources & Evidence.",
      ],
    };
  }

  global.HeExecutiveOwnershipNarrative = {
    sanitizeCustomerPartyName: sanitizeCustomerPartyName,
    classifyOwnershipParty: classifyOwnershipParty,
    containsInternalOwnershipTerminology: containsInternalOwnershipTerminology,
    buildExecutiveOwnershipNarrative: buildExecutiveOwnershipNarrative,
    ownershipMethodologyNote: ownershipMethodologyNote,
  };
})(typeof window !== "undefined" ? window : globalThis);
