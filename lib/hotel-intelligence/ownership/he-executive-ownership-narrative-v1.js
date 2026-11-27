/**
 * Customer-facing Executive Ownership Summary narrative (display only).
 * Builds 2–3 readable sentences from verified legal ownership, proposed/verified
 * economic sponsorship, brand, and operator — never HI/Airtable internals.
 */

const INTERNAL_TERM_RE =
  /\b(HI\s+graph|HI\s+Organizations|Hotel[–-]Org|live\s+Airtable|Airtable|source[- ]of[- ]truth|research\s+posture|edges?\b|validated\s+relationship|PropCo\b|UBO\b|Phase\s*3\s+Proposed|not\s+Airtable-persisted)/i;

/** Strip internal role tags like "(Managed)" from customer-facing party names. */
export function sanitizeCustomerPartyName(value) {
  if (value == null) return null;
  let s = String(value).trim();
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

/**
 * @param {object|null|undefined} party
 * @returns {{ name: string|null, known: boolean, proposed: boolean, status: string }}
 */
export function classifyOwnershipParty(party) {
  const p = party && typeof party === "object" ? party : {};
  const name = sanitizeCustomerPartyName(p.name);
  const known = Boolean(name) && p.known !== false;
  const status = String(p.status || p.verification_bucket || p.confidence || "").toUpperCase();
  const note = String(p.note || "");
  const proposed =
    known &&
    (/PROPOSED|MEDIUM|LOW|PARTIAL|SPONSOR/i.test(status) ||
      /sponsor attribution|not (a )?certified|proposed|ubo unresolved|ultimate ownership/i.test(
        note
      ));
  return { name, known, proposed, status };
}

export function containsInternalOwnershipTerminology(text) {
  return INTERNAL_TERM_RE.test(String(text || ""));
}

/**
 * @param {object} input
 * @param {string} [input.hotelName]
 * @param {object} [input.legalOwner] control.legal_property_owner_propco
 * @param {object} [input.economicOwner] control.economic_owner_or_group
 * @param {string} [input.brand]
 * @param {string} [input.operator]
 * @returns {{ text: string, sentences: string[], classification: object }}
 */
export function buildExecutiveOwnershipNarrative(input) {
  const opts = input || {};
  const hotel = sanitizeCustomerPartyName(opts.hotelName) || "This hotel";
  const legal = classifyOwnershipParty(opts.legalOwner);
  const economic = classifyOwnershipParty(opts.economicOwner);
  const brand = sanitizeCustomerPartyName(opts.brand);
  const operator = sanitizeCustomerPartyName(opts.operator);

  const sentences = [];

  if (legal.known) {
    sentences.push(`${hotel}'s legal property company is ${legal.name}.`);
  }

  if (economic.known) {
    if (economic.proposed) {
      sentences.push(
        `${economic.name} has been identified as a potential economic sponsor, although its ultimate ownership interest has not been independently confirmed.`
      );
    } else {
      sentences.push(`Economic ownership is attributed to ${economic.name}.`);
    }
  } else if (!legal.known) {
    sentences.push(
      "Legal ownership and economic sponsorship have not been independently confirmed for this property."
    );
  }

  if (brand && operator) {
    sentences.push(
      `The hotel operates under the ${brand} brand and is managed by ${operator}.`
    );
  } else if (operator) {
    sentences.push(`The hotel is managed by ${operator}.`);
  } else if (brand) {
    sentences.push(`The hotel operates under the ${brand} brand.`);
  }

  // Cap at three readable sentences when all parties are present.
  const trimmed = sentences.slice(0, 3);
  const text = trimmed.join(" ").replace(/\s+/g, " ").trim();

  return {
    text,
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

export function ownershipMethodologyNote() {
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

