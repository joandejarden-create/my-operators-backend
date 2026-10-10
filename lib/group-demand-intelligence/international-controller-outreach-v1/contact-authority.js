/**
 * Verify controller contact authority before outreach.
 */

import { CONTACT_AUTHORITY, CONTACT_CHANNEL } from "./constants.js";

/**
 * @param {object} input
 * @returns {{ authority, channel, responsibleFor, reasons, okForOutreach }}
 */
export function verifyContactAuthority(input = {}) {
  const email = String(input.email || input.contact || "").replace(/^mailto:/i, "").trim();
  const org = String(input.controllerName || input.organization || "").trim();
  const role = String(input.role || input.function || "").trim();
  const source = String(input.officialSource || input.evidenceSource || "").trim();
  const lodgingAuth = String(input.lodgingAuthority || "").toUpperCase();

  const reasons = [];
  let authority = CONTACT_AUTHORITY.UNCONFIRMED;
  let channel = CONTACT_CHANNEL.OTHER;

  if (!email) {
    return {
      authority: CONTACT_AUTHORITY.UNCONFIRMED,
      channel: CONTACT_CHANNEL.OTHER,
      responsibleFor: [],
      reasons: ["No email contact"],
      okForOutreach: false,
    };
  }

  const local = email.split("@")[0] || "";
  const domain = (email.split("@")[1] || "").toLowerCase();

  if (/^(info|contact|hello|hola|admin)@/i.test(email)) {
    authority = CONTACT_AUTHORITY.WEAK;
    channel = CONTACT_CHANNEL.FUNCTIONAL_EMAIL;
    reasons.push("Generic inbox prefix");
  } else if (/expositores|exhibitor|alojamiento|housing|hotel|reservas|accommodation/i.test(local)) {
    authority = CONTACT_AUTHORITY.FUNCTIONALLY_RELEVANT;
    channel = CONTACT_CHANNEL.FUNCTIONAL_EMAIL;
    reasons.push("Functional mailbox tied to exhibitors/housing");
  } else if (/congreso|congress|secretariat|secretaria|adofil|rif|cielo/i.test(local + domain + org)) {
    authority = CONTACT_AUTHORITY.FUNCTIONALLY_RELEVANT;
    channel = CONTACT_CHANNEL.SECRETARIAT_EMAIL;
    reasons.push("Secretariat / congress mailbox");
  } else if (/@gmail\.com$/i.test(email) && /adofil|cielo|congreso/i.test(local + org)) {
    authority = CONTACT_AUTHORITY.PLAUSIBLE;
    channel = CONTACT_CHANNEL.SECRETARIAT_EMAIL;
    reasons.push("Named congress Gmail used on official convocatoria — plausible secretariat");
  } else {
    authority = CONTACT_AUTHORITY.PLAUSIBLE;
    channel = CONTACT_CHANNEL.FUNCTIONAL_EMAIL;
    reasons.push("Non-generic email");
  }

  if (lodgingAuth === "CONFIRMED_LODGING_CONTROLLER") {
    if (authority === CONTACT_AUTHORITY.WEAK) {
      authority = CONTACT_AUTHORITY.PLAUSIBLE;
    } else if (
      authority === CONTACT_AUTHORITY.FUNCTIONALLY_RELEVANT ||
      authority === CONTACT_AUTHORITY.PLAUSIBLE
    ) {
      authority = CONTACT_AUTHORITY.HIGH_AUTHORITY;
    }
    reasons.push("Confirmed lodging controller from prior finalization");
  }

  if (source) {
    reasons.push(`Official source: ${source}`);
    if (authority === CONTACT_AUTHORITY.PLAUSIBLE) {
      authority = CONTACT_AUTHORITY.FUNCTIONALLY_RELEVANT;
    }
  }

  const responsibleFor = [];
  if (/expositores|exhibitor/i.test(local + role)) responsibleFor.push("exhibitors");
  if (/hotel|alojamiento|housing|convenio|lodging/i.test(local + role + org)) {
    responsibleFor.push("hotel_selection", "lodging");
  }
  if (/secretar|congreso|adofil|cielo|rif|organiz/i.test(local + org + role)) {
    responsibleFor.push("secretariat", "event_operations");
  }
  if (!responsibleFor.length) responsibleFor.push("event_operations");

  const okForOutreach = ![CONTACT_AUTHORITY.WEAK, CONTACT_AUTHORITY.UNCONFIRMED].includes(authority);

  return {
    authority,
    channel,
    responsibleFor,
    reasons,
    okForOutreach,
    email,
    organization: org,
    role: role || null,
    officialSource: source || null,
  };
}
