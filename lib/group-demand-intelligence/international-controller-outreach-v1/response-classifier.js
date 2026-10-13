/**
 * Classify controller responses → CONTROLLER_RESPONSE_STATUS + selection/lodging facts.
 * Synthetic fixtures must set isSyntheticTest=true and must not be persisted as production.
 */

import { CONTROLLER_RESPONSE_STATUS, RESPONSE_AUTHORITY } from "./constants.js";
import { SELECTION_STATUS, SELECTION_MODEL } from "../international-finalization/constants.js";
import { mapHotelSuppliedResponseToFacts } from "../international-finalization/hotel-supplied-response-map.js";

/**
 * Classify free-text controller reply.
 */
export function classifyControllerOutreachResponse(text = "", prior = {}) {
  const t = String(text || "").trim();
  const lower = t.toLowerCase();
  const base = mapHotelSuppliedResponseToFacts(t, prior);

  let status = CONTROLLER_RESPONSE_STATUS.OTHER;
  if (!t) status = CONTROLLER_RESPONSE_STATUS.NO_RESPONSE;
  // Rate/proposal takes priority over "still selecting" when both appear
  else if (
    /env[ií]e.*(tarif|precios|rates)|enviarnos tarifas|enviar tarifas|manden tarifas|send (us )?rates|please send.*rates/i.test(
      t
    )
  ) {
    status = CONTROLLER_RESPONSE_STATUS.RATE_REQUESTED;
  } else if (
    /env[ií]e.*(propuesta|proposal)|enviar propuesta|manden propuesta|submit (a )?proposal/i.test(t)
  ) {
    status = CONTROLLER_RESPONSE_STATUS.PROPOSAL_REQUESTED;
  } else if (
    /podemos incluir|can (still )?add|pueden a[ñn]adir|incluido en la lista|we can add your/i.test(t)
  ) {
    status = CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_CAN_APPLY;
  } else if (/ya (está |esta )?inclu|added to the list|hotel added/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_ADDED;
  } else if (
    /lista.*(final|cerrad|cerró)|already finalized|hotel list is closed|no aceptamos m[aá]s/i.test(t)
  ) {
    status = CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_FINALIZED;
  } else if (/no (pueden|podemos) incluir|cannot (be )?includ|rejected|no es posible/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_REJECTED;
  } else if (/lista abierta|todav[ií]a pueden|still (being )?select|submissions? open/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_OPEN;
  } else if (
    /a[uú]n no.*(lista|publicado)|not yet|pr[oó]xima circular|seleccionando|a[uú]n se est[aá]n/i.test(t)
  ) {
    status = CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_NOT_YET_OPEN;
  } else if (/nuestro pco|pco (se )?encarga|contact(e|ar) (a |al )?pco|agencia de alojamiento|dmc/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.PCO_REDIRECT;
  } else if (/contact(e|ar) (a |con )?(otra|otro)|redirig|forward|escriba a/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.CONTROLLER_REDIRECT;
  } else if (
    /reservan individual|self.?book|cada uno reserva|no hay programa (central|oficial) de hoteles/i.test(t)
  ) {
    status = CONTROLLER_RESPONSE_STATUS.SELF_BOOKING_ONLY;
  } else if (/no (hay|tenemos) (lista|programa|convenio) (de )?hotel/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.NO_HOTEL_PROGRAM;
  } else if (/seguimiento|follow.?up|le respondemos|en breve/i.test(t)) {
    status = CONTROLLER_RESPONSE_STATUS.NEEDS_FOLLOW_UP;
  }

  // Align selection facts with status
  const facts = { ...base, controllerResponseStatus: status };
  switch (status) {
    case CONTROLLER_RESPONSE_STATUS.RATE_REQUESTED:
    case CONTROLLER_RESPONSE_STATUS.PROPOSAL_REQUESTED:
    case CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_CAN_APPLY:
    case CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_OPEN:
      facts.selectionStatus = SELECTION_STATUS.OPEN;
      facts.lodgingEvidenceClass = "STRONG_HOTEL_MOTION";
      facts.futureDecisionProven = true;
      facts.buyerControllerProven = true;
      break;
    case CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_ADDED:
      facts.selectionStatus = SELECTION_STATUS.UNDER_REVIEW;
      facts.lodgingEvidenceClass = "DIRECT_LODGING_EVIDENCE";
      facts.targetHotelListed = true;
      facts.futureDecisionProven = true;
      break;
    case CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_FINALIZED:
    case CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_REJECTED:
      facts.selectionStatus = SELECTION_STATUS.CLOSED;
      facts.closedForPursuit = true;
      break;
    case CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_NOT_YET_OPEN:
      facts.selectionStatus = SELECTION_STATUS.EXPECTED;
      facts.futureDecisionProven = true;
      facts.lodgingEvidenceClass =
        facts.lodgingEvidenceClass === "UNCONFIRMED" ? "PLAUSIBLE_HOTEL_MOTION" : facts.lodgingEvidenceClass;
      break;
    case CONTROLLER_RESPONSE_STATUS.SELF_BOOKING_ONLY:
      facts.selectionModel = SELECTION_MODEL.SELF_BOOKING_RECOMMENDED_LIST;
      facts.selfBookingProven = true;
      break;
    case CONTROLLER_RESPONSE_STATUS.NO_HOTEL_PROGRAM:
      facts.closedForPursuit = true;
      facts.selectionStatus = SELECTION_STATUS.CLOSED;
      break;
    case CONTROLLER_RESPONSE_STATUS.PCO_REDIRECT:
    case CONTROLLER_RESPONSE_STATUS.CONTROLLER_REDIRECT:
      facts.buyerControllerProven = true;
      facts.redirectRequired = true;
      break;
    default:
      break;
  }

  facts.readyEligibleFromResponseAlone = false;
  facts.notes = [...(facts.notes || []), `controllerResponseStatus=${status}`];
  return facts;
}

/**
 * Authority of the reply sender (not every reply is canonical).
 */
export function assessResponseAuthority(input = {}) {
  const sender = String(input.senderEmail || input.sender || "").toLowerCase();
  const expected = String(input.expectedEmail || "").replace(/^mailto:/i, "").toLowerCase();
  const role = String(input.senderRole || "").toLowerCase();
  const refersEvent = input.refersToCorrectEvent !== false;
  const synthetic = input.isSyntheticTest === true;

  if (synthetic) {
    return {
      authority: RESPONSE_AUTHORITY.WEAK,
      okForCanonicalFacts: false,
      reason: "SYNTHETIC_TEST — must not persist as production evidence",
    };
  }
  if (!refersEvent) {
    return { authority: RESPONSE_AUTHORITY.WEAK, okForCanonicalFacts: false, reason: "Wrong event/cycle scope" };
  }
  if (expected && sender && sender === expected) {
    return {
      authority: RESPONSE_AUTHORITY.AUTHORITATIVE,
      okForCanonicalFacts: true,
      reason: "Sender matches outreach contact",
    };
  }
  if (/secretar|organiz|expositores|hotel|alojamiento|pco|housing/i.test(role + sender)) {
    return {
      authority: RESPONSE_AUTHORITY.STRONG,
      okForCanonicalFacts: true,
      reason: "Sender function relevant to lodging",
    };
  }
  if (sender) {
    return { authority: RESPONSE_AUTHORITY.PLAUSIBLE, okForCanonicalFacts: false, reason: "Unverified sender domain/role" };
  }
  return { authority: RESPONSE_AUTHORITY.WEAK, okForCanonicalFacts: false, reason: "No sender" };
}

/**
 * What a YES / NO style outcome means commercially (for handoff packets).
 */
export function interpretLikelyAnswers(objectiveKey = "") {
  const map = {
    biocultura: {
      yes: "Partner/recommended hotel list exists or will open → selection EXPECTED/OPEN; AC may apply; lodging motion strengthens; recompute Watch/Ready after contact path holds.",
      no: "No hotel program / list already closed without AC → NO_CURRENT_HOTEL_PATH or WAITING self-book only; do not force Ready.",
    },
    rif: {
      yes: "Convenio still forming / Radisson can apply or send rates → selection OPEN; STRONG lodging motion; closest path to Ready if named traveling entity also strengthens.",
      no: "Convenio finalized without Radisson → SELECTION_CLOSED / NO_CURRENT_HOTEL_PATH for this cycle.",
    },
    cielo: {
      yes: "List not final / Radisson can be added or send rates → TARGET_HOTEL_CAN_APPLY; upgrade fit eligibility after inclusion evidence.",
      no: "List finalized and Radisson excluded → drop current inclusion path; self-booking alone does not auto-Ready.",
    },
  };
  const k = String(objectiveKey || "").toLowerCase();
  if (k.includes("bio")) return map.biocultura;
  if (k.includes("rif")) return map.rif;
  if (k.includes("cielo")) return map.cielo;
  return {
    yes: "Selection open / hotel can apply → strengthen lodging + future decision; recompute packet; Ready only if all pillars pass.",
    no: "Selection closed / excluded → honest drop or watch without Ready inflation.",
  };
}

/** Marked TEST fixtures — never persist as production hotel-supplied evidence. */
export const SYNTHETIC_RESPONSE_FIXTURES = Object.freeze([
  {
    id: "TEST_OPEN",
    isSyntheticTest: true,
    text: "Sí, aún estamos seleccionando hoteles colaboradores. Pueden enviarnos tarifas.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.RATE_REQUESTED,
  },
  {
    id: "TEST_CLOSED",
    isSyntheticTest: true,
    text: "La lista de hoteles recomendados ya está finalizada y no aceptamos más propiedades.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.HOTEL_LIST_FINALIZED,
  },
  {
    id: "TEST_PCO_REDIRECT",
    isSyntheticTest: true,
    text: "Nuestro PCO se encarga del alojamiento: contacten a Housing Agency XYZ.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.PCO_REDIRECT,
  },
  {
    id: "TEST_RATE_REQUESTED",
    isSyntheticTest: true,
    text: "Please send us rates for the preferred hotel list.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.RATE_REQUESTED,
  },
  {
    id: "TEST_SELF_BOOKING_ONLY",
    isSyntheticTest: true,
    text: "No hay programa central: los delegados reservan individualmente en los hoteles recomendados.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.SELF_BOOKING_ONLY,
  },
  {
    id: "TEST_CAN_APPLY",
    isSyntheticTest: true,
    text: "Podemos incluir su hotel si envían propuesta antes de final de mes.",
    expectedStatus: CONTROLLER_RESPONSE_STATUS.TARGET_HOTEL_CAN_APPLY,
  },
]);

export function runSyntheticResponseMappingTests() {
  const results = [];
  for (const fx of SYNTHETIC_RESPONSE_FIXTURES) {
    const mapped = classifyControllerOutreachResponse(fx.text, {});
    const auth = assessResponseAuthority({ isSyntheticTest: true, senderEmail: "test@example.com" });
    const pass =
      mapped.controllerResponseStatus === fx.expectedStatus &&
      mapped.readyEligibleFromResponseAlone === false &&
      auth.okForCanonicalFacts === false;
    results.push({
      id: fx.id,
      expected: fx.expectedStatus,
      got: mapped.controllerResponseStatus,
      selectionStatus: mapped.selectionStatus,
      closedForPursuit: mapped.closedForPursuit === true,
      readyFromResponseAlone: mapped.readyEligibleFromResponseAlone,
      syntheticBlockedFromProduction: !auth.okForCanonicalFacts,
      pass,
    });
  }
  return {
    pass: results.every((r) => r.pass),
    results,
    note: "Synthetic fixtures only — do not persist as HOTEL_SUPPLIED_EVIDENCE production records",
  };
}
