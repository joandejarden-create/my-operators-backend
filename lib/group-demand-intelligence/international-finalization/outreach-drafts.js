/**
 * Evidence-seeking outreach drafts — short, specific, non-salesy.
 * Local language by market. Does not claim relationship or inclusion.
 */

import { EVIDENCE_ASK_CATEGORY } from "./constants.js";

/**
 * @returns {{ language, subject, body, category, disclaimer }}
 */
export function generateEvidenceOutreachDraft({
  hotelName = "",
  campaignName = "",
  controllerName = "",
  contactPath = "",
  category = EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS,
  languages = ["en"],
  ask = "",
} = {}) {
  const lang = pickLang(languages);
  const hotel = hotelName || "our hotel";
  const event = campaignName || "your event";
  const who = controllerName || "your team";
  // Always localize the ask by category; do not inject English evidence-request strings into local drafts
  const question = defaultAsk(category, lang) || ask;

  const templates = {
    en: {
      subject: `${event} — accommodation process question`,
      body: [
        `Hello ${who},`,
        ``,
        `I am writing regarding accommodation planning for ${event}.`,
        `${question}`,
        ``,
        `We represent ${hotel}. We are not assuming any prior arrangement — only asking how hotel selection / accommodation is handled for this cycle so we can follow the correct process.`,
        ``,
        `Thank you for any guidance you can share.`,
      ].join("\n"),
    },
    es: {
      subject: `${event} — consulta sobre proceso de alojamiento`,
      body: [
        `Hola ${who},`,
        ``,
        `Les escribo en relación con la gestión de alojamiento para ${event}.`,
        `${question}`,
        ``,
        `Representamos a ${hotel}. No damos por hecho ningún acuerdo previo: solo queremos entender cómo se gestiona la selección de hoteles / alojamiento en este ciclo para seguir el procedimiento correcto.`,
        ``,
        `Gracias de antemano por su orientación.`,
      ].join("\n"),
    },
    gl: {
      subject: `${event} — consulta sobre aloxamento`,
      body: [
        `Ola ${who},`,
        ``,
        `Escríballes en relación coa xestión de aloxamento para ${event}.`,
        `${question}`,
        ``,
        `Representamos a ${hotel}. Non presupomos ningún acordo previo; só queremos entender como se xestiona a selección de hoteis neste ciclo.`,
        ``,
        `Grazas pola súa orientación.`,
      ].join("\n"),
    },
    de: {
      subject: `${event} — Frage zum Unterkunftsprozess`,
      body: [
        `Guten Tag ${who},`,
        ``,
        `ich melde mich bezüglich der Hotel-/Unterkunftsplanung für ${event}.`,
        `${question}`,
        ``,
        `Wir vertreten ${hotel}. Wir unterstellen keine bestehende Vereinbarung — uns interessiert ausschließlich, wie die Hotelauswahl in diesem Zyklus organisiert ist, damit wir den richtigen Prozess einhalten.`,
        ``,
        `Vielen Dank für Ihre Hinweise.`,
      ].join("\n"),
    },
  };

  const t = templates[lang] || templates.en;
  return {
    language: lang,
    subject: t.subject,
    body: t.body,
    category,
    contactPath: contactPath || null,
    disclaimer:
      "Evidence-seeking draft only. Do not claim inclusion, prior relationship, or confirmed room demand.",
  };
}

function pickLang(languages = []) {
  const order = ["es", "gl", "de", "en"];
  for (const l of order) {
    if (languages.map((x) => String(x).toLowerCase()).includes(l)) return l;
  }
  return "en";
}

function defaultAsk(category, lang) {
  const map = {
    en: {
      [EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS]:
        "Could you confirm whether partner hotels are still being selected, and when a hotel list is expected?",
      [EVIDENCE_ASK_CATEGORY.RATE_SUBMISSION]:
        "Are you currently accepting hotel rate submissions for the recommended or official hotel list?",
      [EVIDENCE_ASK_CATEGORY.LODGING_CONTROLLER]:
        "Who manages accommodation / hotel selection for this event, and what is the best contact path?",
      [EVIDENCE_ASK_CATEGORY.ROOM_BLOCK]:
        "Is there an official room block, or do delegates book individually from a recommended list?",
      [EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST]:
        "Can a hotel still be considered for the preferred / recommended hotel list before finalization?",
      [EVIDENCE_ASK_CATEGORY.OVERFLOW]:
        "If a host hotel is already designated, are secondary or overflow hotels still being considered? (We will not assume overflow.)",
      [EVIDENCE_ASK_CATEGORY.RFP_TIMELINE]:
        "Is there an RFP or procurement timeline for lodging / meeting services?",
      [EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY]:
        "What criteria determine hotel eligibility for this program?",
    },
    es: {
      [EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS]:
        "¿Podrían confirmar si aún se están seleccionando hoteles colaboradores y cuándo se espera la lista?",
      [EVIDENCE_ASK_CATEGORY.RATE_SUBMISSION]:
        "¿Están aceptando envío de tarifas para la lista de hoteles recomendados u oficiales?",
      [EVIDENCE_ASK_CATEGORY.LODGING_CONTROLLER]:
        "¿Quién gestiona el alojamiento / la selección de hoteles y cuál es el mejor canal de contacto?",
      [EVIDENCE_ASK_CATEGORY.ROOM_BLOCK]:
        "¿Existe un bloque oficial de habitaciones o los participantes reservan individualmente?",
      [EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST]:
        "¿Se puede valorar aún la inclusión de un hotel en la lista recomendada antes del cierre?",
      [EVIDENCE_ASK_CATEGORY.OVERFLOW]:
        "Si ya hay un hotel sede, ¿se contemplan aún hoteles secundarios u overflow? (No asumimos overflow.)",
      [EVIDENCE_ASK_CATEGORY.RFP_TIMELINE]:
        "¿Existe un calendario de RFP o contratación para alojamiento?",
      [EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY]:
        "¿Qué criterios determinan la elegibilidad de un hotel para este programa?",
    },
    gl: {
      [EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS]:
        "Podrían confirmar se aínda se están seleccionando hoteis colaboradores e cando se espera a lista?",
      [EVIDENCE_ASK_CATEGORY.RATE_SUBMISSION]:
        "Están a aceptar envío de tarifas para a lista de hoteis recomendados ou oficiais?",
      [EVIDENCE_ASK_CATEGORY.LODGING_CONTROLLER]:
        "Quen xestiona o aloxamento / a selección de hoteis e cal é o mellor canal de contacto?",
      [EVIDENCE_ASK_CATEGORY.ROOM_BLOCK]:
        "Existe un bloque oficial de habitacións ou os participantes reservan individualmente?",
      [EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST]:
        "Pódese valorar aínda a inclusión dun hotel na lista recomendada antes do peche?",
      [EVIDENCE_ASK_CATEGORY.OVERFLOW]:
        "Se xa hai un hotel sede, contemplan aínda hoteis secundarios ou overflow? (Non asumimos overflow.)",
      [EVIDENCE_ASK_CATEGORY.RFP_TIMELINE]:
        "Existe un calendario de RFP ou contratación para aloxamento?",
      [EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY]:
        "Que criterios determinan a elixibilidade dun hotel para este programa?",
    },
    de: {
      [EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS]:
        "Können Sie bestätigen, ob Partnerhotels noch ausgewählt werden und wann eine Hotelliste erwartet wird?",
      [EVIDENCE_ASK_CATEGORY.RATE_SUBMISSION]:
        "Nehmen Sie derzeit Hotelraten für die empfohlene oder offizielle Hotelliste entgegen?",
      [EVIDENCE_ASK_CATEGORY.LODGING_CONTROLLER]:
        "Wer verantwortet die Unterkunft / Hotelauswahl, und welcher Kontaktweg ist der richtige?",
      [EVIDENCE_ASK_CATEGORY.ROOM_BLOCK]:
        "Gibt es ein offizielles Zimmerkontingent, oder buchen Teilnehmende individuell?",
      [EVIDENCE_ASK_CATEGORY.PREFERRED_HOTEL_LIST]:
        "Kann ein Hotel vor Finalisierung noch für die empfohlene Liste berücksichtigt werden?",
      [EVIDENCE_ASK_CATEGORY.OVERFLOW]:
        "Falls ein Haupthotel bereits feststeht: werden noch sekundäre / Overflow-Hotels geprüft? (Kein Overflow wird unterstellt.)",
      [EVIDENCE_ASK_CATEGORY.RFP_TIMELINE]:
        "Gibt es einen RFP- oder Vergabezeitplan für Unterkunftsleistungen?",
      [EVIDENCE_ASK_CATEGORY.HOTEL_ELIGIBILITY]:
        "Welche Kriterien bestimmen die Eignung eines Hotels für dieses Programm?",
    },
  };
  const pack = map[lang] || map.en;
  return pack[category] || pack[EVIDENCE_ASK_CATEGORY.HOTEL_SELECTION_STATUS];
}
