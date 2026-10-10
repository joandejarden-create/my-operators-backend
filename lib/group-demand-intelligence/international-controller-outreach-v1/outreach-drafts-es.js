/**
 * Spanish evidence-seeking outreach drafts — BioCultura / RIF / CIELO.
 * Short, specific, non-salesy. Does not claim inclusion or partnership.
 */

/**
 * @returns {{ subject, body, language, objectives, disclaimer }}
 */
export function buildBioculturaOutreachDraft({ hotelName = "AC Hotel A Coruña" } = {}) {
  const subject = "BioCultura A Coruña 2027 — consulta sobre alojamiento / hoteles colaboradores";
  const body = [
    "Hola,",
    "",
    "Les escribo en relación con BioCultura A Coruña 2027 (5–7 de marzo, EXPOCoruña).",
    "",
    "¿Tienen previsto publicar una lista de hoteles colaboradores, recomendados o con convenio para expositores? ¿Quién gestiona ese proceso y en qué fechas se espera?",
    "",
    `Si aún está abierto, ¿pueden indicar si ${hotelName} podría ser considerado?`,
    "",
    "No damos por hecho ningún acuerdo previo: solo queremos seguir el procedimiento correcto.",
    "",
    "Gracias de antemano por su orientación.",
    "",
    "Un saludo,",
  ].join("\n");
  return {
    subject,
    body,
    language: "es",
    objectives: ["HOTEL_LIST_STATUS", "HOTEL_ELIGIBILITY", "SELECTION_TIMING", "HOUSING_CONTROLLER"],
    disclaimer: "Evidence-seeking draft only. Do not send automatically. Do not claim inclusion.",
  };
}

export function buildRifOutreachDraft({ hotelName = "Radisson Hotel Santo Domingo" } = {}) {
  const subject = "VII Congreso Iberoamericano de Filosofía 2027 — consulta sobre convenio hotelero";
  const body = [
    "Hola,",
    "",
    "Les escribo en relación con el VII Congreso Iberoamericano de Filosofía (15–19 de marzo de 2027, Santo Domingo / UASD).",
    "",
    "En la segunda convocatoria se indica que la organización ha gestionado tarifas preferenciales y que la lista de hoteles en convenio se publicará en una próxima circular.",
    "",
    `¿La lista sigue en formación? ¿Puede ${hotelName} aún ser incluido en ese convenio? ¿Quién gestiona los acuerdos de alojamiento y cuándo se prevé la circular definitiva?`,
    "",
    "Si procede enviar tarifas o una propuesta breve, indíquennos el canal correcto.",
    "",
    "Gracias por su orientación.",
    "",
    "Un saludo,",
  ].join("\n");
  return {
    subject,
    body,
    language: "es",
    objectives: ["HOTEL_LIST_STATUS", "HOTEL_ELIGIBILITY", "RATE_SUBMISSION", "SELECTION_TIMING"],
    disclaimer: "Evidence-seeking draft only. Do not send automatically. Do not claim inclusion.",
  };
}

export function buildCieloOutreachDraft({ hotelName = "Radisson Hotel Santo Domingo" } = {}) {
  const subject = "6º Congreso Mundial CIELO Laboral 2026 — consulta sobre lista de hoteles recomendados";
  const body = [
    "Hola,",
    "",
    "Les escribo en relación con el 6º Congreso Mundial CIELO Laboral (2–4 de diciembre de 2026, Santo Domingo / PUCMM).",
    "",
    "Hemos visto el PDF de hoteles recomendados publicados para el congreso.",
    "",
    `¿Esa lista está ya cerrada, o aún es posible añadir hoteles? En concreto, ¿podría ${hotelName} enviar tarifas / datos para valorar su inclusión?`,
    "",
    "También les agradeceríamos confirmar si el alojamiento es solo reserva individual por parte de los asistentes, o si hay alguna gestión centralizada de la lista.",
    "",
    "Gracias de antemano.",
    "",
    "Un saludo,",
  ].join("\n");
  return {
    subject,
    body,
    language: "es",
    objectives: ["HOTEL_LIST_STATUS", "HOTEL_ELIGIBILITY", "RATE_SUBMISSION", "PREFERRED_HOTEL_PROGRAM"],
    disclaimer: "Evidence-seeking draft only. Do not send automatically. Do not claim inclusion.",
  };
}

export function buildOutreachDraftForKey(key, opts = {}) {
  const k = String(key || "").toLowerCase();
  if (k.includes("biocultura")) return buildBioculturaOutreachDraft(opts);
  if (k.includes("rif") || k.includes("filosof")) return buildRifOutreachDraft(opts);
  if (k.includes("cielo")) return buildCieloOutreachDraft(opts);
  throw new Error(`Unknown outreach key: ${key}`);
}
