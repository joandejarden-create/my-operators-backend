/**
 * Association / Event named-buyer recovery V1.
 * Uses Exa (+ optional free fetch) with association-specific queries —
 * not Scaling workplace-travel buyer strategies.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const ASSOCIATION_EVENTS_BUYER_RECOVERY_V1 =
  "gdi-association-events-buyer-recovery-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Strip headline pollution from Exa titles used as org names. */
export function cleanAssociationOrganizationName(raw = "") {
  let s = text(raw);
  if (!s) return "";
  // Keep leading association / camera / college phrase when present
  const m = s.match(
    /^((?:Asociaci[oó]n|Asociación|Colegio|Sociedad|C[aá]mara|Federation|Society|Association|Institute|American College|Congreso|Congress)[^|#]{2,90})/i,
  );
  if (m) s = m[1];
  s = s
    .replace(/\s*[|#].*$/, "")
    // Cut article / verb prose that follows a real org identity
    .replace(/\s+is\s+(?:a|an|the)\b.*$/i, "")
    .replace(/\s+(anunci[oó]|llev[oó]|busca|CONMEMORACI[OÓ]N|en\s*-\s*).*$/i, "")
    // Drop trailing date fragments glued onto college/org labels
    .replace(/\s+(?:Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)\b.*$/i, "")
    .replace(/\s+\d{1,2}\s*[-/]\s*\d{1,2}\s*[/]\s*20\d{2}\s*$/i, "")
    .replace(/\s*[-–—,:;]+\s*$/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  // Reject conference themes / slogans mistaken for organizers
  if (
    /^(de |the |a |el |la |design:|speakers\b)/i.test(s) ||
    (/:\s/.test(s) && !/\b(A\.?C\.?|S\.?A\.?|Inc\.?|LLC|Association|Asociaci)/i.test(s))
  ) {
    return "";
  }
  return s.slice(0, 120);
}

export function buildAssociationBuyerRecoveryQueries(opportunity = {}, hotel = {}) {
  const org = cleanAssociationOrganizationName(
    opportunity.organizationName || opportunity.organizer || "",
  );
  const eventName = text(opportunity.eventName || "").slice(0, 80);
  const city = text(hotel.city || opportunity.localConnection || "").split(/[—-]/)[0].trim();
  const queries = [];
  if (org.length >= 4) {
    queries.push(
      `"${org}" (Director OR "Executive Director" OR Secretario OR Secretaria OR "Director de Congresos" OR "Director de Eventos" OR "Meetings Manager" OR "Conference Director")`,
    );
    queries.push(
      `"${org}" ("comité organizador" OR "congreso" OR "asamblea" OR meetings OR conference) (contacto OR contact OR email OR director)`,
    );
  }
  if (eventName.length >= 8) {
    queries.push(
      `"${eventName}" (organizer OR organizador OR "comité" OR PCO OR "professional conference") (director OR manager)`,
    );
  }
  if (org && city) {
    queries.push(`"${org}" ${city} (congreso OR conference OR meetings) (director OR gerente)`);
  }
  // Longevity / VIVAVITA / Apex — expand beyond org inboxes
  const longevityBlob = `${org} ${eventName}`.toLowerCase();
  if (/longevity|vivavita|apex business|future human/i.test(longevityBlob)) {
    queries.push(
      `"Apex Business Group Worldwide" (founder OR director OR "managing director" OR CEO OR organiser OR organizer) (VIVAVITA OR Longevity OR Cancun OR Cancún)`,
    );
    queries.push(
      `VIVAVITA (Cancun OR Cancún) (director OR "event manager" OR "project manager" OR housing OR "hotel sourcing" OR "venue manager")`,
    );
    queries.push(
      `"info@vivavitaexpos.com" OR "info@gotoapex.com" (contact OR team OR director OR name)`,
    );
  }
  // IMCP lodging / venue owner beyond inscriptions/sponsorship
  if (/IMCP|Contadores Públicos|Asamblea-Convención/i.test(`${org} ${eventName}`)) {
    queries.push(
      `"IMCP" OR "Instituto Mexicano de Contadores" (Cancún OR Cancun) (hospedaje OR hotel OR sede OR venue OR "room block" OR reservaciones) (2027)`,
    );
    queries.push(
      `"Convención IMCP" 2027 (director OR gerente OR "coordinador de eventos" OR "organización") (hotel OR sede)`,
    );
  }
  // ANHP congress dates / lodging coordinator
  if (/ANHP|Hospitales Privados/i.test(`${org} ${eventName}`)) {
    queries.push(
      `"ANHP" OR "Asociación Nacional de Hospitales Privados" (congreso 2027 OR "Congreso de Gestión") (fechas OR junio OR hospedaje OR convenio OR "director de educación")`,
    );
  }
  return [...new Set(queries)].slice(0, 8);
}

function extractPeople(blob = "") {
  const people = [];
  const patterns = [
    /([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})\s*[,–-]\s*([^.\n|]{0,70}?(?:Director|Manager|Presidente|President|Secretari[oa]|Coordinator|Organizer|Gerente|CEO)[^.\n|]{0,40})/gi,
    /(?:Director|Manager|President|Presidente|Secretario|Secretaria|Gerente)\s+(?:de\s+[\wáéíóúñ]+\s+)?([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})/gi,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(blob))) {
      const name = text(m[1]);
      const title = text(m[2] || m[0]).slice(0, 90);
      if (looksLikePersonName(name) && name.split(/\s+/).length >= 2) {
        people.push({ name, title });
      }
    }
  }
  const seen = new Set();
  return people.filter((p) => {
    const k = p.name.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  }).slice(0, 6);
}

function extractRoutes(blob = "", url = null) {
  const email = (blob.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i) || [])[0] || null;
  const phone = (blob.match(/(?:\+?\d[\d\s().-]{7,}\d)/) || [])[0] || null;
  return {
    contactEmail: email,
    contactPhone: phone ? text(phone) : null,
    contactUrl: url,
  };
}

/**
 * @param {object} args
 * @param {object[]} args.opportunities
 * @param {object} args.hotel
 * @param {(q:string)=>Promise<{hits?:object[],status?:string}>} args.paidSearch
 * @param {number} [args.maxOpportunities] RESEARCH_BATCH_LIMIT wave size (not inventory max). Prefer batchWaveSize.
 * @param {number} [args.batchWaveSize]
 * @param {number} [args.cursor] resumable offset into opportunities
 * @param {number} [args.maxQueriesPerOpp]
 */
export async function runAssociationEventsBuyerRecovery({
  opportunities = [],
  hotel = {},
  paidSearch,
  maxOpportunities,
  batchWaveSize = 25,
  cursor = 0,
  maxQueriesPerOpp = 3,
} = {}) {
  const results = [];
  const list = opportunities || [];
  const wave =
    maxOpportunities == null || Number(maxOpportunities) <= 0
      ? Math.max(1, Number(batchWaveSize) || 25)
      : Math.max(1, Number(maxOpportunities));
  const start = Math.max(0, Number(cursor) || 0);
  const work = list.slice(start, start + wave);
  for (const opp of work) {
    const cleanedOrg = cleanAssociationOrganizationName(
      opp.organizationName || opp.organizer || "",
    );
    let next = {
      ...opp,
      organizationName: cleanedOrg || opp.organizationName,
      organizer: cleanedOrg || opp.organizer,
    };
    const queries = buildAssociationBuyerRecoveryQueries(next, hotel).slice(
      0,
      maxQueriesPerOpp,
    );
    const attempts = [];
    let people = [...(next.namedStakeholders || [])];
    for (const q of queries) {
      if (typeof paidSearch !== "function") break;
      const exa = await paidSearch(q);
      const hits = exa?.hits || [];
      attempts.push({ query: q, status: exa?.status, hitCount: hits.length });
      for (const hit of hits) {
        const blob = `${hit.title || ""} ${hit.text || hit.excerpt || ""}`;
        people.push(...extractPeople(blob));
        const routes = extractRoutes(blob, hit.url || hit.sourceUrl);
        if (routes.contactEmail && !next.contactEmail) next.contactEmail = routes.contactEmail;
        if (routes.contactPhone && !next.contactPhone) next.contactPhone = routes.contactPhone;
        if (routes.contactUrl && !next.contactUrl) next.contactUrl = routes.contactUrl;
        if (hit.url) {
          next.sources = [
            ...(next.sources || []),
            { url: hit.url, sourceFamily: "buyer_recovery", excerpt: text(hit.text || hit.excerpt).slice(0, 280) },
          ];
        }
      }
      if (people.some((p) => looksLikePersonName(p.name)) && (next.contactEmail || next.contactUrl || next.contactPhone)) {
        break;
      }
    }
    // Dedupe people
    const seen = new Set();
    people = people.filter((p) => {
      if (!looksLikePersonName(p?.name)) return false;
      const k = p.name.toLowerCase();
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
    if (people.length) {
      next.namedStakeholders = people;
      if (!looksLikePersonName(next.contactName)) {
        next.contactName = people[0].name;
        next.contactRole = people[0].title || "association_meetings";
      }
    }
    // Prefer org contact page as route when email missing
    if (!next.contactEmail && !next.contactPhone && !next.contactUrl && next.sourceUrl) {
      next.contactUrl = next.sourceUrl;
    }
    results.push({
      id: next.id,
      organizationName: next.organizationName,
      peopleFound: people.length,
      hasRoute: Boolean(next.contactEmail || next.contactPhone || next.contactUrl),
      attempts,
      opportunity: next,
    });
  }
  return {
    version: ASSOCIATION_EVENTS_BUYER_RECOVERY_V1,
    count: results.length,
    withNamedBuyer: results.filter((r) => r.peopleFound > 0).length,
    withRoute: results.filter((r) => r.hasRoute).length,
    results,
  };
}
