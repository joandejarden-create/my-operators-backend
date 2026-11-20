/**
 * Buying-ecosystem research targets + search queries for Commercial Contact Intelligence.
 * Extends Public Contact Discovery 2.0 — does not replace person-channel search.
 */
export const GDI_COMMERCIAL_CONTACT_BUYING_ECOSYSTEM_V1 =
  "gdi-commercial-contact-buying-ecosystem-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/**
 * Infer likely demand-placement stakeholders from opportunity evidence.
 * Returns research targets — not invented named people.
 */
export function inferBuyingEcosystemTargets(opportunity = {}) {
  const segment = text(
    opportunity.segment ||
      opportunity.opportunityType ||
      opportunity.demandMechanism ||
      "",
  );
  const title = text(opportunity.title || opportunity.displayTitle);
  const org = text(
    opportunity.organizationName || opportunity.displayCompanyName || "",
  );
  const blob = `${segment} ${title} ${org} ${text(opportunity.whatIsHappening)}`.toLowerCase();

  const targets = [];
  const push = (id, label, reason) => {
    if (targets.some((t) => t.id === id)) return;
    targets.push({ id, label, reason });
  };

  const isEvent =
    /conference|congress|expo|exhibition|fair|summit|symposium|congreso|feria|evento/i.test(
      blob,
    ) || /associations_conferences_events|trade.?exhibition/i.test(segment);
  const isGov =
    /gobierno|secretar|jalisco|municipal|public.?sector|siop|infraestructura/i.test(
      blob,
    ) || /projects.?infrastructure/i.test(segment);
  const isCorp =
    /corporate|empresa|company|procurement|travel.?management/i.test(blob);
  const isSports = /sport|team|tournament|liga|delegat/i.test(blob);
  const isMedical = /hospital|medical|university|research|universidad/i.test(
    blob,
  );
  const isProject = /project|obra|construction|workforce|infraestructura/i.test(
    blob,
  );

  if (isEvent) {
    push(
      "event_organizer_commercial",
      "Event / exhibition commercial contacts",
      "Organizer sales staff are reachable entry points; hotel sourcing may be separate",
    );
    push(
      "housing_coordinator",
      "Official housing / accommodation coordinator",
      "Housing partners and hotel-rate pages often identify lodging decision routes",
    );
    push(
      "meeting_planner",
      "Association / conference meeting planner",
      "Meeting planners often control or refer room-block decisions",
    );
    push(
      "hospitality_logistics",
      "Event hospitality / guest services",
      "Hospitality logistics may own delegate accommodation",
    );
  }
  if (isGov || isProject) {
    push(
      "project_visitor_travel",
      "Project visitor / delegation travel contact",
      "Infrastructure programs may generate visiting-staff lodging without a procurement buyer",
    );
    push(
      "operations_coordinator",
      "Operations / logistics coordinator",
      "Operations contacts can refer travel patterns even when not the buyer",
    );
  }
  if (isCorp) {
    push(
      "corporate_travel_procurement",
      "Corporate travel / meetings procurement",
      "Travel management and procurement often place hotel demand",
    );
  }
  if (isSports) {
    push(
      "team_travel_coordinator",
      "Team / delegation travel coordinator",
      "Sports and delegation travel often has a dedicated lodging owner",
    );
  }
  if (isMedical) {
    push(
      "medical_research_coordinator",
      "University / medical / research coordinator",
      "Academic and medical events often route lodging through a coordinator",
    );
  }

  // Always consider existing primary + org routes
  push(
    "existing_verified_contact",
    "Existing verified GDI contact",
    "Reuse before paid or new discovery search",
  );
  push(
    "organizational_meetings_route",
    "Organizational meetings / contact desk",
    "Fallback when named buyers are unavailable",
  );

  return {
    version: GDI_COMMERCIAL_CONTACT_BUYING_ECOSYSTEM_V1,
    organizationName: org || null,
    demandCategory: segment || null,
    targets,
    flags: { isEvent, isGov, isCorp, isSports, isMedical, isProject },
  };
}

/**
 * Bounded ecosystem search queries (Exa). Snippets are candidates only.
 */
export function buildBuyingEcosystemQueryPlan(opportunity = {}, opts = {}) {
  const org = text(
    opportunity.organizationName || opportunity.displayCompanyName || "",
  );
  const title = text(opportunity.title || opportunity.displayTitle);
  const eventToken = title.split(/[–—|-]/)[0]?.trim() || title;
  const lang = opts.includeSpanish !== false;
  const ecosystem = inferBuyingEcosystemTargets(opportunity);
  const queries = [];
  const push = (q, kind, targetId) => {
    const s = text(q);
    if (!s || queries.some((x) => x.query === s)) return;
    queries.push({ query: s, kind, targetId: targetId || null });
  };

  if (org) {
    push(`${org} hotel accommodation contact`, "org_hotel_accommodation", "housing_coordinator");
    push(`${org} travel manager`, "org_travel_manager", "corporate_travel_procurement");
    push(`${org} meeting planner`, "org_meeting_planner", "meeting_planner");
    push(`${org} housing coordinator`, "org_housing", "housing_coordinator");
    if (lang) {
      push(`${org} hospedaje contacto`, "es_org_hospedaje", "housing_coordinator");
      push(`${org} bloque de habitaciones`, "es_org_room_block", "housing_coordinator");
      push(`${org} coordinador de hospedaje`, "es_org_housing_coord", "housing_coordinator");
    }
  }

  if (eventToken) {
    push(`${eventToken} official housing coordinator`, "event_housing", "housing_coordinator");
    push(`${eventToken} room block`, "event_room_block", "housing_coordinator");
    push(`${eventToken} hotel booking`, "event_hotel_booking", "housing_coordinator");
    push(`${eventToken} accommodation partner`, "event_accommodation_partner", "housing_coordinator");
    push(`${eventToken} hospitality logistics`, "event_hospitality", "hospitality_logistics");
    push(`${eventToken} conference meeting planner`, "event_meeting_planner", "meeting_planner");
    if (lang) {
      push(`${eventToken} hoteles tarifas`, "es_event_hotel_rates", "housing_coordinator");
      push(`${eventToken} hospedaje oficial`, "es_event_official_housing", "housing_coordinator");
    }
  }

  if (org && eventToken) {
    push(
      `${org} ${stripAccents(eventToken)} organizer accommodation`,
      "org_event_accommodation",
      "event_organizer_commercial",
    );
  }

  // Domain-aware extras for known fixtures
  if (/ecomondo|igeco/i.test(`${org} ${title}`)) {
    push(
      `Ecomondo Mexico hotel rates accommodation`,
      "ecomondo_hotels_page",
      "housing_coordinator",
    );
    push(
      `site:ecomondomexico.com.mx hoteles`,
      "ecomondo_hoteles_site",
      "housing_coordinator",
    );
    push(
      `site:ecomondomexico.com.mx contact Sales Manager`,
      "ecomondo_contact_sales",
      "event_organizer_commercial",
    );
  }
  if (/jalisco|siop/i.test(`${org} ${title}`)) {
    push(
      `SIOP Jalisco visitas delegaciones hospedaje`,
      "siop_visitor_travel",
      "project_visitor_travel",
    );
    push(
      `Gobierno de Jalisco coordinador viajes oficiales`,
      "jalisco_official_travel",
      "project_visitor_travel",
    );
  }

  const standardCap = Number(opts.standardQueryCap || 6);
  const escalationCap = Number(opts.escalationQueryCap || 12);

  return {
    version: GDI_COMMERCIAL_CONTACT_BUYING_ECOSYSTEM_V1,
    ecosystem,
    queries,
    standardQueryCap: standardCap,
    escalationQueryCap: escalationCap,
    maxCandidates: Number(opts.maxCandidates || 5),
  };
}
