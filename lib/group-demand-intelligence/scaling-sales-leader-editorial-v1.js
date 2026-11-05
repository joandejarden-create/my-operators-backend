/**
 * Sales-leader editorial layer for Scaling commercialIntelligenceBrief.
 * Improves shared CIB customer prose from existing evidence — no paid research.
 */
import {
  applyScalingDisplayIdentity,
  buildScalingDisplayIdentity,
  isScalingOpportunity,
} from "./scaling-display-identity-v1.js";
import { buildAuthoritativeHotelResearchContext } from "./research/authoritative-hotel-research-context-v1.js";
import { buildScalingTileCustomerSummary } from "./scaling-tile-customer-summary-v1.js";

export const SCALING_SALES_LEADER_EDITORIAL_V1 =
  "gdi-scaling-sales-leader-editorial-v1";

const INTERNAL_LEAK_PATTERNS = [
  /Selection signal:\s*/i,
  /Dealality selected\b/i,
  /research found dated public growth/i,
  /Scaling inventory\b/i,
  /Scaling Corporate Account\b/i,
  /Do not treat association\/PCO/i,
  /host-overflow sourcing/i,
  /EXA_AGENT_|ORS_|BUDGET_PAUSED|RESEARCH_CONTINUATION/i,
  /authorizeExaAgentLive|GDI_EXA_AGENT/i,
  /Best available public contact for lodging\s*\/\s*meetings\s*\/\s*workplace coordination/i,
  /Reference the dated expansion while local build-out/i,
  /qualification-engine|research-model|disposition token/i,
  /SYNTHETIC_FIXTURE|stageEFreshCompanyMock/i,
];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function distinct(a, b) {
  const x = text(a).toLowerCase();
  const y = text(b).toLowerCase();
  if (!x) return "";
  if (!y) return text(a);
  if (x === y || y.includes(x) || x.includes(y.slice(0, Math.min(60, y.length)))) {
    return "";
  }
  return text(a);
}

/**
 * Detect customer-facing language leaks.
 */
export function detectCustomerLanguageLeaks(content = "") {
  const body = typeof content === "string" ? content : JSON.stringify(content || {});
  const hits = [];
  for (const re of INTERNAL_LEAK_PATTERNS) {
    if (re.test(body)) hits.push(re.source);
  }
  if (/No separately listed established facts beyond the activity narrative/i.test(body)) {
    hits.push("empty_facts_placeholder");
  }
  return {
    version: SCALING_SALES_LEADER_EDITORIAL_V1,
    hasLeaks: hits.length > 0,
    hits,
  };
}

export function scrubCustomerFacingProse(input = "", displayName = "") {
  let s = text(input);
  if (!s) return "";
  s = s.replace(/Selection signal:\s*/gi, "");
  s = s.replace(
    /Dealality selected [^.]+ because research found[^.]*\./gi,
    "",
  );
  s = s.replace(
    /The hotel sales team should treat this as a corporate account-development lead[^.]*\./gi,
    "",
  );
  s = s.replace(
    /Do not treat association\/PCO\/host-overflow sourcing questions as the default[^.]*\./gi,
    "",
  );
  s = s.replace(/\bScaling Corporate Account\b/gi, "corporate account");
  s = s.replace(/\bappears in Scaling inventory\b/gi, "is a corporate account prospect");
  s = s.replace(
    /Best available public contact for lodging\s*\/\s*meetings\s*\/\s*workplace coordination at [^.]+/gi,
    "Relevant public contact for account introduction",
  );
  s = s.replace(
    /Reference the dated expansion while local build-out or operations are observable\.?/gi,
    "Introduce the hotel while local operations remain visible and qualify upcoming travel.",
  );
  // Collapse repeated display/legal name spam at sentence starts
  const name = text(displayName);
  if (name && name.length >= 3) {
    const esc = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`(?:${esc}(?:\\s*[—–-]\\s*[^.]{0,80})?\\s*){2,}`, "gi");
    s = s.replace(re, `${name} `);
  }
  s = s.replace(/\s{2,}/g, " ").replace(/\s+\./g, ".").trim();
  return s;
}

/**
 * Market label for hotel-independent overview — city/market only, never property name.
 */
export function marketLabelForOverview(hotel = {}, opportunity = {}) {
  return (
    text(hotel.city || opportunity.hotelCity || opportunity.marketCity) ||
    text(opportunity.market || opportunity.demandMarket) ||
    "this market"
  );
}

/**
 * Strip property-specific identity from Opportunity overview prose.
 * Keeps market/city geography. Does not touch hotel-specific CIB sections.
 */
export function stripHotelSpecificFromOverview(input = "", hotel = {}) {
  let s = text(input);
  if (!s) return "";
  const names = [
    hotel.displayName,
    hotel.name,
    hotel.hotelName,
    hotel.propertyName,
    "Grand Fiesta Americana Chapultepec",
    "Bethesda Marriott",
  ]
    .map((n) => text(n))
    .filter((n) => n.length >= 4);
  for (const n of [...new Set(names)]) {
    const esc = n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    s = s.replace(new RegExp(`\\bfor\\s+${esc}\\b`, "gi"), "in the market");
    s = s.replace(new RegExp(`\\b${esc}\\b`, "gi"), "a hotel in the market");
  }
  const addrBits = [
    hotel.address,
    hotel.streetAddress,
    hotel.formattedAddress,
    "Campos Elíseos",
    "Campos Eliseos",
  ]
    .map((a) => text(a))
    .filter((a) => a.length >= 6);
  for (const a of [...new Set(addrBits)]) {
    const esc = a.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    s = s.replace(new RegExp(esc, "gi"), "");
  }
  // Property selling-point fragments that belong only in Why this hotel
  s = s.replace(
    /\b(?:candidate property for related transient lodging and meetings|may compete for visiting-staff and meeting demand)[^.]*\./gi,
    "Plausible lodging or meeting demand depends on confirmed travel patterns.",
  );
  s = s.replace(/\s{2,}/g, " ").replace(/\s+\./g, ".").trim();
  return s;
}

/**
 * Hotel-independent Opportunity overview for Scaling CAR briefs.
 * Reusable across hotels in the same market. No property name, address, or selling points.
 * Classification label is shown separately in the drawer header — do not repeat it here.
 */
export function buildHotelIndependentOpportunityOverview(args = {}) {
  const displayName = args.displayName || "";
  const marketCity = args.marketCity || "";
  const chronoObj = args.chrono && typeof args.chrono === "object" ? args.chrono : {};
  const hasContinuityEvidence = args.hasContinuityEvidence;
  const org = text(displayName) || "This company";
  const market = text(marketCity) || "this market";
  const activityDate = text(chronoObj.originalEventDate) || text(chronoObj.originalActivityDate);
  const dateBit = activityDate ? (" (announced " + activityDate + ")") : "";
  const continuity =
    hasContinuityEvidence === true || chronoObj.hasContinuityEvidence === true
      ? " Later public signals support ongoing local presence."
      : "";
  const historical =
    chronoObj.activityStatus === "CONTINUING_ESTABLISHED_OPERATIONS" ||
    chronoObj.activityStatus === "HISTORICAL_ACCOUNT_DEVELOPMENT";

  const marketPhrase =
    !market || /^this market$/i.test(market) ? "local-market" : `${market}-related`;
  let body;
  if (historical) {
    body = `${org} publicly announced ${marketPhrase} expansion activity${dateBit}. That announcement is historical — do not treat it as a brand-new market entry.${continuity} Documented local activity still supports qualifying whether visiting staff, partners, or customer meetings create realistic lodging or meeting demand. Contracting authority, preferred-hotel status, and volume remain unverified.`;
  } else {
    body = `${org} has documented ${marketPhrase} commercial activity${dateBit}. The plausible hotel-demand mechanism is visiting staff, partners, or customer meetings — not a confirmed booking.${continuity} Contracting authority, preferred-hotel status, and guestroom or meeting volume remain unverified.`;
  }
  return scrubCustomerFacingProse(body, org);
}

/**
 * True when overview still names a specific hotel property (regression guard).
 */
export function overviewContainsHotelIdentity(overview = "", hotel = {}) {
  const body = text(overview).toLowerCase();
  if (!body) return false;
  const needles = [
    hotel.displayName,
    hotel.name,
    hotel.hotelName,
    "grand fiesta americana chapultepec",
    "bethesda marriott",
    hotel.address,
    "campos elíseos",
    "campos eliseos",
  ]
    .map((n) => text(n).toLowerCase())
    .filter((n) => n.length >= 4);
  return needles.some((n) => body.includes(n));
}

function sourceRows(opportunity = {}) {
  const cib = opportunity.commercialIntelligenceBrief || {};
  const fromCib = cib.evidenceAndUncertainty?.sources || [];
  const fromOpp = opportunity.sources || opportunity.evidenceUrls || [];
  const rows = [...fromCib, ...fromOpp]
    .map((s) => (typeof s === "string" ? { url: s } : s))
    .filter((s) => text(s?.url) && /^https?:\/\//i.test(s.url))
    .filter((s) => !/^https?:\/\/[^/]+\/?$/i.test(text(s.url)));
  const seen = new Set();
  const out = [];
  for (const s of rows) {
    const key = text(s.url).replace(/\/$/, "").toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      url: s.url,
      title: text(s.title) || null,
      publisher: text(s.publisher) || null,
      publishedDate: text(s.publishedDate || s.publicationDate) || null,
      establishes: text(s.establishes) || null,
      evidenceGroup: s.evidenceGroup || null,
      sourceId: s.sourceId || null,
    });
  }
  return out;
}

/**
 * Editorially select a concise primary evidence list (≈5–8); keep full list separately.
 * Prioritize commercial importance over ingestion order or recency alone.
 */
export function selectConciseSupportingEvidence(sources = [], opts = {}) {
  const minDefault = Number(opts.minDefault) || 5;
  const maxDefault = Number(opts.primaryLimit) || 8;
  const scored = (sources || []).map((s, idx) => {
    let score = 0;
    const blob = `${s.title || ""} ${s.establishes || ""} ${s.evidenceGroup || ""} ${s.url || ""}`.toLowerCase();
    // 1) actual business activity
    if (/business|product|platform|operations|fintech|consulting|payments|office|hq/i.test(blob))
      score += 4;
    // 2) trigger + chronology
    if (/funding|serie|series|invest|opening|expansion|announce|press|launch/i.test(blob))
      score += 5;
    // 3) hotel demand signals
    if (/hotel|meeting|lodging|offsite|workshop|training|travel|visitor|rooms/i.test(blob))
      score += 6;
    // 4) location / facilities / competitive fit
    if (/mexico|cdmx|chapultepec|polanco|santa fe|address|facility|campus/i.test(blob))
      score += 3;
    // 5) named contacts / roles
    if (/contact|people|linkedin|buyer|manager|workplace|facilities/i.test(blob)) score += 4;
    // 6) commercial timing
    if (/2025|2026|timing|deadline|milestone/i.test(blob)) score += 2;
    if (s.publishedDate) score += 1;
    if (s.title && s.title.length > 12) score += 1;
    // Deprioritize bare corporate homepage and legal/privacy pages
    if (/^https?:\/\/[^/]+\/?$/i.test(String(s.url || "").trim())) score -= 4;
    if (/terminos|términos|privacy|cookie|legal|terms-of/i.test(blob)) score -= 5;
    // Stable tie-break: do not prefer later ingestion order
    return { s, score, idx };
  });
  scored.sort((a, b) => b.score - a.score || a.idx - b.idx);

  // Cover major commercial buckets before filling to max
  const buckets = [
    { id: "activity", re: /business|product|operations|fintech|consulting|payments|office/i },
    { id: "trigger", re: /funding|serie|invest|opening|expansion|announce|press/i },
    { id: "demand", re: /hotel|meeting|lodging|offsite|workshop|travel|visitor/i },
    { id: "location", re: /mexico|cdmx|chapultepec|polanco|santa fe|address|facility/i },
    { id: "contact", re: /contact|people|linkedin|buyer|workplace|facilities/i },
  ];
  const picked = [];
  const used = new Set();
  for (const b of buckets) {
    const hit = scored.find(
      ({ s, idx }) =>
        !used.has(idx) &&
        b.re.test(`${s.title || ""} ${s.establishes || ""} ${s.evidenceGroup || ""}`),
    );
    if (hit) {
      used.add(hit.idx);
      picked.push(hit.s);
    }
  }
  for (const row of scored) {
    if (picked.length >= maxDefault) break;
    if (used.has(row.idx)) continue;
    if (row.score < 0 && picked.length >= Math.min(minDefault, scored.length)) continue;
    used.add(row.idx);
    picked.push(row.s);
  }
  // Prefer at least minDefault when enough quality sources exist; never invent.
  const primaryRows =
    scored.length <= maxDefault
      ? scored.filter((r) => r.score >= 0 || picked.includes(r.s)).map((r) => r.s)
      : picked.slice(0, maxDefault);
  const unique = [];
  const urlSeen = new Set();
  for (const s of primaryRows.length ? primaryRows : scored.slice(0, maxDefault).map((r) => r.s)) {
    const key = String(s.url || "")
      .replace(/\/$/, "")
      .toLowerCase();
    if (!key || urlSeen.has(key)) continue;
    urlSeen.add(key);
    unique.push(s);
  }

  const primary = unique.slice(0, maxDefault).map((s) => ({
    url: s.url,
    title: s.title || s.url,
    publisher: s.publisher || null,
    publishedDate: s.publishedDate || null,
    establishes:
      s.establishes && s.establishes.length <= 120 ? s.establishes : null,
  }));
  return {
    primaryEvidenceSources: primary,
    sourcesFull: sources,
    primaryCount: primary.length,
    fullCount: sources.length,
  };
}

function buildFacts(opportunity, displayName, sources) {
  const facts = [];
  const cib = opportunity.commercialIntelligenceBrief || {};
  const activityDate = text(
    cib.evidenceAndUncertainty?.originalActivityDate ||
      opportunity.growthEvidenceDate,
  );
  const activity = scrubCustomerFacingProse(
    cib.companyAndLocalActivity || opportunity.summaryWhat || "",
    displayName,
  );
  if (activityDate) {
    facts.push(`Public local-activity evidence dated ${activityDate}.`);
  }
  const foot = text(
    opportunity.localFacilityAddress ||
      cib.companyBackground?.match?.(/Documented local footprint reference:\s*([^.]+)/)?.[1],
  );
  // Extract footprint from background prose
  const bg = text(cib.companyBackground || cib.companyAndLocalPresence || "");
  const footMatch = bg.match(/Documented local footprint reference:\s*([^.]+)/i);
  if (footMatch?.[1]) {
    facts.push(`Documented local footprint: ${text(footMatch[1])}.`);
  } else if (/Mexico City|CDMX|Ciudad de México/i.test(activity || bg)) {
    facts.push(`${displayName} has documented commercial activity associated with Mexico City.`);
  }
  if (opportunity.primaryContact?.name) {
    facts.push(
      `Named commercial contact on file: ${opportunity.primaryContact.name}${
        opportunity.primaryContact.title || opportunity.primaryContact.role
          ? ` (${opportunity.primaryContact.title || opportunity.primaryContact.role})`
          : ""
      }.`,
    );
  }
  if (opportunity.secondaryContact?.name) {
    facts.push(`Secondary named contact on file: ${opportunity.secondaryContact.name}.`);
  }
  if (sources.some((s) => /funding|serie|series|investment|million/i.test(`${s.title} ${s.establishes}`))) {
    facts.push("Public funding or expansion coverage is on file (see Supporting evidence).");
  }
  // Deduplicate short list
  return [...new Set(facts)].slice(0, 5);
}

function buildUncertainties(opportunity, displayName) {
  const cib = opportunity.commercialIntelligenceBrief || {};
  const raw = [
    ...(cib.evidenceAndUncertainty?.uncertainties || []),
    ...(opportunity.unresolvedResearchTasks || []).map((t) => t.question),
  ]
    .map((u) => scrubCustomerFacingProse(u, displayName))
    .filter(Boolean)
    .filter((u) => !/secondary named contact not yet/i.test(u))
    .filter((u) => !/correct hotel address|campos el[ií]seos/i.test(u))
    .filter((u) => !/No confirmed guestroom contract or RFP/i.test(u));

  const prioritized = [
    "Hotel / travel contracting authority is not verified for the named contacts.",
    "Guestroom or meeting volume is not confirmed — treat demand as a qualified hypothesis.",
    ...raw,
  ];
  // One RFP disclaimer only
  const out = [];
  const seen = new Set();
  for (const u of prioritized) {
    const key = u.toLowerCase().slice(0, 80);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(u);
    if (out.length >= 4) break;
  }
  return out;
}

function demandMechanismCopy(opportunity, displayName, hotelName) {
  const mech = text(opportunity.demandMechanism || "");
  const pot = scrubCustomerFacingProse(
    opportunity.commercialIntelligenceBrief?.potentialBusinessForThisHotel ||
      opportunity.summaryWhyMatters ||
      "",
    displayName,
  );
  const mappedDemand = scrubCustomerFacingProse(
    opportunity.potentialHotelBusiness || opportunity.proposedHotelUse || "",
    displayName,
  );

  const bits = [];
  // Preserve substantive deep-research demand mechanism (incentive / resort / meetings).
  if (
    mappedDemand &&
    mappedDemand.length >= 60 &&
    !/Public growth\/local-activity evidence is on file/i.test(mappedDemand)
  ) {
    bits.push(mappedDemand);
    if (
      /incentive|recognition|dealer|training|resort|all-inclusive|GOLD|passport|president/i.test(
        `${mappedDemand} ${opportunity.whatIsHappening || ""}`,
      )
    ) {
      bits.push(
        `Room nights, contracted venue status, and private-function scope remain unconfirmed — qualify before proposing a block at ${hotelName}.`,
      );
    }
    return bits.join(" ");
  }
  if (/fintech_office|office_opening|customer_meetings/i.test(mech)) {
    bits.push(
      `What business: occasional guestrooms and small meetings for ${displayName}'s own visiting leadership, partner counterparts, and customer-facing teams tied to Mexico operations — not travel booked through ${displayName}'s end-user product.`,
    );
    bits.push(
      `Who travels: internal employees and partner/customer visitors supporting local commercial activity. Demand may be regional (LATAM ↔ Mexico City) or international when HQ/partner teams visit.`,
    );
    bits.push(
      `Pattern: episodic and activity-linked (office milestones, partner sessions), not proven as a standing contracted room block. Evidence makes demand plausible via documented local operations; volume and preferred-hotel status remain unconfirmed.`,
    );
  } else if (/distributed_leadership|implementation|offsite/i.test(mech)) {
    bits.push(
      `What business: episodic Mexico City guestrooms plus compact working sessions or offsites when distributed leadership and implementation teams convene.`,
    );
    bits.push(
      `Who travels: multi-country employees and implementation stakeholders. Demand can be international or regional; public expansion/offsite coverage strengthens the hypothesis but does not confirm bookings at ${hotelName}.`,
    );
    bits.push(
      `Pattern: likely activity-linked rather than weekly recurring. Unconfirmed: who books lodging, typical party size, and whether meetings need catering or only rooms.`,
    );
  } else if (/payments_hq|workshop|partner_visits/i.test(mech)) {
    bits.push(
      `What business: rooms and small meeting space for internal product workshops and partner visits into Mexico City — far more relevant than end-customer payment volume.`,
    );
    bits.push(
      `Who travels: product, partnership, and HQ visitors. Geography is primarily Mexico City–centered with possible international partner travel. Recurrence depends on workshop cadence (unconfirmed).`,
    );
    bits.push(
      `Qualify whether ${displayName} routinely hosts overnight visitors and who owns lodging decisions before pitching volume.`,
    );
  } else if (/consulting_project|partner_meetings/i.test(mech)) {
    bits.push(
      `What business: transient guestrooms, project team nights, and small catering/meeting needs when client work or partner gatherings sit near Chapultepec / Reforma / Polanco.`,
    );
    bits.push(
      `Who travels: consultants, project teams, and client counterparts — often international or multi-city. Demand is project-linked (bursts) rather than a fixed weekly pattern.`,
    );
    bits.push(
      `A distant suburban office alone does not win the stay if meetings occur downtown; conversely, if clients insist on Santa Fe, Chapultepec is conditional. Volume and contracting authority remain unconfirmed.`,
    );
  } else if (pot && pot.length > 80) {
    bits.push(pot);
  } else {
    bits.push(
      `Plausible demand includes overnight stays for visiting staff or partners and small meetings or training sessions at or near ${hotelName}.`,
    );
    bits.push(
      `Traveler mix, rooms vs meetings mix, local vs international origin, and recurrence are not yet confirmed from public research.`,
    );
  }
  bits.push(
    `Do not treat this as a booked opportunity until a travel or meetings owner confirms patterns.`,
  );
  return bits.join(" ");
}

function whyThisHotelCopy(hotelCtx, displayName, hotelName, opportunity) {
  const advantages = [];
  const limitations = [];
  const blob = `${opportunity.commercialIntelligenceBrief?.companyBackground || ""} ${opportunity.summaryWhat || ""} ${opportunity.localFacilityAddress || ""} ${opportunity.activityLocation || ""}`;
  const city = text(hotelCtx?.city || opportunity.hotelCity || "this market");
  const neighborhood =
    text(hotelCtx?.neighborhood || hotelCtx?.submarket || "") ||
    text(hotelCtx?.corridorLabel || "");
  const corridorLabel = neighborhood
    ? neighborhood
    : city
      ? `the commercial core of ${city}`
      : "the hotel's commercial corridor";
  advantages.push(
    `${hotelName} sits in ${corridorLabel} — a realistic base when ${displayName} visitors meet near that corridor rather than only at a distant campus.`,
  );
  advantages.push(
    `Corporate visitors who need guestrooms plus compact meeting space can be pitched on the hotel's location and facilities once itinerary geography is confirmed — not because the company and hotel merely share a city label.`,
  );
  if (/Santa Fe/i.test(blob)) {
    limitations.push(
      `If day-to-day activity is anchored in Santa Fe, hotels nearer that corridor may be more convenient unless meetings or client work pull visitors toward ${corridorLabel}.`,
    );
  } else {
    advantages.push(
      `When itineraries are not locked to a suburban campus, introducing ${hotelName} for rooms and small meetings is a defensible first conversation.`,
    );
  }
  limitations.push(
    `Competing properties closer to a verified office or client site can win on commute convenience; no preferred-hotel agreement or confirmed booking pattern is on file.`,
  );
  limitations.push(
    `The hotel wins when visitor nights and meetings align with ${corridorLabel} geography and when the account still lacks a locked preferred hotel — both conditions need qualification.`,
  );
  const body = [
    `Realistic option: ${hotelName} fits when ${displayName}'s relevant work brings people into ${corridorLabel}${city ? ` (${city})` : ""}.`,
    `Facility match: guestrooms and compact meetings/training are the plausible ask; do not invent travel times or exclusive advantages.`,
    `What could prevent a win: itineraries locked to another corridor, an existing preferred hotel, or no overnight visitor pattern.`,
    `Compelling when: visitors need a commercial address near this property and the account is still open to hotel introductions.`,
  ].join(" ");
  return { body, advantages: advantages.slice(0, 4), limitations: limitations.slice(0, 3) };
}

function whyActNowCopy(opportunity, displayName) {
  const date = text(
    opportunity.commercialIntelligenceBrief?.evidenceAndUncertainty?.originalActivityDate ||
      opportunity.growthEvidenceDate,
  );
  const recent = date && /2025|2026/.test(date);
  if (recent) {
    return `Use the recent public activity (${date}) as a reason to open a qualification conversation now — confirm whether travel and meeting patterns are active, not to pitch a confirmed RFP.`;
  }
  return `This is better framed as proactive corporate account introduction than as an urgent event-driven chase. Qualify whether ${displayName} currently brings visitors into Mexico City before investing heavy pursuit time.`;
}

function contactWhy(person, displayName, which) {
  const grounded = scrubCustomerFacingProse(
    person?.whyRelevantToHotelDemand ||
      person?.whyThisContact ||
      person?.relationshipToOpportunity ||
      "",
    displayName,
  );
  if (
    grounded &&
    grounded.length > 40 &&
    !/Best available public contact for lodging/i.test(grounded) &&
    !/Relevant public contact for account introduction$/i.test(grounded)
  ) {
    const authority =
      person?.hotelSourcingAuthorityVerified === true
        ? " Hotel-sourcing authority is verified."
        : " Hotel-sourcing authority remains unknown — treat as influencer or referral until confirmed.";
    return `${grounded}${authority}`.trim();
  }
  const title = text(person?.title || person?.role);
  const cls = text(
    person?.commercialClass ||
      person?.contactRelevanceLabel ||
      person?.relevanceClass ||
      "",
  );
  if (/workplace|facilities|office manager|partner assistant/i.test(title)) {
    return which === "secondary"
      ? `${title} at ${displayName} is a practical alternate route for visitor logistics and onsite coordination; not verified as the contracting buyer.`
      : `${title} at ${displayName} is relevant because workplace/facilities or office coordination roles often know who hosts visitors and who books lodging — confirm authority before pitching rates.`;
  }
  if (/people|culture|hr|human resources|rewards|benefits/i.test(title)) {
    return `${title} at ${displayName} may influence or refer offsites, onboarding gatherings, or employee travel programs; hotel purchasing authority is not verified.`;
  }
  if (/finance|cfo|treasury|collections/i.test(title)) {
    return `${title} at ${displayName} can often identify supplier-approval paths or budgets, but should not be assumed to be the travel buyer — ask who owns lodging and meetings.`;
  }
  if (title) {
    return which === "secondary"
      ? `${title} at ${displayName} — useful second path to identify who coordinates travel or meetings; contracting authority unverified.`
      : `${title} at ${displayName} — start here to learn who owns visiting-staff lodging and local meetings; contracting authority unverified.`;
  }
  if (/operational_buyer|CONTACT_RELEVANT/i.test(cls)) {
    return `Publicly associated with workplace or commercial operations at ${displayName}; ask who owns hotel and meeting decisions.`;
  }
  if (/influencer|referral/i.test(cls)) {
    return `Useful introduction or referral contact at ${displayName}; not verified as the contracting buyer.`;
  }
  return `Named contact at ${displayName} for account introduction and owner identification; hotel-sourcing authority unknown.`;
}

function nextStepCopy(opportunity, displayName, hotelName) {
  const primary = opportunity.primaryContact;
  const who = primary?.name || `the workplace / people / operations owner at ${displayName}`;
  const title = text(primary?.title || primary?.role);
  const mech = text(opportunity.demandMechanism || "");
  let opportunityHook =
    `overnight stays and small meetings for ${displayName} visitors in Mexico City`;
  if (/consulting_project|partner_meetings/i.test(mech)) {
    opportunityHook =
      `project-team nights and small meetings when client or partner work sits near Chapultepec / Reforma / Polanco`;
  } else if (/payments_hq|workshop/i.test(mech)) {
    opportunityHook = `workshop and partner-visit lodging tied to Mexico City product activity`;
  } else if (/distributed_leadership|offsite/i.test(mech)) {
    opportunityHook = `episodic leadership/implementation stays and compact working sessions in Mexico City`;
  }
  const position =
    `Position ${hotelName} as a Chapultepec-corridor option for rooms and compact meetings when itineraries align — not as a confirmed preferred hotel.`;
  const qualify =
    title && /finance|cfo/i.test(title)
      ? `Ask who approves or coordinates lodging and meetings (not only budgets).`
      : `Ask who coordinates lodging and meetings for visiting teams, and whether a preferred hotel or TMC already exists.`;
  const timing = /2025|2026/.test(
    text(
      opportunity.commercialIntelligenceBrief?.evidenceAndUncertainty?.originalActivityDate ||
        opportunity.growthEvidenceDate,
    ),
  )
    ? `Act now while recent public activity is still a natural conversation opener.`
    : `Act as proactive account introduction; escalate urgency only if a near-term workshop, offsite, or project travel trigger appears.`;

  return {
    nextAction: `Approach ${who}${title ? ` (${title})` : ""} first. Discuss ${opportunityHook}. ${position} Most important qualification: ${qualify} Seek a named travel/meetings owner or a clear referral as the next commercial outcome. ${timing}`,
    qualificationQuestions: [
      `Who owns lodging and meeting decisions for ${displayName} visitors in Mexico City?`,
      `When teams or partners stay overnight near Chapultepec / Reforma / Polanco, do you already have preferred hotels or a travel-management process?`,
    ],
  };
}

/**
 * Apply sales-leader editorial rewrite to a Scaling opportunity using existing evidence only.
 */
export function applySalesLeaderEditorialBrief(opportunity = {}, hotel = {}) {
  if (!isScalingOpportunity(opportunity) && !opportunity.forceEditorial) {
    return opportunity;
  }

  const withIdentity = applyScalingDisplayIdentity(opportunity);
  const identity = buildScalingDisplayIdentity(withIdentity);
  const displayName = identity.displayCompanyName;
  const hotelCtx = buildAuthoritativeHotelResearchContext(hotel);
  const hotelName =
    text(hotel.displayName || hotel.name) || "Grand Fiesta Americana Chapultepec";

  const prevCib = opportunity.commercialIntelligenceBrief || opportunity.salesBrief || {};
  const allSources = sourceRows(opportunity);
  const evidence = selectConciseSupportingEvidence(allSources, { primaryLimit: 8, minDefault: 5 });
  const whyHotel = whyThisHotelCopy(hotelCtx, displayName, hotelName, opportunity);
  const next = nextStepCopy(withIdentity, displayName, hotelName);
  const facts = buildFacts(withIdentity, displayName, allSources);
  const uncertainties = buildUncertainties(withIdentity, displayName);

  // Merge evidence-grounded contact rationale from Stage C buyer candidates when present
  const buyers = [
    ...(opportunity.buyerCandidates || []),
    ...(opportunity.contactPackage?.buyerCandidates || []),
    ...(opportunity.sanitized?.buyerCandidates || []),
  ];
  function enrichContactFromBuyers(person) {
    if (!person?.name) return person;
    const match = buyers.find(
      (b) =>
        text(b.fullName || b.name).toLowerCase() === text(person.name).toLowerCase(),
    );
    if (!match) return person;
    return {
      ...person,
      title: person.title || person.role || match.role || null,
      role: person.role || match.role || null,
      profileUrl: person.profileUrl || person.linkedinUrl || match.sourceUrl || null,
      linkedinUrl: person.linkedinUrl || match.sourceUrl || null,
      whyRelevantToHotelDemand:
        person.whyRelevantToHotelDemand || match.whyRelevantToHotelDemand || null,
      relevanceClass: person.relevanceClass || match.relevanceClass || null,
      email: person.email || match.email || null,
      phone: person.phone || match.phone || null,
    };
  }

  const activityRaw = scrubCustomerFacingProse(
    prevCib.companyAndLocalActivity || opportunity.summaryWhat || "",
    displayName,
  );
  // Prefer narrative that doesn't restart with full legal name
  let whatHappening = activityRaw
    .replace(new RegExp(`^${text(opportunity.organizationName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\.?\\s*`, "i"), "")
    .replace(new RegExp(`^${displayName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\.?\\s*`, "i"), "");
  // Drop orphaned legal-entity fragments left after stripping the display name
  whatHappening = whatHappening
    .replace(
      /^[—–\-,:;]*\s*(?:S\.?\s*de\s*R\.?\s*L\.?[^.]*|S\.?\s*A\.?\s*de\s*C\.?\s*V\.?[^.]*|PayClip[^.]*|Payclip[^.]*|S\.?\s*C\.?)\s*/i,
      "",
    )
    .replace(/^,\s*/, "")
    .trim();
  const date = text(
    prevCib.evidenceAndUncertainty?.originalActivityDate ||
      opportunity.growthEvidenceDate,
  );
  const looksBroken =
    !whatHappening ||
    /^on\s*,/i.test(whatHappening) ||
    /^[.,;]/.test(whatHappening) ||
    /^(?:S\.?\s*)?(?:de\s*)?(?:R\.?\s*L\.?|A\.?\s*de\s*C\.?\s*V\.?|V\.;)/i.test(
      whatHappening,
    ) ||
    /^Payclip|^PayClip/i.test(whatHappening);
  if (looksBroken) {
    whatHappening = date
      ? `On ${date}, public materials described ${displayName}'s local commercial activity in Mexico City. Continuity and exact travel patterns still need qualification with the account.`
      : `${displayName} has documented commercial activity relevant to Mexico City. Public materials describe ongoing operations; exact travel patterns still need qualification with the account.`;
  } else if (!/^[A-Z]/.test(whatHappening)) {
    whatHappening = whatHappening.charAt(0).toUpperCase() + whatHappening.slice(1);
  }
  // Remove selection / research narration leftovers
  whatHappening = scrubCustomerFacingProse(whatHappening, displayName);

  const overview = buildHotelIndependentOpportunityOverview({
    displayName,
    marketCity: marketLabelForOverview(hotel, withIdentity),
    chrono: {
      originalEventDate:
        prevCib.evidenceAndUncertainty?.originalActivityDate ||
        opportunity.growthEvidenceDate ||
        null,
      activityStatus: prevCib.evidenceAndUncertainty?.activityStatus || null,
      hasContinuityEvidence:
        prevCib.evidenceAndUncertainty?.hasContinuityEvidence === true,
    },
  });

  const primaryBase = enrichContactFromBuyers(withIdentity.primaryContact);
  const secondaryBase = enrichContactFromBuyers(withIdentity.secondaryContact);
  const primary = primaryBase
    ? {
        ...primaryBase,
        organization: displayName,
        whyThisContact: contactWhy(primaryBase, displayName, "primary"),
        hotelSourcingAuthorityVerified: primaryBase.hotelSourcingAuthorityVerified === true,
        contactRouteType:
          primaryBase.email || primaryBase.phone
            ? "NAMED_DIRECT"
            : primaryBase.profileUrl || primaryBase.linkedinUrl
              ? "NAMED_PROFILE"
              : "NAMED_PARTIAL",
      }
    : null;
  const secondary = secondaryBase
    ? {
        ...secondaryBase,
        organization: displayName,
        whyThisContact: contactWhy(secondaryBase, displayName, "secondary"),
        hotelSourcingAuthorityVerified: secondaryBase.hotelSourcingAuthorityVerified === true,
      }
    : null;

  const cib = {
    ...prevCib,
    version: prevCib.version || "gdi-scaling-commercial-intelligence-brief-v1",
    editorialVersion: SCALING_SALES_LEADER_EDITORIAL_V1,
    displayCompanyName: displayName,
    legalEntityName: identity.legalEntityName,
    opportunityOverview: overview,
    // Selection rationale is internal-only — never customer CIB field
    whyDealalitySelectedThisCompany: null,
    selectionSignals: null,
    companyAndLocalActivity: whatHappening,
    potentialBusinessForThisHotel: demandMechanismCopy(
      withIdentity,
      displayName,
      hotelName,
    ),
    whyThisHotel: whyHotel.body,
    whyThisHotelAdvantages: whyHotel.advantages,
    whyThisHotelLimitations: whyHotel.limitations,
    whyActNow: whyActNowCopy(withIdentity, displayName),
    whoToContact: {
      ...(prevCib.whoToContact || {}),
      primaryNamed: primary?.name || null,
      secondaryNamed: secondary?.name || null,
      coverage: secondary?.name
        ? "PRIMARY_AND_SECONDARY_NAMED"
        : primary?.name
          ? "PRIMARY_NAMED"
          : "RESEARCH_INCOMPLETE",
      note: primary
        ? `Primary ${primary.name}; contracting authority not verified.`
        : "Named-person research incomplete.",
    },
    recommendedSalesApproach: {
      ...(prevCib.recommendedSalesApproach || {}),
      suggestedInitialTarget: primary?.name || `operations / people owner at ${displayName}`,
      nextAction: next.nextAction,
      qualificationQuestions: next.qualificationQuestions,
      timingRationale: whyActNowCopy(withIdentity, displayName),
      purposeOfOutreach: `Introduce ${hotelName} and identify who owns visiting-staff lodging and local meetings for ${displayName}.`,
      commercialAngle: `Connect ${hotelName} to documented ${displayName} activity without overstating confirmed demand.`,
    },
    evidenceAndUncertainty: {
      ...(prevCib.evidenceAndUncertainty || {}),
      establishedFacts: facts,
      uncertainties,
      sources: evidence.primaryEvidenceSources,
      sourcesFull: evidence.sourcesFull,
      primaryEvidenceCount: evidence.primaryCount,
      fullEvidenceCount: evidence.fullCount,
      activityStatus: prevCib.evidenceAndUncertainty?.activityStatus || null,
      originalActivityDate:
        prevCib.evidenceAndUncertainty?.originalActivityDate || null,
    },
  };

  const nextOpp = {
    ...withIdentity,
    title: identity.drawerTitle,
    displayTitle: identity.drawerTitle,
    displayCompanyName: displayName,
    legalEntityName: identity.legalEntityName,
    segmentDisplayLabel: identity.segmentDisplayLabel,
    opportunityTypeLabel:
      identity.opportunityTypeLabel || "Corporate Account Relationship",
    opportunityType:
      withIdentity.opportunityType || "CORPORATE_ACCOUNT_RELATIONSHIP",
    commercialIntelligenceBrief: cib,
    salesBrief: cib,
    primaryContact: primary,
    secondaryContact: secondary,
    sources: evidence.primaryEvidenceSources,
    evidenceUrls: evidence.primaryEvidenceSources,
    sourcesFull: evidence.sourcesFull,
    summaryWhat: whatHappening,
    tileCustomerSummary: buildScalingTileCustomerSummary({
      ...withIdentity,
      displayCompanyName: displayName,
      commercialIntelligenceBrief: cib,
      summaryWhat: whatHappening,
      summaryWhyMatters: cib.potentialBusinessForThisHotel,
    }),
    summaryWhyHotel: whyHotel.body,
    summaryWhyMatters: cib.potentialBusinessForThisHotel,
    salesActionRecommended: next.nextAction,
    stageFEditorial: true,
    stageFEditorialVersion: SCALING_SALES_LEADER_EDITORIAL_V1,
    // Preserve selection rationale for audit — not rendered in customer drawer
    internalResearchNotes: {
      ...(opportunity.internalResearchNotes || {}),
      whyDealalitySelectedThisCompany:
        prevCib.whyDealalitySelectedThisCompany || null,
      selectionSignals: prevCib.selectionSignals || null,
    },
  };

  const customerFacingPayload = {
    overview: cib.opportunityOverview,
    what: cib.companyAndLocalActivity,
    pot: cib.potentialBusinessForThisHotel,
    why: cib.whyThisHotel,
    advantages: cib.whyThisHotelAdvantages,
    limitations: cib.whyThisHotelLimitations,
    act: cib.whyActNow,
    next: next.nextAction,
    questions: next.qualificationQuestions,
    facts: cib.evidenceAndUncertainty?.establishedFacts,
    uncertainties: cib.evidenceAndUncertainty?.uncertainties,
    primaryWhy: primary?.whyThisContact,
    secondaryWhy: secondary?.whyThisContact,
  };
  const leakScan = detectCustomerLanguageLeaks(customerFacingPayload);

  return {
    ...nextOpp,
    stageFEditorialLeakScan: leakScan,
  };
}

export { INTERNAL_LEAK_PATTERNS };
