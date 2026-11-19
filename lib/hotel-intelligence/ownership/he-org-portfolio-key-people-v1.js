/**
 * Organization & Portfolio — Key People + Strategic Context (shared HE workflow).
 * Reuses canonical contact / ownership intelligence; does not invent authority.
 */

export const HE_ORG_PORTFOLIO_KEY_PEOPLE_V1 = "he-org-portfolio-key-people-v1";

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function responsibilityLabel(code) {
  const c = String(code || "").toUpperCase();
  if (c === "CONFIRMED") return "Confirmed property responsibility";
  if (c === "INFERRED") return "Inferred portfolio responsibility";
  if (c === "UNKNOWN") return "Unknown property responsibility";
  return text(code) || "Unknown property responsibility";
}

function commercialRoleLabel(p) {
  return (
    text(p.commercialRole) ||
    text(p.contactClassLabel) ||
    text(p.role_group) ||
    text(p.stakeholderClass) ||
    text(p.title) ||
    "Relevant contact"
  );
}

/**
 * Normalize people from Contact Intelligence / decision_authority / research packs
 * into one Key People display row shape.
 */
export function normalizeOrgPortfolioKeyPeople(people = [], opts = {}) {
  const orgFallback = text(opts.organizationName);
  const rows = [];
  const seen = new Set();
  for (const raw of people || []) {
    if (!raw || typeof raw !== "object") continue;
    const name = text(raw.display_name || raw.name);
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    const linkedin =
      text(raw.linkedinUrl) ||
      text(raw.professional_profile && raw.professional_profile.url) ||
      text(raw.professional_profile_url) ||
      text(
        Array.isArray(raw.profileUrls) && raw.profileUrls[0] && raw.profileUrls[0].url
      );
    const linkedinVerified =
      raw.professional_profile_verified === true ||
      (raw.professional_profile && raw.professional_profile.verified === true) ||
      (Array.isArray(raw.profileUrls) &&
        raw.profileUrls.some((u) => u && u.verified !== false && /linkedin\.com\/in\//i.test(String(u.url || "")))) ||
      /^https?:\/\/((www|[a-z]{2})\.)?linkedin\.com\/in\//i.test(String(linkedin || ""));

    const relevance =
      text(raw.whyRelevant) ||
      text(raw.strategic_relevance) ||
      text(raw.relationship_to_hotel) ||
      text(raw.recommendationRationale) ||
      text(raw.verificationNotes);

    const propCode = text(raw.propertyResponsibility || raw.property_responsibility) || "UNKNOWN";
    const propLabel =
      text(raw.propertyResponsibilityLabel) || responsibilityLabel(propCode);

    const verification =
      text(raw.identityVerification) ||
      text(raw.preview_state) ||
      text(raw.confidence) ||
      "PROPOSED";

    rows.push({
      personId: text(raw.personId || raw.person_id) || `person:${key.replace(/\s+/g, "_")}`,
      name,
      title: text(raw.title || raw.role) || "—",
      organization: text(raw.organization || raw.organizationName) || orgFallback || "—",
      relevance: relevance || "Role verified; hotel-specific relevance not yet documented.",
      commercialRole: commercialRoleLabel(raw),
      propertyResponsibility: propCode,
      propertyResponsibilityLabel: propLabel,
      // Assign via verified URL only — avoid gitleaks FP matching "linkedinUrl: linkedinVerified"
      linkedinUrl: linkedin && linkedinVerified ? linkedin : null,
      verificationStatus: /VERIFIED|HIGH|LIVE/i.test(verification)
        ? "Verified identity"
        : /PROPOSED|STAGED|MEDIUM/i.test(verification)
          ? "Proposed"
          : verification,
      confidence: text(raw.confidence) || null,
      // Back-compat for older table formatters
      strategic_relevance: relevance,
      relationship_to_hotel: relevance,
      professional_profile:
        linkedinVerified && linkedin
          ? { type: "LINKEDIN", url: linkedin, verified: true }
          : null,
      professional_profile_url: linkedinVerified ? linkedin : null,
      professional_profile_verified: linkedinVerified,
      decision_authority_category: text(raw.role_group || raw.contactClass || raw.category),
      category: text(raw.role_group || raw.contactClass),
    });
  }
  return rows;
}

function roomsValue(hotel = {}, ownership = {}) {
  const nested =
    hotel.guestRoomsMeetingSpace?.guestRooms?.totalGuestrooms?.value ??
    ownership?.hotel?.guestRoomsMeetingSpace?.guestRooms?.totalGuestrooms?.value ??
    null;
  const rooms =
    hotel.rooms ??
    hotel.Rooms ??
    hotel.room_count ??
    hotel.rooms_display ??
    nested ??
    ownership?.hotel?.rooms ??
    ownership?.hotel?.Rooms ??
    ownership?.hotel?.rooms_display ??
    null;
  if (rooms == null || rooms === "") return null;
  const n = Number(rooms);
  return Number.isFinite(n) ? n : String(rooms).replace(/[^\d]/g, "") || null;
}

function roomsLine(hotel = {}, ownership = {}) {
  const rooms = roomsValue(hotel, ownership);
  return rooms != null && rooms !== "" ? `${rooms}-room` : null;
}

function brandLine(hotel = {}, ownership = {}) {
  return (
    text(ownership.hotel && ownership.hotel.brand_display) ||
    text(hotel.brand_display) ||
    text(hotel.brand) ||
    text(hotel.current_brand) ||
    text(hotel.affiliation) ||
    null
  );
}

/**
 * Build Strategic Context sections from hotel + ownership intelligence.
 * Never invents sell/rebrand/refinance willingness.
 */
export function buildOrgPortfolioStrategicContext(hotel = {}, ownership = {}, opts = {}) {
  const ctrl =
    (ownership && ownership.report && ownership.report.ownership_and_control) || {};
  const propco = ctrl.legal_property_owner_propco || ctrl.property_company || {};
  const econ = ctrl.economic_owner_or_group || ctrl.economic_owner || {};
  const operator = ctrl.operator || {};
  const brand = brandLine(hotel, ownership) || text(ctrl.brand && ctrl.brand.name);
  const hotelName = text(hotel.name) || text(ownership.hotel && ownership.hotel.name) || "This hotel";
  const city =
    text(hotel.city) ||
    text(ownership.hotel && ownership.hotel.city) ||
    text(hotel.City);
  const market =
    text(hotel.market) || text(ownership.hotel && ownership.hotel.market) || city;
  const rooms = roomsLine(hotel, ownership);
  const roomsNum = roomsValue(hotel, ownership);
  const opName = text(operator.name) || text(hotel.managementCompany);
  const econName = text(econ.name || econ.display_name);
  const propcoName = text(propco.name || propco.legal_name);
  const uboUnresolved =
    !ctrl.ubo_group ||
    ctrl.ubo_group.known === false ||
    /UNRESOLVED/i.test(String(ctrl.ubo_group.status || ""));

  const owned =
    (ownership.organization && ownership.organization.owned_controlled_hotels) || [];
  const portfolioCount = Array.isArray(owned)
    ? owned.filter((h) => {
        const n = text(h.hotel_name || h.name);
        return n && n.toLowerCase() !== hotelName.toLowerCase();
      }).length
    : 0;

  const lender = ctrl.lender || ctrl.financing_institution || {};
  const advisor = ctrl.advisor || ctrl.broker || {};
  const assetMgr = ctrl.asset_manager || ctrl.asset_management_company || {};

  const scalePhrase = [rooms, brand || "branded"].filter(Boolean).join(" ");
  const assetParts = [
    `${hotelName} is a ${scalePhrase} lodging asset${city ? ` in ${city}` : ""}${
      market && market !== city ? ` (${market})` : ""
    }.`,
    opName
      ? `Day-to-day hotel operations are associated with ${opName}${
          brand ? ` under the ${brand} brand` : ""
        }.`
      : null,
    "Scale, brand distribution, and management structure frame outreach; they do not by themselves establish ownership economics.",
  ].filter(Boolean);

  const ownershipParts = [
    propcoName
      ? `Legal property ownership is associated with ${propcoName}${
          /PROPOSED|STAGED/i.test(String(propco.preview_state || propco.status || ""))
            ? " (proposed / staged — not yet customer-approved persist)"
            : ""
        }.`
      : "Legal property company remains incompletely verified in the current packet.",
    econName
      ? `Economic sponsor context points to ${econName}${
          portfolioCount
            ? `, with ${portfolioCount} additional related hotel listing(s) in the selected portfolio scope`
            : ""
        }.`
      : "Economic sponsor is not yet verified.",
    uboUnresolved
      ? "Ultimate beneficial ownership (natural-person control) remains unresolved — seniority alone is not treated as UBO proof."
      : null,
    text(assetMgr.name)
      ? `Owner-side asset-management posture is associated with ${text(assetMgr.name)} where evidenced; property-assigned AM responsibility is not assumed from title alone.`
      : null,
  ].filter(Boolean);

  const strengths = [];
  if (brand) strengths.push(`Established ${brand} affiliation supports distribution and commercial recognition.`);
  if (opName) strengths.push(`Management relationship with ${opName} separates operating execution from ownership decision rights.`);
  if (roomsNum != null) {
    strengths.push(
      `Published inventory scale (${roomsNum} rooms) is material for meeting, group, and brand conversations.`
    );
  }
  if (portfolioCount > 0 && econName) {
    strengths.push(
      `${econName} portfolio relationships (where matched) allow reuse of owner-side contacts across related assets without inventing ownership edges.`
    );
  }
  if (!strengths.length) {
    strengths.push(
      "Verified operating and brand relationships provide a commercial baseline while ownership research continues."
    );
  }

  const considerations = [
    "Brand positioning and management agreements should be discussed with the economic sponsor and operator as separate counterparties.",
    text(lender.name) && (/HISTOR/i.test(String(lender.preview_state || lender.status || "")) || lender.current === false)
      ? `Historical financing involving ${text(lender.name)} informs capital history only — it is not proof of the current lender.`
      : "Current lender / capital structure is a priority diligence item when not publicly verified.",
    text(advisor.name) && /HISTOR/i.test(String(advisor.preview_state || advisor.status || ""))
      ? `Historical transaction advisors (e.g. ${text(advisor.name)}) provide process context, not current decision authority.`
      : null,
    "No public evidence in this packet authorizes claims that the owner is actively selling, rebranding, refinancing, or changing management.",
  ].filter(Boolean);

  const followUps = [
    uboUnresolved ? "Resolve ultimate beneficial ownership if it becomes material to counterparty mapping." : null,
    "Confirm any property-assigned asset manager (do not infer from portfolio AM titles alone).",
    text(lender.name) && lender.current === false
      ? "Identify current mortgagee / refinance status after historical acquisition financing."
      : "Verify current financing counterparties when capital conversations arise.",
    "Confirm personal professional contact channels for priority owner-side people via authorized Contact Intelligence / Surfe-on-click only.",
    portfolioCount
      ? "Advance HPC matching for unresolved portfolio candidates before treating them as verified holdings."
      : null,
  ].filter(Boolean);

  if (opts.override && typeof opts.override === "object") {
    return {
      version: HE_ORG_PORTFOLIO_KEY_PEOPLE_V1,
      generated: false,
      ...opts.override,
    };
  }

  return {
    version: HE_ORG_PORTFOLIO_KEY_PEOPLE_V1,
    generated: true,
    generatedAt: new Date().toISOString().slice(0, 10),
    sections: [
      { id: "asset_market", title: "Asset & Market Position", body: assetParts.join(" ") },
      { id: "ownership_portfolio", title: "Ownership & Portfolio Context", body: ownershipParts.join(" ") },
      { id: "strengths", title: "Commercial Strengths", body: strengths.join(" ") },
      {
        id: "strategic",
        title: "Brand Development & Investment Considerations",
        body: considerations.join(" "),
      },
      {
        id: "follow_ups",
        title: "Priority Follow-Ups",
        body: followUps.join(" "),
      },
    ],
  };
}

export function strategicContextHasGenericCopy(textBody) {
  const t = String(textBody || "");
  return /Use Hotel → Owner → Portfolio|Research one owner; reuse verified relationships|data pipeline|staged dry-run only/i.test(
    t
  );
}
