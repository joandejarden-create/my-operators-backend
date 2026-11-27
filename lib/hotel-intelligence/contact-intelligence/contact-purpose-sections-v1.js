/**
 * Contact Intelligence CX V2 — purpose-based sections (no global "first contact" hierarchy).
 * A person may appear in multiple sections without duplicating canonical records.
 */

import {
  PROPERTY_RESPONSIBILITY,
  rankContactsForHotel,
} from "./property-decision-maker-engine-v1.js";

export const CONTACT_PURPOSE_SECTIONS_VERSION = "contact-purpose-sections-v1";

export const CONTACT_PURPOSE_SECTION = Object.freeze({
  HOTEL_LEADERSHIP: "hotel_leadership",
  SALES_COMMERCIAL: "sales_commercial",
  OWNERSHIP_ASSET: "ownership_asset",
  MANAGEMENT_COMPANY: "management_company",
  BRAND_DEVELOPMENT: "brand_development",
});

export const CONTACT_PURPOSE_SECTION_META = Object.freeze([
  {
    id: CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP,
    label: "Hotel Leadership",
    description: "Property general management and on-site leadership.",
    showWhenEmpty: true,
    emptyHint:
      "Property general manager and on-site leadership have not been established for this hotel yet.",
  },
  {
    id: CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL,
    label: "Sales & Commercial Leadership",
    description: "Sales, revenue, and commercial performance contacts.",
    showWhenEmpty: true,
    emptyHint:
      "Director of sales / marketing and revenue leadership have not been established for this hotel yet.",
  },
  {
    id: CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET,
    label: "Ownership & Asset Management",
    description:
      "Owner-side and asset-management contacts associated with this hotel’s ownership organization. Appearance here does not confirm hotel-specific decision authority.",
    showWhenEmpty: false,
  },
  {
    id: CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY,
    label: "Management Company & Regional Leadership",
    description: "Operator / management-company and regional leadership associated with the property.",
    showWhenEmpty: false,
  },
  {
    id: CONTACT_PURPOSE_SECTION.BRAND_DEVELOPMENT,
    label: "Brand & Development",
    description: "Brand, franchise, and development contacts when relevant.",
    showWhenEmpty: false,
  },
]);

/** Optional customer filter — does not invent contacts; only filters visible sections. */
export const BUSINESS_PURPOSE_FILTERS = Object.freeze([
  {
    id: "all",
    label: "All contacts",
    sectionIds: CONTACT_PURPOSE_SECTION_META.map((s) => s.id),
  },
  {
    id: "hotel_sales_bd",
    label: "Hotel sales / business development",
    sectionIds: [
      CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL,
      CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP,
      CONTACT_PURPOSE_SECTION.BRAND_DEVELOPMENT,
    ],
  },
  {
    id: "commercial_ops",
    label: "Commercial operations",
    sectionIds: [
      CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP,
      CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL,
      CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY,
    ],
  },
  {
    id: "asset_management",
    label: "Asset management",
    sectionIds: [CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET],
  },
  {
    id: "ownership_outreach",
    label: "Ownership outreach",
    sectionIds: [CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET],
  },
  {
    id: "management_contract",
    label: "Management contract",
    sectionIds: [
      CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY,
      CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET,
      CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP,
    ],
  },
  {
    id: "hotel_development",
    label: "Hotel development",
    sectionIds: [
      CONTACT_PURPOSE_SECTION.BRAND_DEVELOPMENT,
      CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET,
      CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY,
    ],
  },
]);

const HOTEL_LEADERSHIP_RE =
  /\b(general\s*manager|\bgm\b|resident\s*manager|hotel\s*manager|complex\s*manager|director\s*of\s*operations|rooms\s*division)\b/i;
const SALES_COMMERCIAL_RE =
  /\b(director\s*of\s*sales|dosm|director\s*of\s*sales\s*(and|&)\s*marketing|area\s*sales|sales\s*leader|revenue\s*manag|commercial\s*director|director\s*of\s*revenue)\b/i;
const OWNERSHIP_ASSET_RE =
  /\b(owner|ownership|president|ceo|chairman|asset\s*manag|portfolio\s*manag|investments?|economic\s*sponsor)\b/i;
const MANAGEMENT_CO_RE =
  /\b(management\s*company|regional\s*(director|vp|manager)|area\s*(director|vp)|cluster\s*manager|multi[- ]property)\b/i;
const BRAND_DEV_RE =
  /\b(brand\s*development|franchise|development\s*director|openings?|pipeline|affiliation)\b/i;

function blob(person = {}) {
  return [
    person.title,
    person.role,
    person.functionRole,
    person.function_role,
    person.whyRelevant,
    person.why_relevant,
    person.contactClass,
    person.contactClassLabel,
    person.hotelResponsibility,
    person.hotel_commercial_responsibility,
    person.commercialRole,
    person.role_group,
  ]
    .filter(Boolean)
    .join(" ");
}

/**
 * Assign zero or more purpose sections for UI grouping (not canonical duplication).
 */
export function assignPurposeSections(person = {}) {
  const text = blob(person);
  const contactClass = String(person.contactClass || person.contact_class || "").toUpperCase();
  const ids = new Set();

  if (
    HOTEL_LEADERSHIP_RE.test(text) ||
    contactClass === "HOTEL_LEADERSHIP" ||
    contactClass === "PRIMARY_PROPERTY_DECISION_MAKER" ||
    /VERIFIED_PROPERTY_LEADERSHIP|PROPERTY LEADERSHIP\s*[—-]\s*GENERAL/i.test(text)
  ) {
    ids.add(CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP);
  }

  if (
    SALES_COMMERCIAL_RE.test(text) ||
    /PROPERTY LEADERSHIP\s*[—-]\s*SALES|VERIFIED_PROPERTY_COMMERCIAL/i.test(text)
  ) {
    ids.add(CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL);
  }

  if (
    contactClass === "OWNER_DECISION_AUTHORITY" ||
    contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    OWNERSHIP_ASSET_RE.test(text) ||
    /asset management/i.test(text)
  ) {
    // Sales titles should not auto-land in ownership solely via "commercial"
    if (!SALES_COMMERCIAL_RE.test(text) || /asset|owner|invest|president|ceo/i.test(text)) {
      ids.add(CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET);
    }
  }

  if (MANAGEMENT_CO_RE.test(text) || /managed by|management company/i.test(text)) {
    ids.add(CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY);
  }

  // Operator-side hotel GM/DOS also belong under Management Company context when org is operator
  if (
    (ids.has(CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP) ||
      ids.has(CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL)) &&
    /marriott|hilton|hyatt|ihg|accor|operator/i.test(String(person.organization || person.organizationName || ""))
  ) {
    ids.add(CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY);
  }

  if (BRAND_DEV_RE.test(text) || contactClass === "BRAND_DEVELOPMENT") {
    ids.add(CONTACT_PURPOSE_SECTION.BRAND_DEVELOPMENT);
  }

  // Fallback: verified org contacts with unknown role → ownership/asset if owner-org flavored
  if (!ids.size) {
    if (/OWNER|SPONSOR|MULLEN/i.test(String(person.organization || "") + text)) {
      ids.add(CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET);
    } else {
      ids.add(CONTACT_PURPOSE_SECTION.MANAGEMENT_COMPANY);
    }
  }

  return [...ids];
}

function sectionRelevanceScore(person, sectionId) {
  const responsibility = String(
    person.propertyResponsibility || person.property_responsibility || ""
  ).toUpperCase();
  let score = 0;
  if (responsibility === PROPERTY_RESPONSIBILITY.CONFIRMED) score += 100;
  else if (responsibility === PROPERTY_RESPONSIBILITY.INFERRED) score += 40;
  else score += 10;

  const text = blob(person);
  if (sectionId === CONTACT_PURPOSE_SECTION.HOTEL_LEADERSHIP && HOTEL_LEADERSHIP_RE.test(text)) {
    score += 30;
  }
  if (sectionId === CONTACT_PURPOSE_SECTION.SALES_COMMERCIAL && SALES_COMMERCIAL_RE.test(text)) {
    score += 30;
  }
  if (sectionId === CONTACT_PURPOSE_SECTION.OWNERSHIP_ASSET) {
    if (/asset\s*manag/i.test(text)) score += 28;
    if (/\b(ceo|president)\b/i.test(text)) score += 12; // seniority alone is weaker
  }
  // Do NOT boost for email/phone availability — channels must not drive ranking.
  const rankingScore = Number(person.ranking?.score);
  if (Number.isFinite(rankingScore)) {
    // Strip channel points roughly: use ranking as soft tie-break only
    score += Math.min(20, Math.max(0, Math.round(rankingScore / 10)));
  }
  return score;
}

/**
 * Build purpose-grouped sections for the Contact Intelligence directory.
 */
export function buildPurposeGroupedContactSections(people = [], hotelContext = {}) {
  const ranked = rankContactsForHotel(people, hotelContext);
  const withPurposes = ranked.map((p) => ({
    ...p,
    purposeSections: assignPurposeSections(p),
  }));

  const sections = CONTACT_PURPOSE_SECTION_META.map((meta) => {
    const members = withPurposes
      .filter((p) => (p.purposeSections || []).includes(meta.id))
      .slice()
      .sort(
        (a, b) =>
          sectionRelevanceScore(b, meta.id) - sectionRelevanceScore(a, meta.id) ||
          String(a.name || "").localeCompare(String(b.name || "")),
      );
    return {
      id: meta.id,
      label: meta.label,
      description: meta.description,
      people: members,
      empty: members.length === 0,
    };
  });

  return {
    version: CONTACT_PURPOSE_SECTIONS_VERSION,
    ranked: withPurposes,
    sections,
    businessPurposeFilters: BUSINESS_PURPOSE_FILTERS,
    // Legacy fields retained for API compatibility — UI must not render a global first-contact hierarchy.
    recommendation: null,
    recommendedContacts: [],
    otherContacts: withPurposes,
  };
}

export function filterSectionsByBusinessPurpose(sections = [], purposeId = "all") {
  const filter =
    BUSINESS_PURPOSE_FILTERS.find((f) => f.id === purposeId) || BUSINESS_PURPOSE_FILTERS[0];
  const allow = new Set(filter.sectionIds);
  return (sections || []).filter((s) => allow.has(s.id) && !(s.empty && purposeId === "all" ? false : s.empty && !allow.has(s.id)));
}
