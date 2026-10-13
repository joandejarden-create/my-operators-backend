/**
 * Country-specific discovery method contracts (framework + starters).
 * Discovery only — no hardcoded conclusions.
 */

export const COUNTRY_METHODS_V2 = "country-contact-methods-v2";

function method({ method_id, country, objective, trigger_conditions, search_templates, preferred_source_types, extraction_rules, confidence_rules, cost_class }) {
  return {
    method_id,
    country,
    objective,
    trigger_conditions,
    search_templates,
    preferred_source_types,
    extraction_rules,
    confidence_rules,
    cost_class,
  };
}

export const COUNTRY_CONTACT_METHODS = Object.freeze([
  method({
    method_id: "mx_owner_leadership_search_v1",
    country: "Mexico",
    objective: "named_owner_side_person",
    trigger_conditions: ["owner_org_known", "domain_optional"],
    search_templates: [
      '"{company}" presidente',
      '"{company}" propietario',
      '"{company}" director general',
      '"{company}" hoteles',
      '"{company}" desarrollador',
      '"{company}" contacto',
      '"{company}" telefono',
      '"{person}" "{company}"',
      '"{person}" email',
    ],
    preferred_source_types: ["OFFICIAL_OWNER_SITE", "NEWS_ARTICLE", "COMPANY_REGISTRY", "PDF_DOCUMENT"],
    extraction_rules: ["leadership_name_title", "mailto", "tel"],
    confidence_rules: ["require_company_cooccurrence"],
    cost_class: "LOW_SERP",
  }),
  method({
    method_id: "co_owner_leadership_search_v1",
    country: "Colombia",
    objective: "named_owner_side_person",
    trigger_conditions: ["owner_org_known"],
    search_templates: [
      '"{company}" representante legal',
      '"{company}" propietario',
      '"{company}" gerente',
      '"{company}" hoteles',
      '"{company}" contacto',
    ],
    preferred_source_types: ["OFFICIAL_OWNER_SITE", "COMPANY_REGISTRY", "NEWS_ARTICLE"],
    extraction_rules: ["leadership_name_title", "mailto"],
    confidence_rules: ["require_company_cooccurrence"],
    cost_class: "LOW_SERP",
  }),
  method({
    method_id: "do_owner_leadership_search_v1",
    country: "Dominican Republic",
    objective: "named_owner_side_person",
    trigger_conditions: ["owner_org_known"],
    search_templates: [
      '"{company}" presidente',
      '"{company}" propietario',
      '"{company}" grupo hotelero',
      '"{company}" contacto',
    ],
    preferred_source_types: ["OFFICIAL_OWNER_SITE", "NEWS_ARTICLE", "HOTEL_ASSOCIATION"],
    extraction_rules: ["leadership_name_title"],
    confidence_rules: ["require_company_cooccurrence"],
    cost_class: "LOW_SERP",
  }),
  method({
    method_id: "cr_owner_leadership_search_v1",
    country: "Costa Rica",
    objective: "named_owner_side_person",
    trigger_conditions: ["owner_org_known"],
    search_templates: [
      '"{company}" presidente',
      '"{company}" propietario',
      '"{company}" desarrollador',
      '"{company}" contacto',
    ],
    preferred_source_types: ["OFFICIAL_OWNER_SITE", "NEWS_ARTICLE"],
    extraction_rules: ["leadership_name_title"],
    confidence_rules: ["require_company_cooccurrence"],
    cost_class: "LOW_SERP",
  }),
]);

export function methodsForCountry(country) {
  const c = String(country || "").toLowerCase();
  return COUNTRY_CONTACT_METHODS.filter((m) => m.country.toLowerCase() === c || c.includes(m.country.toLowerCase().split(" ")[0]));
}

export function expandSearchTemplates(method, { company, person }) {
  return (method.search_templates || []).map((t) =>
    t.replace(/\{company\}/g, company || "").replace(/\{person\}/g, person || "").trim()
  );
}
