/**
 * Packet 2.8C-2 — EvidenceUpgradePlanner.
 * PARTIAL claims generate focused decisive-evidence follow-ups — not whole-domain re-runs.
 */

export const EVIDENCE_UPGRADE_PLANNER_VERSION = "evidence-upgrade-planner-v1";

const UPGRADE_RECIPES = Object.freeze({
  OWNERSHIP: {
    missing: "authoritative property-owner / PropCo evidence",
    followup_intent: "DECISIVE_OWNERSHIP_EVIDENCE",
    query_templates: [
      '"{hotel}" propietario OR "S.A." OR Inmobiliaria',
      '"{company}" "reporte anual" hotel OR portafolio',
      '"{hotel}" "{propco}"',
      'site:bmv.com.mx "{company}"',
    ],
  },
  PROPCO: {
    missing: "legal title / PropCo vehicle name",
    followup_intent: "DECISIVE_PROPCO_EVIDENCE",
    query_templates: [
      '"{hotel}" Inmobiliaria OR Holdings OR PropCo OR fideicomiso',
      '"{company}" subsidiaria "{hotel}"',
      '"{hotel}" "S.A." OR "Limited" owner OR propietario',
    ],
  },
  BRAND_HISTORY: {
    missing: "dated first-party conversion / brand chronology evidence",
    followup_intent: "DECISIVE_BRAND_HISTORY_EVIDENCE",
    query_templates: [
      '"{hotel}" rebrand OR conversion OR "formerly" OR "formerly known"',
      '"{brand}" announces OR signed "{hotel}"',
      '"{former_brand}" "{hotel}"',
    ],
  },
  TRANSACTIONS: {
    missing: "exact buyer/seller/property/date relationship",
    followup_intent: "DECISIVE_TRANSACTION_EVIDENCE",
    query_templates: [
      '"{hotel}" acquired OR acquisition OR sale OR sold OR compró OR venta',
      '"{buyer}" "{hotel}" {year}',
      '"{hotel}" "purchase agreement" OR "asset sale"',
    ],
  },
  OPERATOR: {
    missing: "current operator first-party or named management evidence",
    followup_intent: "DECISIVE_OPERATOR_EVIDENCE",
    query_templates: [
      '"{hotel}" "managed by" OR "operated by" OR "Aimbridge" OR operator',
      '"{operator}" "{hotel}" management',
    ],
  },
  PEOPLE: {
    missing: "verified person-level profile + organization/role match",
    followup_intent: "DECISIVE_PERSON_EVIDENCE",
    query_templates: [
      '"{person}" "{organization}"',
      '"{person}" LinkedIn "{organization}"',
      'site:linkedin.com/in "{person}" "{organization}"',
    ],
  },
  DEVELOPMENT: {
    missing: "dated opening / conversion / investment announcement",
    followup_intent: "DECISIVE_DEVELOPMENT_EVIDENCE",
    query_templates: [
      '"{hotel}" opening OR apertura OR conversion OR renovation',
      '"{hotel}" "expected to open" OR "will open"',
    ],
  },
  MARKET: {
    missing: "Dealality/corridor market label from first-party location evidence",
    followup_intent: "DECISIVE_MARKET_EVIDENCE",
    query_templates: [
      '"{hotel}" address OR "Zona Hotelera" OR Expo OR "West End"',
      '"{hotel}" location market',
    ],
  },
});

/**
 * @param {object} claim accepted or partial claim
 * @param {object} ctx hotel/domain context
 */
export function planEvidenceUpgrade(claim = {}, ctx = {}) {
  const domain = String(claim.domain || ctx.domain || "OWNERSHIP").toUpperCase();
  const recipe = UPGRADE_RECIPES[domain] || UPGRADE_RECIPES.OWNERSHIP;
  const hotel = ctx.hotel_name || claim.hotel_name || "";
  const company = ctx.company || ctx.known_owner || "";
  const propco = ctx.propco || ctx.known_propco || "";
  const brand = ctx.brand || ctx.known_brand || "";
  const former = ctx.former_brand || "";
  const operator = ctx.operator || ctx.known_operator || "";
  const person = claim.entities?.[0] || ctx.person || company;
  const year = ctx.year || new Date().getFullYear();

  const fill = (t) =>
    t
      .replaceAll("{hotel}", hotel)
      .replaceAll("{company}", company || hotel)
      .replaceAll("{propco}", propco)
      .replaceAll("{brand}", brand)
      .replaceAll("{former_brand}", former)
      .replaceAll("{operator}", operator)
      .replaceAll("{organization}", company || operator || hotel)
      .replaceAll("{person}", person || "")
      .replaceAll("{year}", String(year))
      .replace(/\s+/g, " ")
      .trim();

  const secondaryHints = extractSourceChainHints(claim);
  const queries = [
    ...recipe.query_templates.map(fill).filter((q) => q.length > 8),
    ...secondaryHints.queries,
  ].slice(0, 5);

  return {
    version: EVIDENCE_UPGRADE_PLANNER_VERSION,
    claim_id: claim.claim_id || null,
    domain,
    current_status: claim.status || "PARTIAL",
    what_is_missing: recipe.missing,
    followup_intent: recipe.followup_intent,
    do_not_rerun: `generic whole-domain research for ${domain}`,
    focused_queries: queries,
    decisive_source_targets: secondaryHints.document_titles.concat(secondaryHints.company_names).slice(0, 8),
    source_chain_leads: secondaryHints,
    exact_capable_if_found: true,
  };
}

export function extractSourceChainHints(claim = {}) {
  const text = `${claim.claim_text || ""} ${(claim.entities || []).join(" ")} ${claim.lead_text || ""}`;
  const company_names = [];
  const document_titles = [];
  const queries = [];
  const govBodies = [];

  const companyRe = /\b([A-Z][A-Za-z0-9&+.'\-\s]{2,40}(?:LLC|Ltd\.?|Limited|S\.A\.(?:B)?\. de C\.V\.|Hospitality|Management|Hotels?|Group|Grupo))\b/g;
  let m;
  while ((m = companyRe.exec(text))) {
    company_names.push(m[1].trim());
  }
  if (/BMV|CNBV|reporte anual|annual report|10-K|hecho relevante/i.test(text)) {
    document_titles.push("issuer annual report / hecho relevante");
    queries.push(`${claim.entities?.[0] || ""} reporte anual OR "annual report"`.trim());
  }
  if (/Registro Público|deed|folio|Tourism Order/i.test(text)) {
    govBodies.push("registry_or_tourism_authority");
  }
  for (const c of company_names.slice(0, 3)) {
    queries.push(`"${c}" investor OR filing OR "reporte anual"`);
  }
  return {
    company_names: [...new Set(company_names)].slice(0, 6),
    document_titles,
    government_bodies: govBodies,
    queries: [...new Set(queries)].slice(0, 4),
  };
}

export function planUpgradesForObservations(observations = [], ctx = {}) {
  return observations
    .filter((o) => String(o.status || "ACCEPTED").toUpperCase() !== "REJECTED")
    .map((o) => planEvidenceUpgrade(o, { ...ctx, domain: o.domain || ctx.domain }));
}
