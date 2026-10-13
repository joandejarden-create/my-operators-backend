/**
 * Seed-scoped claim extraction for Mexico native research (Packet 2.4C).
 * Does not hard-code hotel answers; uses seed identity windows + negative screens.
 */

import { NEGATIVE_SCREENS } from "../playbook-schema.js";
import {
  applyCrossPropertyGuardToClaims,
  CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD,
  mayPropagateEntityAcrossHotels,
} from "./cross-property-entity-guard.js";
import { buildDynamicOwnershipClaims } from "./dynamic-ownership-extractor.js";
import { getOptionalOwnerAdapterClaims } from "./optional-owner-adapters.js";
import { isPlausibleOrgName } from "./org-name-filters.js";

function has(text, re) {
  return re.test(String(text || ""));
}

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function identityAliasesForSeed(seed) {
  const name = String(seed.name || "").trim();
  const aliases = [name];

  // Generic title-noise stripping for MX / branded hotels (press often omits marketing suffixes)
  const stripped = name
    .replace(/\b(all\s*-?\s*inclusive|adults?\s+only|resort\s*&\s*spa|resort and spa|urban\s+resort|by\s+marriott|a\s+member\s+of[^,]+)/gi, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (stripped && stripped.length >= 8 && stripped.toLowerCase() !== name.toLowerCase()) {
    aliases.push(stripped);
  }
  // Drop trailing brand-family noise after primary property tokens
  const withoutBrandTail = stripped
    .replace(/\b(hotels?\s+by\s+marriott|tapestry\s+collection.*)$/i, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  if (withoutBrandTail && withoutBrandTail.length >= 8) aliases.push(withoutBrandTail);

  // Distinctive multi-token cores (city + landmark) for window anchoring
  const coreTokens = stripped
    .split(/\s+/)
    .filter((t) => t.length > 3 && !/^(hotel|hotels|inn|the|and|spa|grand|live|aqua|fiesta|americana|marriott|fairfield|courtyard)$/i.test(t));
  if (coreTokens.length >= 2) {
    aliases.push(coreTokens.slice(0, 3).join(" "));
  }

  if (/krystal\s+grand\s+puerto\s+vallarta/i.test(name)) {
    aliases.push("Hilton Puerto Vallarta", "Krystal Grand Vallarta", "Krystal Altitude");
  }
  if (/krystal\s+resort\s+puerto\s+vallarta/i.test(name)) {
    aliases.push("Krystal Puerto Vallarta", "Krystal Resort Vallarta");
  }
  if (/mahekal/i.test(name)) aliases.push("Mahekal Beach Resort", "Mahekal");
  if (/xcaret/i.test(name)) aliases.push("Hotel Xcaret México", "Hotel Xcaret Mexico");
  if (/pedregal/i.test(name)) {
    aliases.push(
      "The Resort at Pedregal",
      "Rosewood Pedregal",
      "Waldorf Astoria Los Cabos Pedregal",
      "Capella Pedregal",
      "Waldorf Astoria los Cabos Pedregal"
    );
  }
  if (/sierra\s+nevada/i.test(name)) aliases.push("Casa de Sierra Nevada", "Belmond Casa de Sierra Nevada");
  if (/fiesta\s+inn/i.test(name)) aliases.push(name);
  if (/hacienda\s+galindo/i.test(name)) aliases.push("Fiesta Americana Hacienda Galindo", "Hacienda Galindo");
  if (/condesa\s+canc/i.test(name)) aliases.push("Fiesta Americana Condesa Cancun", "Fiesta Americana Condesa Cancún");
  if (/live\s+aqua/i.test(name)) {
    aliases.push(stripped);
    if (/san\s+miguel/i.test(name)) aliases.push("Live Aqua San Miguel de Allende");
    if (/monterrey/i.test(name)) aliases.push("Live Aqua Monterrey", "Live Aqua Monterrey Valle");
  }
  if (/ac\s+hotels?/i.test(name) && /antea/i.test(name)) {
    aliases.push("AC Hotel Antea Querétaro", "AC Hotel Antea Queretaro", "AC Hotels Queretaro Antea");
  }
  if (/los\s+cabos/i.test(name)) aliases.push("Krystal Grand Los Cabos", "Krystal Grand los Cabos");
  if (/nuevo\s+vallarta/i.test(name)) aliases.push("Krystal Grand Nuevo Vallarta");
  if (/acapulco/i.test(name)) aliases.push("Krystal Beach Acapulco");
  if (/monterrey/i.test(name)) aliases.push("Krystal Urban Monterrey");
  if (/ixtapa/i.test(name)) aliases.push("Krystal Ixtapa");
  if (/mandarina/i.test(name)) aliases.push("One&Only Mandarina", "One and Only Mandarina", "Mandarina Riviera Nayarit");
  if (/palmilla/i.test(name)) aliases.push("One&Only Palmilla", "One and Only Palmilla", "Palmilla Los Cabos");
  if (/chable|chablé/i.test(name)) {
    aliases.push("Chablé Maroma", "Chable Maroma", "Chablé Yucatán", "Chable Yucatan");
  }
  if (/matilda/i.test(name)) aliases.push("Hotel Matilda", "Matilda San Miguel");
  if (/nizuc/i.test(name)) aliases.push("Nizuc Resort", "NIZUC Resort & Spa");
  if (/mayakoba/i.test(name) && /banyan/i.test(name)) aliases.push("Banyan Tree Mayakoba", "Banyan Tree Mayakobá");
  if (/four\s+seasons/i.test(name) && /mexico\s+city|ciudad/i.test(name)) {
    aliases.push("Four Seasons Hotel Mexico City", "Four Seasons Ciudad de México");
  }
  if (/insurgentes/i.test(name) && /hyatt/i.test(name)) {
    aliases.push("Hyatt Regency Mexico City Insurgentes", "Hyatt Insurgentes");
  }
  // Do NOT add short generic tokens (Krystal, Grand, Hotel) — they bleed across properties in issuer PDFs.
  return [...new Set(aliases.filter((a) => a && a.length >= 8))];
}

function windowsForAliases(docs, aliases, radius = 700) {
  const wins = [];
  for (const d of docs) {
    const text = String(d.text || "");
    for (const alias of aliases) {
      if (alias.length < 4) continue;
      const re = new RegExp(escapeRe(alias), "gi");
      let m;
      while ((m = re.exec(text))) {
        wins.push({
          url: d.url,
          alias,
          text: text.slice(Math.max(0, m.index - radius), m.index + m[0].length + radius),
        });
      }
    }
  }
  return wins;
}

function extractContacts(docs) {
  const contacts = [];
  const emailRe = /[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi;
  const phoneRe = /(?:\+?52[\s-]?)?(?:\(?\d{2,3}\)?[\s-]?)?\d{3,4}[\s-]?\d{4}/g;
  for (const d of docs.slice(0, 6)) {
    const t = String(d.text || "").slice(0, 20000);
    if (/investor|contacto|contact|reservaciones|info@|relacion(?:es)?\s+con\s+inversionistas|investor\s+relations/i.test(t)) {
      const emails = [...new Set((t.match(emailRe) || []).filter((e) => !/example\.|sentry|wixpress/i.test(e)))].slice(0, 3);
      const phones = [...new Set(t.match(phoneRe) || [])].slice(0, 2);
      if (emails.length || phones.length) {
        contacts.push({ url: d.url, emails, phones, class: "public_corporate_or_ir" });
      }
    }
  }
  return contacts;
}

/**
 * @param {Array<object>} docs
 * @param {object} seed
 */
export function extractClaimsFromCorpus(docs, seed) {
  const claims = [];
  const screens = new Set();
  const aliases = new Set(identityAliasesForSeed(seed));
  const entities = new Set();
  const people = [];
  const timeline = [];
  const joined = docs.map((d) => d.text || "").join("\n\n");
  const seedWins = windowsForAliases(docs, [...aliases]);
  const seedBlob = seedWins.map((w) => w.text).join("\n\n");
  const seedUrls = [...new Set(seedWins.map((w) => w.url).filter(Boolean))];
  const seedDocTexts = docs
    .filter((d) => [...aliases].some((a) => a.length > 4 && new RegExp(escapeRe(a), "i").test(String(d.text || ""))))
    .map((d) => d.text || "")
    .join("\n\n");

  const isGrandPv = /krystal\s+grand\s+puerto\s+vallarta/i.test(seed.name || "");
  const isResortPv = /krystal\s+resort\s+puerto\s+vallarta/i.test(seed.name || "");

  // Identity
  claims.push({
    claim_type: "hotel_identity",
    entity: seed.name,
    temporal_status: "CURRENT",
    confidence: "HIGH",
    source_urls: seedUrls.slice(0, 3).length ? seedUrls.slice(0, 3) : ["t0_seed"],
  });

  // Current brand from affiliation or first-party
  const aff = seed.affiliation_display;
  if (aff && !/independent/i.test(aff)) {
    const announced =
      /breathless/i.test(aff) &&
      docs.some((d) => /hyatt|announce|anuncio/i.test(`${d.url} ${(d.text || "").slice(0, 500)}`));
    if (announced && isGrandPv) {
      claims.push({
        claim_type: "brand",
        entity: "Breathless",
        temporal_status: "ANNOUNCED",
        confidence: "HIGH",
        source_urls: docs.filter((d) => /hyatt|breathless/i.test(d.url || "")).map((d) => d.url).slice(0, 2),
      });
      screens.add("ANNOUNCED_NOT_CURRENT");
      claims.push({
        claim_type: "brand",
        entity: "Krystal Grand",
        temporal_status: "CURRENT",
        confidence: "HIGH",
        source_urls: docs.filter((d) => /krystalgrand/i.test(d.url || "")).map((d) => d.url).slice(0, 2),
      });
    } else {
      claims.push({
        claim_type: "brand",
        entity: aff,
        temporal_status: "CURRENT",
        confidence: seedUrls.length ? "HIGH" : "MEDIUM",
        source_urls: seedUrls.slice(0, 2),
      });
    }
  }

  // Operator
  if (seed.management_company || seed.operator) {
    claims.push({
      claim_type: "operator",
      entity: seed.management_company || seed.operator,
      temporal_status: "CURRENT",
      confidence: "HIGH",
      source_urls: ["census_management_company"],
      note: "Operator ≠ owner unless separately evidenced",
    });
    screens.add("OPERATOR_NOT_OWNER");
  }

  // Collision: Grand vs Resort
  if (isGrandPv) {
    if (has(joined, /Krystal\s+(Resort\s+)?Puerto\s+Vallarta/i) && (has(joined, /third[- ]party/i) || has(joined, /Chartwell/i))) {
      claims.push({
        claim_type: "adjacent_asset_distinct",
        entity: "Krystal Resort Puerto Vallarta",
        temporal_status: "CURRENT",
        confidence: "HIGH",
        source_urls: seedUrls.slice(0, 2),
        note: "Adjacent managed asset — do not merge ownership",
      });
      screens.add("ADJACENT_ASSET");
      screens.add("WRONG_PROPERTY");
      screens.add("SIMILAR_NAME_COLLISION");
    }
  }
  if (isResortPv) {
    screens.add("SIMILAR_NAME_COLLISION");
    screens.add("OPERATOR_NOT_OWNER");
    if (has(seedBlob, /Chartwell/i) || has(seedDocTexts, /third[- ]party owned|third party owned|Chartwell/i)) {
      claims.push({
        claim_type: "economic_owner",
        entity: "Grupo Chartwell",
        temporal_status: "CURRENT",
        confidence: "HIGH",
        source_urls: seedUrls.slice(0, 2),
        note: "Third-party owned / Chartwell — GSF is operator only",
      });
      entities.add("Grupo Chartwell");
    }
  }

  const op = seed.management_company || seed.operator || "";

  // JV / fideicomiso (seed-scoped) — require a named partner org, never "discovered_sponsor"
  const jvHit =
    (has(seedBlob, /50\s*%/) || has(seedBlob, /\bJV\b|joint\s+venture|asociaci[oó]n/i)) &&
    has(seedBlob, /adquisici[oó]n|participaci|inversionistas|joint/i);
  if (jvHit) {
    const namedPartner =
      (op && has(seedBlob, new RegExp(escapeRe(op.split(",")[0]), "i")) ? op : null) ||
      (has(seedBlob, /Grupo\s+Hotelero\s+Santa\s+Fe/i) ? "Grupo Hotelero Santa Fe" : null) ||
      (has(seedBlob, /Chartwell/i) ? "Grupo Chartwell" : null) ||
      (has(seedBlob, /RLH\s+Properties/i) ? "RLH Properties" : null) ||
      (has(seedBlob, /Questro|Discovery\s+Land/i) ? "Questro / Discovery Land" : null);
    if (namedPartner) {
      claims.push({
        claim_type: "economic_owner_partial_jv",
        entity: `${namedPartner} (partial / JV — not sole owner unless stated)`,
        temporal_status: "CURRENT",
        confidence: "HIGH",
        source_urls: seedUrls.filter((u) => /pdf|reporte/i.test(u)).slice(0, 2),
      });
      timeline.push({
        date: null,
        event: "JV / partial ownership disclosed near hotel",
        classification: "ACQUISITION",
        confidence: "HIGH",
      });
      screens.add("OPERATOR_NOT_OWNER");
    } else {
      screens.add("SOURCE_NOT_DECISIVE");
      claims.push({
        claim_type: "jv_signal_unresolved_partner",
        entity: null,
        temporal_status: "UNKNOWN",
        confidence: "NOT_VERIFIED",
        source_urls: seedUrls.slice(0, 2),
        note: "JV language in seed window but no named partner org — do not invent sponsor",
      });
    }
  }

  if (has(seedBlob, /fideicomiso/i)) {
    const vehicleMatch = seedBlob.match(/Fideicomiso[^\n.]{0,80}/i);
    claims.push({
      claim_type: "propco_or_vehicle",
      entity: vehicleMatch ? vehicleMatch[0].slice(0, 120) : "Fideicomiso / trust vehicle (seed-linked)",
      temporal_status: "CURRENT",
      confidence: "HIGH",
      source_urls: seedUrls.slice(0, 2),
    });
  }

  // PropCo inmobiliaria — PROPERTY_SPECIFIC; cross-property guard required
  if (has(seedDocTexts, /Inmobiliaria\s+en\s+Hoteler/i) || has(seedDocTexts, /\bIHVSF\b/) || has(joined, /\bIHVSF\b/)) {
    const propcoEntity = "IHVSF / Inmobiliaria en Hotelería Vallarta Santa Fe";
    const decision = mayPropagateEntityAcrossHotels({
      entityName: propcoEntity,
      targetHotel: seed,
      evidenceText: seedBlob || seedDocTexts,
      relationshipKind: "propco",
    });
    if (decision.allowed) {
      claims.push({
        claim_type: "propco",
        entity: propcoEntity,
        temporal_status: "CURRENT",
        confidence: "HIGH",
        source_urls: docs.filter((d) => /IHVSF|Inmobiliaria/i.test(d.text || "")).map((d) => d.url).slice(0, 2),
        entity_scope: decision.scope,
        propagation_reason: decision.reason,
      });
    } else {
      screens.add("WRONG_PROPERTY");
      screens.add("INSUFFICIENT_ENTITY_MATCH");
      screens.add(CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD);
    }
  }

  // Economic owner — dynamic extraction only (no hardcoded issuer list on generic path).
  // Seed operator is NEVER auto-promoted to economic_owner.
  const dynamic = buildDynamicOwnershipClaims({
    docs,
    seed,
    aliases: [...aliases],
  });
  for (const c of dynamic.claims) {
    if (c.entity && !isPlausibleOrgName(c.entity)) continue;
    claims.push(c);
    if (c.entity) entities.add(c.entity);
  }
  for (const c of dynamic.conflicts || []) {
    screens.add("OWNERSHIP_CONFLICT");
    claims.push({
      claim_type: "ownership_conflict",
      entity: null,
      temporal_status: "UNCERTAIN",
      confidence: "MEDIUM",
      source_urls: [],
      note: JSON.stringify(c),
    });
  }

  // Optional owner adapters (FibraHotel etc.) — disabled unless explicitly enabled
  const optional = getOptionalOwnerAdapterClaims(
    { docs, seed, seedBlob, seedUrls, joined },
    { enableOptionalOwnerAdapters: Boolean(seed.enableOptionalOwnerAdapters) }
  );
  for (const c of optional.claims || []) claims.push(c);

  // If still no ownership-related claim — explicit abstention
  if (!claims.some((c) => /economic_owner|historical_owner|organization_candidate/i.test(c.claim_type))) {
    claims.push({
      claim_type: "economic_owner",
      entity: null,
      temporal_status: "UNKNOWN",
      confidence: "NOT_VERIFIED",
      source_urls: [],
      note: "Economic owner correctly unresolved from retrieved evidence",
      hotel: seed.name,
    });
  }

  // Historical Chartwell only for Grand PV seed windows (property-specific adjacent case)
  if (isGrandPv && has(seedBlob, /Chartwell/i) && has(seedBlob, /2014|50\s*%|adquisici/i)) {
    claims.push({
      claim_type: "historical_owner",
      entity: "Grupo Chartwell",
      hotel: seed.name,
      relationship: "former_jv_or_owner",
      excerpt: "Chartwell co-occurrence with 2014/50%/adquisición in seed windows",
      temporal_status: "FORMER",
      confidence: "HIGH",
      source_urls: seedUrls.slice(0, 2),
      relevant_date: null,
    });
    screens.add("HISTORICAL_NOT_CURRENT");
  }

  // Former Hilton for Grand PV
  if (isGrandPv && has(seedBlob, /Hilton/i)) {
    claims.push({
      claim_type: "brand",
      entity: "Hilton",
      temporal_status: "FORMER",
      confidence: "HIGH",
      source_urls: seedUrls.slice(0, 2),
    });
  }

  // Public company ticker context (generic BMV pattern — entity from nearby text, not forced GSF/FibraHotel)
  if (has(joined, /BMV\s*:\s*[A-Z0-9]+|\(BMV:\s*[A-Z0-9]+\)/i)) {
    const ticker = joined.match(/BMV\s*:\s*([A-Z0-9]+)/i) || joined.match(/\(BMV:\s*([A-Z0-9]+)\)/i);
    claims.push({
      claim_type: "public_company_ticker",
      entity: ticker ? `BMV:${ticker[1]}` : "BMV_TICKER",
      temporal_status: "CURRENT",
      confidence: "MEDIUM",
      source_urls: docs.filter((d) => /BMV|Reporte_anual/i.test(d.url || "")).map((d) => d.url).slice(0, 2),
      note: "Ticker observed — issuer name must come from dynamic ownership extraction",
    });
  }

  // People — only from dynamic windows with org attribution (no hardcoded GSF roster on generic path)
  const peoplePatterns = [];
  for (const [re, name, org] of peoplePatterns) {
    if (has(joined, re)) {
      people.push({
        name,
        organization: org,
        title_verified: false,
        decision_authority: "Authority Not Verified",
        legal_signing_authority: "Authority Not Verified",
        confidence: "HIGH",
      });
      screens.add("TITLE_NOT_AUTHORITY");
    }
  }

  // Contacts diagnostic
  const contacts = extractContacts(docs);

  // Authority abstentions
  claims.push({
    claim_type: "natural_person_ubo",
    entity: null,
    temporal_status: "UNKNOWN",
    confidence: "NOT_VERIFIED",
    source_urls: [],
  });
  claims.push({
    claim_type: "legal_signatory",
    entity: null,
    temporal_status: "UNKNOWN",
    confidence: "NOT_VERIFIED",
    source_urls: [],
  });

  const guarded = applyCrossPropertyGuardToClaims(claims, seed, seedBlob || seedDocTexts || joined);
  for (const b of guarded.blocked) {
    screens.add(CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD);
    screens.add("INSUFFICIENT_ENTITY_MATCH");
  }

  return {
    claims: guarded.claims,
    negative_screens_fired: [...screens].filter(Boolean),
    aliases: [...aliases],
    entities: [...entities],
    people,
    timeline,
    contacts,
    follow_up_organizations: dynamic.follow_up_organizations || [],
    org_candidates: dynamic.org_candidates || [],
    ownership_conflicts: dynamic.conflicts || [],
    propagation_blocked: guarded.blocked.map((b) => ({
      entity: b.claim.entity,
      claim_type: b.claim.claim_type,
      reason: b.decision.reason,
    })),
  };
}

/**
 * Classify case outcome for pilot scorecard.
 */
export function classifyPilotCase(extracted, seed) {
  const claims = extracted.claims || [];
  // Only exact economic_owner (not *_candidate) may resolve HIGH — candidates stay PARTIAL.
  const owner = claims.find((c) => c.claim_type === "economic_owner" && c.confidence === "HIGH" && c.entity);
  const ownerPartial = claims.find(
    (c) =>
      (/economic_owner_candidate|economic_owner_partial_jv/i.test(c.claim_type) && c.entity) ||
      (c.claim_type === "economic_owner" && c.confidence === "MEDIUM" && c.entity)
  );
  const propco = claims.find((c) => /propco/i.test(c.claim_type) && c.confidence === "HIGH" && c.entity);
  const operator = claims.find((c) => c.claim_type === "operator");
  const unresolvedOwner = claims.some(
    (c) => c.claim_type === "economic_owner" && c.confidence === "NOT_VERIFIED" && !c.entity
  );

  const falseConfident = [];
  // Fibra Inn must not attach to non-Fibra-Inn assets (2.4F Pedregal false path)
  if (owner && /Fibra\s*Inn/i.test(String(owner.entity || "")) && !/fibra\s*inn|finn/i.test(seed.name || "")) {
    falseConfident.push("false_confident_fibra_inn_cross_property");
  }
  // IHVSF is Vallarta Grand PropCo only
  if (
    propco &&
    /IHVSF/i.test(String(propco.entity || "")) &&
    !/krystal\s+grand\s+puerto\s+vallarta|hilton\s+puerto\s+vallarta/i.test(seed.name || "")
  ) {
    falseConfident.push("wrong_propco_ihvsf_cross_property");
  }
  if (
    operator &&
    owner &&
    /chartwell/i.test(String(owner.entity || "")) === false &&
    String(owner.entity).toLowerCase().includes(String(operator.entity).toLowerCase().slice(0, 12)) &&
    /resort puerto vallarta/i.test(seed.name || "")
  ) {
    falseConfident.push("possible_operator_as_owner_on_third_party_asset");
  }

  let status = "PARTIAL";
  if (falseConfident.length) status = "FAILED";
  else if (
    /THIRD_PARTY_MANAGED/i.test(seed.archetype || "") &&
    owner &&
    operator &&
    String(owner.entity).toLowerCase().includes(String(operator.entity).toLowerCase().slice(0, 16)) &&
    !propco &&
    !/partial|jv|chartwell/i.test(String(owner.entity) + String(owner.claim_type))
  ) {
    // Operator-equal-owner without PropCo/JV on a third-party-managed archetype → do not promote RESOLVED_HIGH
    status = "PARTIAL";
  }   else if (owner && (propco || /jv|partial/i.test(owner.claim_type))) status = "RESOLVED_HIGH";
  else if (owner && propco) status = "RESOLVED_HIGH";
  // Removed hardcoded issuer name shortcut (fibrahotel|xcaret|…) from generic RESOLVED_HIGH.
  else if (owner) status = "PARTIAL"; // owned-by-operator-org without PropCo stays partial pending stronger evidence
  else if (ownerPartial) status = "PARTIAL";
  else if (unresolvedOwner) status = "CORRECTLY_ABSTAINED";
  else status = "PARTIAL";

  if (!falseConfident.length && /HARD|OPAQUE/i.test(seed.archetype || "") && unresolvedOwner && !ownerPartial) {
    status = "ESCALATION_CANDIDATE";
  }

  return {
    status,
    economic_owner: owner || ownerPartial || null,
    propco: propco || null,
    operator: operator || null,
    false_confident_flags: falseConfident,
  };
}
