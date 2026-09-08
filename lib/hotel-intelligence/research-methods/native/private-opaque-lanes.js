/**
 * Packet 2.4E — private/opaque research lanes, query families, and
 * relationship-specific extraction (developer / transaction / financing / brand-signing).
 * Does not hard-code hotel answers. Obeys CROSS_PROPERTY_ENTITY_PROPAGATION_GUARD.
 */

import { applyCrossPropertyGuardToClaims } from "./cross-property-entity-guard.js";
import { identityAliasesForSeed } from "./seed-scoped-extractor.js";

function has(text, re) {
  return re.test(String(text || ""));
}

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Bounded query families — max ~10 per hotel. */
export function buildPrivateOpaqueQueryFamilies(seed = {}) {
  const name = String(seed.name || "").trim();
  const city = String(seed.city || "").trim();
  const brand = String(seed.affiliation_display || "").trim();
  const loc = city || String(seed.market || "");
  const base = `"${name}"`;
  const withCity = loc ? `${base} ${loc}` : base;
  const families = [
    { lane: "TRANSACTION", q: `${withCity} adquisición OR acquired OR sale OR compró OR vendió hotel` },
    { lane: "DEVELOPER", q: `${withCity} desarrollador OR developer OR inauguración OR opening` },
    { lane: "BRAND_SIGNING", q: brand ? `"${name}" ${brand} announce OR anuncia OR firma OR signing` : `${withCity} brand OR marca anuncia OR announcement owner OR developer` },
    { lane: "FINANCING", q: `${withCity} financiamiento OR crédito OR loan OR refinancing OR borrower` },
    { lane: "OWNER_ES", q: `${withCity} propietario OR dueño OR ownership OR grupo` },
    { lane: "FIDEICOMISO", q: `${withCity} fideicomiso OR JV OR asociación OR sociedad` },
    { lane: "PROJECT", q: `${withCity} SEMARNAT OR MIA OR proyecto OR environmental OR impact` },
    { lane: "ARCHITECTURE", q: `${withCity} architecture OR arquitecto OR construction OR construcción` },
    { lane: "CORPORATE", q: `${base} "grupo" OR "properties" OR "holdings" OR "capital" hotel México` },
    { lane: "PORTFOLIO", q: `${base} portfolio OR portafolio OR hoteles` },
  ];
  // Deduplicate by query string; keep order
  const seen = new Set();
  return families.filter((f) => {
    if (seen.has(f.q)) return false;
    seen.add(f.q);
    return true;
  });
}

export const PRIVATE_LANE_IDS = Object.freeze([
  "MX-TRANSACTION-OWNER-01",
  "MX-DEVELOPER-OWNER-01",
  "MX-BRAND-SIGNING-01",
  "MX-FINANCING-01",
  "MX-PROJECT-APPROVAL-01",
  "MX-MIXEDUSE-01",
  "MX-PRIVATE-DOMAIN-01",
  "MX-PRIVATE-PORTFOLIO-01",
  "MX-PRIVATE-CORP-DISCOVERY-01",
  "MX-PRIVATE-CORP-ENRICHMENT-01",
]);

/**
 * Score URL/title for private-opaque source class.
 */
export function classifySourceClass(url = "", title = "") {
  const u = `${url} ${title}`.toLowerCase();
  if (/\.pdf($|\?)/i.test(url)) return "pdf_document";
  if (/semarnat|mia\b|gob\.mx|impacto\s+ambiental/i.test(u)) return "project_approval";
  // Brand corporate press must beat the hotel|resort catch-all (2.4F Pedregal Hilton debut)
  if (/stories\.hilton\.com|newsroom\.hilton|hilton\.com\/.*\/news|hilton\.com\/.*\/press/i.test(u)) {
    return "brand_signing";
  }
  if (/prnewswire|businesswire|globenewswire|press|noticia|eleconomista|reforma|milenio|hospitalitynet|hotelnews|irei\.com|crenews|maritur\.com|hotel-online|travelagentcentral|latimes\.com\/travel/i.test(u)) {
    return "transaction_or_press";
  }
  if (/justia\.com|courthousenews|law\.|abogad|legal|tombstone|deal/i.test(u)) return "law_firm_deal";
  if (/bank|banco|credit|financi|lender|bbva|banorte|santander/i.test(u)) return "financing";
  if (/architect|arquitect|construction|construcción|designboom|archdaily|hksinc|swagroup/i.test(u)) {
    return "architecture_construction";
  }
  if (/fourseasons|rosewood|oneandonly|chable|matilda|marriott|hyatt|belmond|waldorf/i.test(u) && /announce|newsroom|press|developer|owner|debut|stories/i.test(u)) {
    return "brand_signing";
  }
  if (/investor|ir\.|reporte|bmv|fibra|nasdaq\.com/i.test(u)) return "issuer_filing";
  if (/grupo|properties|holdings|capital|developers|rlh|questro|discovery|walton/i.test(u)) {
    return "corporate_or_developer_site";
  }
  if (/booking\.com|tripadvisor|expedia|hoteles\.com|opentable|instagram\.com|facebook\.com/i.test(u)) {
    return "hotel_or_ota";
  }
  if (/hotel|resort/i.test(u) && /official|hilton\.com\/en\/hotels|waldorfastoria.*\.com\/?$/i.test(u)) {
    return "hotel_or_ota";
  }
  if (/hotel|resort/i.test(u) && !/debut|acqui|owner|capital|press|news/i.test(u)) return "hotel_or_ota";
  return "other";
}

function extractEntityNear(text, patterns) {
  for (const { re, relationship, temporalHint } of patterns) {
    const m = String(text || "").match(re);
    if (!m) continue;
    const entity = (m[1] || m[0] || "").replace(/\s+/g, " ").trim().slice(0, 120);
    if (entity.length < 4) continue;
    return { entity, relationship, temporalHint: temporalHint || "UNKNOWN" };
  }
  return null;
}

const ENTITY_PATTERNS = [
  {
    re: /(?:desarrollad[oa]|developer|desarrollado\s+por|developed\s+by)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,60})/i,
    relationship: "DEVELOPED_BY",
    temporalHint: "FORMER_OR_HISTORICAL_UNLESS_CORROBORATED",
  },
  {
    re: /(?:adquirid[oa]\s+por(?:\s+filiales\s+de)?|acquired\s+by(?:\s+affiliates\s+of)?|comprad[oa]\s+por|sold\s+to|vendid[oa]\s+a)\s*[:\-]?\s*((?:Walton\s+Street\s+Capital(?:\s+M[eé]xico)?|[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,50}?))(?=\s*(?:\.|,|;|y\s|and\s|Actualmente|the\s+luxury|and\s+now))/i,
    relationship: "OWNED_BY_TRANSACTION",
    temporalHint: "FORMER_UNTIL_CURRENT_CORROBORATION",
  },
  {
    re: /(?:adquirid[oa]\s+por|acquired\s+by|comprad[oa]\s+por|sold\s+to|vendid[oa]\s+a)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,60})/i,
    relationship: "OWNED_BY_TRANSACTION",
    temporalHint: "FORMER_UNTIL_CURRENT_CORROBORATION",
  },
  {
    re: /(?:propietario|owned\s+by|ownership\s+of|dueñ[oa])\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,60})/i,
    relationship: "OWNED_BY",
    temporalHint: "CURRENT_CANDIDATE",
  },
  {
    re: /(?:sponsor|patrocinad|promotor)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,60})/i,
    relationship: "SPONSORED_BY",
    temporalHint: "UNKNOWN",
  },
  {
    re: /(?:borrower|acreditad|prestatario)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{3,60})/i,
    relationship: "BORROWER_VEHICLE",
    temporalHint: "UNKNOWN",
  },
  {
    re: /\b(RLH\s+Properties|Grupo\s+Questro|Discovery\s+Land(?:\s+Company)?|Grupo\s+Chabl[eé]|Experiencias\s+Xcaret|Grupo\s+Xcaret|Belmond|LVMH|Blackstone|Related\s+Group|Grupo\s+Vidanta|Opera\s+Hoteles|Grupo\s+Posadas|Walton\s+Street\s+Capital(?:\s+Mexico)?|Nakheel(?:\s+Hotels)?|Ares\s+Management)\b/i,
    relationship: "ORG_MENTION",
    temporalHint: "UNKNOWN",
  },
];

function yearFromText(text) {
  const years = [...String(text || "").matchAll(/\b(19|20)\d{2}\b/g)].map((m) => Number(m[0]));
  if (!years.length) return null;
  return Math.max(...years);
}

function temporalFromEvidence(hint, year, nowYear = new Date().getFullYear()) {
  if (hint === "CURRENT_CANDIDATE") return year && year < nowYear - 5 ? "UNKNOWN_CURRENT_STATUS" : "CURRENT";
  if (hint === "FORMER_UNTIL_CURRENT_CORROBORATION") {
    if (year && year <= nowYear - 3) return "FORMER";
    if (year && year >= nowYear - 2) return "CURRENT";
    return "UNKNOWN_CURRENT_STATUS";
  }
  if (hint === "FORMER_OR_HISTORICAL_UNLESS_CORROBORATED") return "FORMER";
  return "UNKNOWN";
}

/**
 * Extract private/opaque relationship candidates from docs (seed-window preferred).
 */
export function extractPrivateOpaqueCandidates(docs, seed) {
  const aliases = identityAliasesForSeed(seed);
  const candidates = [];
  const rejected = [];
  const sourceClassesHit = new Set();
  const screens = new Set();

  for (const d of docs || []) {
    const sc = classifySourceClass(d.url, d.title);
    sourceClassesHit.add(sc);
    const text = String(d.text || "");
    // Prefer windows near hotel name
    let windows = [];
    for (const alias of aliases) {
      if (alias.length < 6) continue;
      const re = new RegExp(escapeRe(alias), "gi");
      let m;
      while ((m = re.exec(text))) {
        windows.push(text.slice(Math.max(0, m.index - 500), m.index + m[0].length + 500));
      }
    }
    if (!windows.length && aliases.some((a) => has(text, new RegExp(escapeRe(a.slice(0, Math.min(20, a.length))), "i")))) {
      windows = [text.slice(0, 8000)];
    }
    // Still scan title + first chunk for brand/corporate pages that name the hotel weakly
    if (!windows.length && has(`${d.title} ${text.slice(0, 2000)}`, new RegExp(escapeRe(String(seed.name || "").split(/\s+/).slice(0, 2).join("\\s+")), "i"))) {
      windows = [`${d.title}\n${text.slice(0, 4000)}`];
    }

    for (const win of windows.slice(0, 8)) {
      for (const pat of ENTITY_PATTERNS) {
        const hit = extractEntityNear(win, [pat]);
        if (!hit) continue;
        // Reject OTA / generic noise entities
        if (/tripadvisor|booking\.com|expedia|google|wikipedia|instagram|facebook/i.test(hit.entity)) {
          rejected.push({ entity: hit.entity, reason: "noise_entity", url: d.url });
          continue;
        }
        if (
          /fashion|designer|inspired|acclaimed|click\s+here|read\s+more|subscribe|cookie|privacy/i.test(hit.entity) ||
          hit.entity.split(/\s+/).length > 8 ||
          hit.entity.length > 80
        ) {
          rejected.push({ entity: hit.entity, reason: "non_company_phrase", url: d.url });
          continue;
        }
        if (/hotel|resort|the\s+$/i.test(hit.entity) && hit.entity.split(/\s+/).length <= 2) {
          rejected.push({ entity: hit.entity, reason: "too_generic", url: d.url });
          continue;
        }
        // Prefer org-like entities for HIGH promotion later
        const looksOrg =
          /\b(grupo|properties|property|holdings|capital|sa\b|s\.a|s\s+de|llc|inc|bv|nv|fideicomiso|fibra|rlh|questro|discovery|belmond|xcaret|chabl)/i.test(
            hit.entity
          ) || /^[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑ&.,\- ]{2,40}$/.test(hit.entity);
        if (!looksOrg && hit.relationship !== "ORG_MENTION") {
          rejected.push({ entity: hit.entity, reason: "not_org_like", url: d.url });
          continue;
        }
        const year = yearFromText(win) || yearFromText(d.title);
        const temporal = temporalFromEvidence(hit.temporalHint, year);
        candidates.push({
          entity: hit.entity,
          relationship: hit.relationship,
          temporal_status: temporal,
          evidence_year: year,
          source_class: sc,
          source_url: d.url,
          lane_hint:
            hit.relationship === "DEVELOPED_BY"
              ? "MX-DEVELOPER-OWNER-01"
              : hit.relationship === "OWNED_BY_TRANSACTION"
                ? "MX-TRANSACTION-OWNER-01"
                : hit.relationship === "BORROWER_VEHICLE"
                  ? "MX-FINANCING-01"
                  : hit.relationship === "OWNED_BY"
                    ? "MX-PRIVATE-CORP-DISCOVERY-01"
                    : sc === "brand_signing"
                      ? "MX-BRAND-SIGNING-01"
                      : sc === "project_approval"
                        ? "MX-PROJECT-APPROVAL-01"
                        : "MX-PRIVATE-CORP-DISCOVERY-01",
          excerpt: win.slice(0, 280),
        });
      }
      // Domain legal footer
      const legal = win.match(/(?:©|Copyright|razon\s+social|razón\s+social|operado\s+por|operated\s+by)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.,\- ]{4,80})/i);
      if (legal) {
        candidates.push({
          entity: legal[1].trim().slice(0, 120),
          relationship: "OPERATED_BY_OR_LEGAL_FOOTER",
          temporal_status: "CURRENT",
          evidence_year: null,
          source_class: sc,
          source_url: d.url,
          lane_hint: "MX-PRIVATE-DOMAIN-01",
          excerpt: legal[0].slice(0, 200),
        });
      }
      if (has(win, /fideicomiso/i)) {
        const fid = win.match(/Fideicomiso[^\n.]{0,90}/i);
        candidates.push({
          entity: fid ? fid[0].slice(0, 120) : "Fideicomiso (unnamed)",
          relationship: "JV_OR_TRUST_VEHICLE",
          temporal_status: "UNKNOWN",
          evidence_year: yearFromText(win),
          source_class: sc,
          source_url: d.url,
          lane_hint: "MX-PROJECT-APPROVAL-01",
          excerpt: (fid || "fideicomiso").toString().slice(0, 200),
        });
      }
      if (has(win, /residenc|condomin|mixed[- ]use|uso\s+mixto|branded\s+resid/i)) {
        screens.add("MIXED_USE_MULTIPLE_PARTIES");
      }
    }
  }

  // Deduplicate by entity+relationship
  const uniq = [];
  const key = new Set();
  for (const c of candidates) {
    const k = `${c.relationship}|${c.entity.toLowerCase()}`;
    if (key.has(k)) continue;
    key.add(k);
    uniq.push(c);
  }

  return {
    candidates: uniq,
    rejected,
    source_classes_hit: [...sourceClassesHit],
    screens: [...screens],
  };
}

/**
 * Triangulate private ownership confidence.
 * Single old opening article → not HIGH for OWNED_BY.
 */
export function triangulatePrivateOwnership(candidates, seed) {
  const screens = new Set(["OPERATOR_NOT_OWNER"]);
  const byEntity = new Map();
  for (const c of candidates || []) {
    const k = String(c.entity || "").toLowerCase().replace(/\s+/g, " ").trim();
    if (!k) continue;
    if (!byEntity.has(k)) byEntity.set(k, []);
    byEntity.get(k).push(c);
  }

  let best = null;
  for (const [, list] of byEntity) {
    const classes = new Set(list.map((c) => c.source_class));
    const relationships = new Set(list.map((c) => c.relationship));
    const hasCurrentOwned = list.some((c) => c.relationship === "OWNED_BY" && c.temporal_status === "CURRENT");
    const hasTxn = list.some((c) => c.relationship === "OWNED_BY_TRANSACTION");
    const hasDev = list.some((c) => c.relationship === "DEVELOPED_BY");
    const hasOrg = list.some((c) => c.relationship === "ORG_MENTION");
    const hasPortfolio = list.some((c) => /portfolio|corporate/i.test(c.source_class));
    const years = list.map((c) => c.evidence_year).filter(Boolean);
    const newest = years.length ? Math.max(...years) : null;

    let score = 0;
    let temporal = "UNKNOWN";
    let confidence = "NOT_VERIFIED";
    let claimType = "economic_owner_candidate";
    let note = "";

    if (hasCurrentOwned && classes.size >= 2) {
      score = 90;
      confidence = "HIGH";
      temporal = "CURRENT";
      claimType = "economic_owner";
      note = "Triangulated OWNED_BY across ≥2 source classes";
    } else if (hasTxn && hasOrg && newest && newest >= new Date().getFullYear() - 2) {
      score = 75;
      confidence = "HIGH";
      temporal = "CURRENT";
      claimType = "economic_owner";
      note = "Recent transaction + org corroboration";
    } else if (hasTxn && newest && newest < new Date().getFullYear() - 3) {
      score = 40;
      confidence = "MEDIUM";
      temporal = "FORMER";
      claimType = "historical_owner";
      note = "Older transaction — not promoted to current without corroboration";
      screens.add("HISTORICAL_NOT_CURRENT");
    } else if (hasDev && (hasOrg || hasPortfolio) && classes.size >= 2) {
      score = 55;
      confidence = "MEDIUM";
      temporal = "UNKNOWN_CURRENT_STATUS";
      claimType = "developer_sponsor_candidate";
      note = "Developer/sponsor triangulated — not auto OWNED_BY";
      screens.add("DEVELOPER_NOT_OWNER");
    } else if (hasOrg && classes.size >= 2) {
      score = 50;
      confidence = "MEDIUM";
      temporal = "UNKNOWN_CURRENT_STATUS";
      claimType = "economic_owner_candidate";
      note = "Org mention across sources — needs current validation";
    } else if (hasDev) {
      score = 30;
      confidence = "MEDIUM";
      temporal = "FORMER";
      claimType = "developer";
      note = "Single-lane developer evidence";
      screens.add("DEVELOPER_NOT_OWNER");
    } else if (hasTxn) {
      score = 25;
      confidence = "MEDIUM";
      temporal = newest && newest >= new Date().getFullYear() - 2 ? "CURRENT" : "UNKNOWN_CURRENT_STATUS";
      claimType = "transaction_party";
      note = "Single-lane transaction — insufficient alone for HIGH current owner";
      screens.add("SOURCE_NOT_DECISIVE");
    }

    if (score > 0) {
      const entity = list[0].entity;
      const urls = [...new Set(list.map((c) => c.source_url).filter(Boolean))].slice(0, 4);
      const lanes = [...new Set(list.map((c) => c.lane_hint))];
      const candidate = {
        entity,
        claim_type: claimType,
        confidence,
        temporal_status: temporal,
        score,
        source_urls: urls,
        source_classes: [...classes],
        relationships: [...relationships],
        lanes,
        note,
        evidence_year: newest,
      };
      if (!best || candidate.score > best.score) best = candidate;
    }
  }

  // Never promote developer-only to HIGH economic_owner
  if (best?.claim_type === "developer" || best?.claim_type === "developer_sponsor_candidate") {
    screens.add("DEVELOPER_NOT_OWNER");
  }

  const claims = [];
  if (best) {
    claims.push({
      claim_type: best.claim_type === "economic_owner" ? "economic_owner" : best.claim_type,
      entity: best.entity,
      temporal_status: best.temporal_status,
      confidence: best.confidence,
      source_urls: best.source_urls,
      note: best.note,
      private_opaque_meta: best,
    });
  } else {
    claims.push({
      claim_type: "economic_owner",
      entity: null,
      temporal_status: "UNKNOWN",
      confidence: "NOT_VERIFIED",
      source_urls: [],
      note: "No triangulated private owner from retrieved evidence",
    });
  }

  // Collect secondary developer / historical
  for (const [, list] of byEntity) {
    const entity = list[0].entity;
    if (best && entity.toLowerCase() === best.entity.toLowerCase()) continue;
    if (list.some((c) => c.relationship === "DEVELOPED_BY")) {
      claims.push({
        claim_type: "developer",
        entity,
        temporal_status: "FORMER",
        confidence: "MEDIUM",
        source_urls: list.map((c) => c.source_url).slice(0, 2),
        note: "Developer candidate — not current owner unless corroborated",
      });
      screens.add("DEVELOPER_NOT_OWNER");
    }
  }

  const guarded = applyCrossPropertyGuardToClaims(
    claims.filter((c) => c.entity),
    seed,
    (candidates || []).map((c) => c.excerpt).join("\n")
  );

  return {
    best,
    claims: [
      ...guarded.claims,
      ...claims.filter((c) => !c.entity),
    ],
    propagation_blocked: guarded.blocked,
    screens: [...screens],
  };
}

export function classifyPrivateOpaqueCase(triangulated, baseClassification = {}) {
  const best = triangulated.best;
  const false_confident_flags = [];
  if (best?.claim_type === "economic_owner" && best.confidence === "HIGH" && best.source_classes?.length < 2) {
    // Should not happen given triangulation rules; belt-and-suspenders
    false_confident_flags.push("SINGLE_SOURCE_HIGH_BLOCKED");
  }

  if (best?.claim_type === "economic_owner" && best.confidence === "HIGH") {
    return {
      status: "RESOLVED_HIGH",
      economic_owner: { entity: best.entity, confidence: "HIGH", temporal_status: best.temporal_status },
      developer: triangulated.claims.find((c) => c.claim_type === "developer") || null,
      propco: baseClassification.propco || null,
      operator: baseClassification.operator || null,
      false_confident_flags,
      method_lanes: best.lanes || [],
    };
  }

  if (best && (best.confidence === "MEDIUM" || best.claim_type === "developer_sponsor_candidate" || best.claim_type === "historical_owner")) {
    return {
      status: "PARTIAL",
      economic_owner:
        best.claim_type === "historical_owner"
          ? null
          : { entity: best.entity, confidence: best.confidence, temporal_status: best.temporal_status },
      historical_owner: best.claim_type === "historical_owner" ? best : null,
      developer: triangulated.claims.find((c) => c.claim_type === "developer") || null,
      propco: baseClassification.propco || null,
      operator: baseClassification.operator || null,
      false_confident_flags,
      method_lanes: best.lanes || [],
    };
  }

  return {
    status: "ESCALATION_CANDIDATE",
    economic_owner: null,
    developer: null,
    propco: baseClassification.propco || null,
    operator: baseClassification.operator || null,
    false_confident_flags,
    method_lanes: [],
  };
}
