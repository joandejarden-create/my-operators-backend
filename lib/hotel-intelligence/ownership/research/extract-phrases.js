/**
 * Multilingual ownership claim extraction (EN / ES / PT).
 * Requires explicit ownership / operator / developer semantics.
 * Hardened after ownership-eval-v1 (OTA "propiedad", TripAdvisor "propietario at …").
 */

import {
  hasNegativeOwnershipContext,
  looksLikeInstitutionalSponsorName,
  looksLikeMajorBrandName,
} from "./claim-context.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";

export const OWNERSHIP_EXTRACT_VERSION = "ownership-extract-phrases-v2";

/**
 * Entity capture: Title-Case / legal-looking spans.
 * Do NOT use the `i` flag on these patterns — capital-class must stay case-sensitive
 * or "and operated by" gets swallowed into the entity name.
 */
const ENTITY =
  "([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\\-]{1,40}(?:\\s+(?:(?:de|del|da|do|dos|das|of|the)\\s+)?[A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\\-]{1,40}){0,5})";

/**
 * Patterns — keywords case-insensitive via character classes; entity capture case-sensitive.
 * Bare "propietario" / "investor" removed (eval-v1 TripAdvisor / OTA FPs).
 */
export const OWNERSHIP_PHRASE_PATTERNS = Object.freeze([
  // OWNED_BY — EN
  {
    relationship_type: "OWNED_BY",
    re: new RegExp(
      `(?:(?:[Tt]he\\s+)?(?:[Hh]otel|[Pp]roperty|[Rr]esort|[Aa]sset)\\s+is\\s+[Oo]wned\\s+[Bb]y|[Oo]wned\\s+[Bb]y|[Oo]wnership\\s+of\\s+(?:the\\s+)?(?:hotel|property)\\s+(?:is\\s+)?(?:held\\s+by|vested\\s+in)|(?:is\\s+)?(?:a\\s+)?(?:property|asset)\\s+of|[Bb]elongs\\s+[Tt]o|[Pp]ortfolio\\s+of|[Pp]urchased\\s+[Bb]y|[Aa]cquired\\s+[Bb]y)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // OWNED_BY — ES
  {
    relationship_type: "OWNED_BY",
    re: new RegExp(
      `(?:(?:[Ee]l\\s+)?[Hh]otel\\s+es\\s+[Pp]ropiedad\\s+de|[Pp]ropiedad\\s+de|[Pp]ropietario\\s+del\\s+(?:hotel|inmueble|establecimiento)|[Dd]ueño\\s+del\\s+inmueble|[Pp]ertenece\\s+a|[Gg]rupo\\s+[Pp]ropietario\\s*[:\\-]?|[Ee]mpresa\\s+[Pp]ropietaria\\s*[:\\-]?|[Aa]dquirido\\s+por|[Aa]dquiri[oó]\\s+el\\s+hotel|[Cc]ompr[oó]\\s+el\\s+hotel)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // OWNED_BY — PT
  {
    relationship_type: "OWNED_BY",
    re: new RegExp(
      `(?:(?:[Oo]\\s+)?[Hh]otel\\s+[eé]\\s+[Pp]ropriedade\\s+de|[Pp]ropriedade\\s+de|[Pp]ropriet[aá]rio\\s+do\\s+hotel|[Ee]mpresa\\s+[Pp]ropriet[aá]ria\\s*[:\\-]?|[Pp]ertence\\s+a|[Aa]dquirido\\s+por|[Cc]omprou\\s+o\\s+hotel)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // CONTROLLED_BY
  {
    relationship_type: "CONTROLLED_BY",
    re: new RegExp(
      `(?:[Cc]ontrolled\\s+[Bb]y|[Cc]ontrolado\\s+por|[Bb]ajo\\s+control\\s+de|[Ss]ob\\s+controle\\s+de)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // SPONSORED_BY
  {
    relationship_type: "SPONSORED_BY",
    re: new RegExp(
      `(?:[Ii]nvestment\\s+(?:in\\s+)?(?:the\\s+)?(?:hotel|property)\\s+by|[Bb]acked\\s+[Bb]y|[Ss]ponsor(?:ed)?\\s+[Bb]y|[Ff]ondo\\s+(?:de\\s+inversi[oó]n|propriet[aá]rio)\\s+(?:de|por)?|[Ii]nvestidor\\s+(?:do\\s+hotel|institucional)\\s*[:\\-]?)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // DEVELOPED_BY
  {
    relationship_type: "DEVELOPED_BY",
    re: new RegExp(
      `(?:[Dd]eveloped\\s+[Bb]y|[Dd]esarrollad[oa]\\s+por|[Dd]esarrollada\\s+por|[Ii]ncorporado\\s+por|[Ii]ncorporadora\\s*[:\\-]?|[Dd]esenvolvido\\s+por|[Dd]esenvolvedor(?:a)?\\s*[:\\-]?)\\s*${ENTITY}`,
      "g"
    ),
  },
  // OPERATED_BY
  {
    relationship_type: "OPERATED_BY",
    re: new RegExp(
      `(?:[Oo]perated\\s+[Bb]y|[Mm]anaged\\s+[Bb]y|[Oo]perado\\s+por|[Aa]dministrado\\s+por|[Gg]est[aã]o\\s+(?:de|por)|[Aa]dministrado\\s+pela?)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
]);

/**
 * @param {string} text
 * @param {{ url?: string|null, hotelName?: string|null }} [meta]
 * @returns {{ name: string, relationship_type: string, quote: string, url: string|null, relationship_explicitness: number, snippet_only?: boolean }[]}
 */
export function extractOwnershipClaimsFromText(text, meta = {}) {
  const out = [];
  const body = String(text || "");
  if (!body.trim()) return out;

  for (const { relationship_type, re } of OWNERSHIP_PHRASE_PATTERNS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(body)) !== null) {
      if (
        relationship_type === "OWNED_BY" &&
        hasNegativeOwnershipContext(body, m.index, m[0].length)
      ) {
        continue;
      }

      let name = cleanEntityName(m[1]);
      if (!name || name.length < 3) continue;
      if (/^(the|a|an|el|la|los|las|o|os|as|de|do|da|por|para)\b/i.test(name)) {
        continue;
      }

      let relType = relationship_type;

      // Operator / developer language must never become OWNED_BY
      if (
        relType === "OWNED_BY" &&
        /\b(operated|managed|operado|administrado|gest[aã]o|desarrollad|desarrollada|developed|desenvolvido)\b/i.test(
          m[0]
        )
      ) {
        continue;
      }

      // Brands may be operators/developers; OWNED_BY requires stronger entity form
      const allowBrand =
        relType === "OPERATED_BY" ||
        relType === "DEVELOPED_BY" ||
        relType === "SPONSORED_BY";
      if (!isPlausibleLegalEntityName(name, { allowBrandAsEntity: allowBrand })) {
        continue;
      }

      // Brand near OWNED_BY without corporate legal form → skip
      if (relType === "OWNED_BY" && looksLikeMajorBrandName(name)) {
        if (!/\b(international|worldwide|hotels|hospitality|group|grupo|s\.?a|llc|inc)\b/i.test(name)) {
          continue;
        }
      }

      // Institutional PE → prefer SPONSORED_BY over PropCo OWNED_BY
      if (
        relType === "OWNED_BY" &&
        looksLikeInstitutionalSponsorName(name) &&
        !/\b(propco|prop\s*co|s\.?\s*a\.?\s*de\s*c\.?\s*v|fiduciaria|fideicomiso|llc|ltd)\b/i.test(
          name
        )
      ) {
        relType = "SPONSORED_BY";
      }

      const explicitness = scoreRelationshipExplicitness(m[0], relType);

      out.push({
        name,
        relationship_type: relType,
        quote: String(m[0] || "").slice(0, 240),
        url: meta.url || null,
        relationship_explicitness: explicitness,
      });
    }
  }
  return dedupeClaims(out);
}

/**
 * @param {string} matched
 * @param {string} relationshipType
 */
export function scoreRelationshipExplicitness(matched, relationshipType) {
  const s = String(matched || "").toLowerCase();
  let score = 0.45;
  if (relationshipType === "OWNED_BY") {
    if (
      /hotel\s+(is\s+)?owned\s+by|hotel\s+es\s+propiedad|hotel\s+[eé]\s+propriedade|propiedad\s+de|propriedade\s+de|pertenece\s+a|pertence\s+a|dueño\s+del\s+inmueble|propietario\s+del\s+hotel|acquired\s+by|adquirido\s+por|purchased\s+by|compr[oó]\s+el\s+hotel|comprou\s+o\s+hotel/.test(
        s
      )
    ) {
      score = 0.9;
    } else if (/owned\s+by|portfolio\s+of|belongs\s+to/.test(s)) {
      score = 0.75;
    } else {
      score = 0.55;
    }
  } else if (relationshipType === "OPERATED_BY") {
    score = /operated\s+by|managed\s+by|operado\s+por|administrado\s+por/.test(s)
      ? 0.9
      : 0.7;
  } else if (relationshipType === "DEVELOPED_BY") {
    score = /developed\s+by|desarrollad|desenvolvido\s+por|incorporadora/.test(s)
      ? 0.85
      : 0.65;
  } else if (relationshipType === "SPONSORED_BY" || relationshipType === "CONTROLLED_BY") {
    score = 0.7;
  }
  return score;
}

/**
 * @param {string} raw
 */
export function cleanEntityName(raw) {
  return String(raw || "")
    .replace(/\s+/g, " ")
    .replace(/[|].*$/, "")
    .replace(
      /\s+(and|y|e)\s+(operated|managed|operado|administrado|desarrollad|developed|owned|propiedad)\b.*$/i,
      ""
    )
    .replace(/\s+(is|was|with|for|que|responded|respondi[oó]|review)\s+.*$/i, "")
    .replace(/[,.;:]+$/, "")
    .trim()
    .slice(0, 120);
}

/**
 * @param {Array<{name:string,relationship_type:string}>} list
 */
export function dedupeClaims(list) {
  const seen = new Set();
  const out = [];
  for (const c of list || []) {
    const key = `${c.relationship_type}|${String(c.name || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(c);
  }
  return out;
}
