/**
 * Economic ownership claim extraction (P1.5).
 * Separate from exact PropCo OWNED_BY — acquisition, sponsor, portfolio, investment.
 */

import {
  looksLikeInstitutionalSponsorName,
  looksLikeMajorBrandName,
} from "./claim-context.js";
import { isPlausibleLegalEntityName } from "./entity-name-guard.js";
import { cleanEntityName, dedupeClaims } from "./extract-phrases.js";

export const ECONOMIC_EXTRACT_VERSION = "ownership-economic-extract-v1";

const ENTITY =
  "([A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\\-]{1,40}(?:\\s+(?:(?:de|del|da|do|dos|das|of|the)\\s+)?[A-ZÁÉÍÓÚÑÃÕÂÊÔÀ][\\wÁÉÍÓÚÑáéíóúñãõâêôàü&.'’\\-]{1,40}){0,5})";

/** Economic relationship patterns — not generic OWNED_BY. */
export const ECONOMIC_PHRASE_PATTERNS = Object.freeze([
  // SPONSORED_BY / investment sponsor
  {
    relationship_type: "SPONSORED_BY",
    claim_kind: "investment_sponsor",
    re: new RegExp(
      `(?:[Ii]nvestment\\s+(?:in|by)|[Bb]acked\\s+by|[Ss]ponsored\\s+by|[Pp]ortfolio\\s+(?:of|includes)|[Aa]cquired\\s+by|[Pp]urchased\\s+by|[Ii]nversi[oó]n\\s+de|[Ff]inanciado\\s+por|[Pp]atrocinado\\s+por|[Ii]nvestimento\\s+de|[Aa]dquirido\\s+por|[Cc]omprado\\s+por)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // Acquisition → often SPONSORED_BY for PE/institutional (not PropCo)
  {
    relationship_type: "SPONSORED_BY",
    claim_kind: "acquisition",
    re: new RegExp(
      `(?:[Aa]cquisition\\s+of\\s+(?:the\\s+)?(?:hotel|property|portfolio)|[Pp]ortfolio\\s+acquired\\s+by|[Aa]dquiri[oó]\\s+(?:el\\s+)?(?:hotel|portafolio)|[Aa]quisi[cç][aã]o\\s+(?:do|de|por))\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // CONTROLLED_BY — corporate control / parent (economic)
  {
    relationship_type: "CONTROLLED_BY",
    claim_kind: "corporate_control",
    re: new RegExp(
      `(?:[Cc]ontrolled\\s+by|[Pp]arent\\s+company|[Ss]ubsidiary\\s+of|[Cc]ontrolado\\s+por|[Ss]ubsidiaria\\s+de|[Gg]rupo\\s+[Pp]ropietario|[Ss]ob\\s+controle\\s+de)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
  // Portfolio listing — hotel mentioned on owner portfolio page
  {
    relationship_type: "SPONSORED_BY",
    claim_kind: "portfolio_listing",
    re: new RegExp(
      `(?:[Pp]ortfolio\\s+(?:includes|features|properties)|[Pp]roperties\\s+include|[Hh]ospitality\\s+investments|[Pp]ortafolio\\s+incluye|[Ii]nvestimentos\\s+hoteleros)\\s*[:\\-]?\\s*${ENTITY}`,
      "g"
    ),
  },
]);

/**
 * @param {string} text
 * @param {{ url?: string|null, hotelName?: string|null, registeredCompany?: string|null }} [meta]
 */
export function extractEconomicOwnershipClaims(text, meta = {}) {
  const out = [];
  const body = String(text || "");
  if (!body.trim()) return out;
  const hotelName = String(meta.hotelName || "").trim();

  for (const { relationship_type, claim_kind, re } of ECONOMIC_PHRASE_PATTERNS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(body)) !== null) {
      let name = cleanEntityName(m[1]);
      if (!name || name.length < 3) continue;
      if (!isPlausibleLegalEntityName(name, { allowBrandAsEntity: false })) continue;

      // Institutional acquisition language → SPONSORED_BY not OWNED_BY
      if (
        relationship_type === "SPONSORED_BY" &&
        looksLikeInstitutionalSponsorName(name)
      ) {
        // keep as SPONSORED_BY
      } else if (looksLikeMajorBrandName(name) && !/\b(capital|partners|group|grupo|invest|fund|holdings)\b/i.test(name)) {
        continue;
      }

      // Portfolio listing must mention hotel name nearby for property identity
      if (claim_kind === "portfolio_listing" && hotelName) {
        const window = body.slice(
          Math.max(0, m.index - 200),
          m.index + m[0].length + 200
        );
        const hotelNorm = hotelName.toLowerCase().replace(/[^a-z0-9]+/g, " ");
        const winNorm = window.toLowerCase();
        const tokens = hotelNorm.split(" ").filter((t) => t.length > 3);
        const matchCount = tokens.filter((t) => winNorm.includes(t)).length;
        if (tokens.length >= 2 && matchCount < Math.min(2, tokens.length)) {
          continue;
        }
      }

      out.push({
        name,
        relationship_type,
        claim_kind,
        quote: String(m[0] || "").slice(0, 240),
        url: meta.url || null,
        relationship_explicitness: claim_kind === "acquisition" ? 0.85 : 0.75,
        economic_not_propco: true,
      });
    }
  }
  return dedupeClaims(out);
}

/**
 * Detect portfolio page listing hotel by name (owner site).
 * @param {string} pageText
 * @param {string} hotelName
 * @param {string} [ownerSiteHost]
 */
export function detectPortfolioHotelListing(pageText, hotelName, ownerSiteHost = "") {
  const body = String(pageText || "");
  const name = String(hotelName || "").trim();
  if (!body || !name) return null;

  const tokens = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .split(" ")
    .filter((t) => t.length > 3);
  if (tokens.length < 2) return null;

  const lower = body.toLowerCase();
  const hits = tokens.filter((t) => lower.includes(t)).length;
  if (hits < Math.min(2, tokens.length)) return null;

  const portfolioCue =
    /\b(portfolio|properties|investments|hospitality|hotels|portafolio|inversiones|investimentos)\b/i.test(
      body.slice(0, 3000)
    );
  if (!portfolioCue) return null;

  return {
    relationship_type: "SPONSORED_BY",
    claim_kind: "portfolio_hotel_listing",
    quote: `Portfolio page lists hotel matching "${name}"`,
    url: ownerSiteHost || null,
    relationship_explicitness: 0.8,
    economic_not_propco: true,
    name: null, // caller must supply owner entity from page context / domain
  };
}
