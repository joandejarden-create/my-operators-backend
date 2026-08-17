/**
 * D.4E — Six visible Profile fields: research + KEEP/REMOVE decisions.
 * yearsInBusiness ALWAYS derives: 2026 - yearEstablished.
 */
import { resolveOperatorYears, OPERATOR_SETUP_YEARS_AS_OF } from "../partner-intelligence/operator-setup-years-registry.js";

export const D4E_AS_OF_YEAR = OPERATOR_SETUP_YEARS_AS_OF;

export const SIX_FIELD_DECISIONS = Object.freeze({
  yearEstablished: {
    action: "POPULATE",
    semantic:
      "Year the current operator/company traces its operating origin to (not brand launch alone, not CALA division start).",
  },
  yearsInBusiness: {
    action: "DERIVE",
    semantic: `yearsInBusiness = ${D4E_AS_OF_YEAR} - yearEstablished (deterministic; never free-typed).`,
  },
  brands: {
    action: "DERIVE",
    semantic:
      "Linked Brand Basics records for brands with documented current/operating experience via Brand Relationships ∪ Current Assignments (not brand ownership alone).",
  },
  primaryServiceModel: {
    action: "KEEP — POPULATE",
    why: "Explorer/alignment still consume it; distinct from Operating Model (legal/commercial structure) — portfolio service orientation.",
    optionsCanonical: ["Full-Service Focus", "Select-Service Focus", "Mixed", "All-Inclusive Focus"],
  },
  managementPhilosophy: {
    action: "KEEP — POPULATE",
    why: "Explorer Company Story field; distinct from differentiators — how the company describes its approach to operating hotels.",
  },
  missionStatement: {
    action: "KEEP — POPULATE",
    why: "Explorer Company Story field; official mission/purpose/vision hierarchy — not invented.",
  },
});

/** Explicit researched mission / philosophy for operators currently blank on those fields. */
export const D4E_STORY_PACK = {
  rec9JSyGQjvodsPSJ: {
    // AADESA
    missionStatement:
      "Enable hotel owners and investors to delegate comprehensive hotel management and franchising to specialists and improve profitability through lower operating costs and sales distribution (aadesa.com.ar).",
    managementPhilosophy:
      "Integral hotel management and franchising for independent and regional hotels across Latin America — owners retain investment freedom while AADESA operates the hotel end-to-end.",
    primaryServiceModel: "Mixed",
    sources: ["https://www.aadesa.com.ar/"],
  },
  recjgHXqTJktijFUR: {
    // Álvarez Argüelles
    missionStatement:
      "Create memorable hotel experiences across Álvarez Argüelles brands from luxury (Costa Galana, Los Cauquenes) through premium and essentials tiers in Argentina (company materials).",
    managementPhilosophy:
      "Family-owned Argentine hotel group operating proprietary multi-tier brands with hands-on ownership of standards from Mar del Plata luxury origins to national city and resort hotels.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.alvarezarguelles.com/", "https://www.alvarezarguelles.com/historia/"],
  },
  recVtNxNeeYlngtUk: {
    // Auberge
    missionStatement:
      "Deliver ultra-luxury destination resorts rooted in place, culinary excellence, and intimate scale — the Auberge Collection experience (aubergeresorts.com).",
    managementPhilosophy:
      "Brand-managed luxury destination-resort collection with experiential, place-specific operating standards rather than a broad multi-brand third-party management platform.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.aubergeresorts.com"],
  },
  rec04aLAfmupWG4ZK: {
    // Barceló
    missionStatement:
      "Operate an integrated hotel company that owns and manages hotels under Barceló, Occidental, Allegro and Royal Hideaway brands for leisure and urban travelers (barcelo.com).",
    managementPhilosophy:
      "Integrated owner–brand–operator model: corporate brand operating control across Barceló family brands rather than a pure third-party management-company stance.",
    primaryServiceModel: "Mixed",
    sources: ["https://www.barcelo.com"],
  },
  rechnXKjpeiNMaqjJ: {
    // Four Seasons
    missionStatement:
      "Define luxury hospitality through uncompromising service and long-term management agreements that protect Four Seasons brand standards on owner-funded assets (fourseasons.com).",
    managementPhilosophy:
      "Management-contract-only luxury platform (no franchising): Four Seasons retains operational control over staffing, service culture, and brand standards.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.fourseasons.com"],
  },
  reculkMOYWDxX14Pv: {
    // Hyatt Managed
    missionStatement:
      "Care for people so they can be their best — Hyatt’s stated purpose applied through managed and franchised hotels for owners (hyatt.com / Hyatt purpose).",
    managementPhilosophy:
      "Asset-light brand management and franchise platform: Hyatt brand operating systems and World of Hyatt loyalty structure managed hotels for third-party owners.",
    primaryServiceModel: "Mixed",
    sources: ["https://www.hyatt.com"],
  },
  rec5xdV2THfFjEUPk: {
    // Mandarin Oriental
    missionStatement:
      "Delight and satisfy guests with oriental hospitality through Mandarin Oriental brand-managed luxury hotels (mandarinoriental.com).",
    managementPhilosophy:
      "Ultra-luxury brand-managed hotels operated exclusively under management agreements with Mandarin Oriental corporate brand standards.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.mandarinoriental.com"],
  },
  rec28eZ7ERwc92XWd: {
    // Meliá
    missionStatement:
      "Create memorable experiences through Meliá’s multi-brand hotel portfolio for leisure and business travelers worldwide (melia.com).",
    managementPhilosophy:
      "Spanish listed multi-brand hotel company combining owned brands (Meliá, Gran Meliá, ME, Innside) with management and franchise contracts under corporate brand governance.",
    primaryServiceModel: "Mixed",
    sources: ["https://www.melia.com"],
  },
  recji1awMffccwox2: {
    // Rosewood
    missionStatement:
      "Create a sense of place — Rosewood’s stated hospitality philosophy for ultra-luxury hotels (rosewoodhotels.com).",
    managementPhilosophy:
      "Ultra-luxury Rosewood brand-managed hotels under “A Sense of Place” via management agreements, not franchise distribution.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.rosewoodhotels.com"],
  },
  rec8XpNv6G0WOlMwu: {
    // Shangri-La
    missionStatement:
      "Shangri-La hospitality — Asian hospitality rooted in care and respect, delivered through Shangri-La brand hotels (shangri-la.com).",
    managementPhilosophy:
      "Integrated luxury brand-operator platform centered on Shangri-La brand standards across owned and managed hotels.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.shangri-la.com"],
  },
  recIq0XYgt5Ghvcsz: {
    // Sonesta
    missionStatement:
      "Wow every guest, team member, partner and community by delivering quality, value, and amazing hospitality (sonesta.com About — official mission).",
    managementPhilosophy:
      "Multi-brand Sonesta brand-operator/franchisor platform (Royal Sonesta, Sonesta Select, ES/Simply Suites) with corporate brand standards and franchise support.",
    primaryServiceModel: "Mixed",
    sources: ["https://www.sonesta.com/about-sonesta"],
  },
  recHj56wpRLUnJ5Wx: {
    // Tremun
    missionStatement:
      "Develop and manage hotel businesses — own and third-party — with experience and flexibility adapted to each property’s need (tremunhoteles.com.ar/sobre-tremun).",
    managementPhilosophy:
      "Mar del Plata–rooted operator combining owned and third-party hotel management with gastronomy-forward service discipline across Argentina destinations.",
    primaryServiceModel: "Full-Service Focus",
    sources: ["https://www.tremunhoteles.com.ar/sobre-tremun"],
  },
};

/** Normalize legacy primaryServiceModel casing. */
export function canonicalizePrimaryServiceModel(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  const map = {
    "full-service focus": "Full-Service Focus",
    "full-service focus.": "Full-Service Focus",
    "select-service focus": "Select-Service Focus",
    mixed: "Mixed",
    "all-inclusive focus": "All-Inclusive Focus",
  };
  const hit = map[s.toLowerCase()];
  if (hit) return hit;
  if (["Full-Service Focus", "Select-Service Focus", "Mixed", "All-Inclusive Focus"].includes(s)) return s;
  return null;
}

export function derivePrimaryServiceModel({ existing, propertyTypes = [], serviceModels = [], om = "" }) {
  const canon = canonicalizePrimaryServiceModel(existing);
  if (canon) return canon;
  const pt = new Set((propertyTypes || []).map(String));
  const sm = new Set((serviceModels || []).map(String));
  const ai = [...pt, ...sm].some((x) => /all-?inclusive/i.test(x));
  const full = [...pt, ...sm].some((x) => /full.?service/i.test(x));
  const select = [...pt, ...sm].some((x) => /select.?service|limited.?service/i.test(x));
  const resort = [...pt, ...sm].some((x) => /resort/i.test(x));
  if (ai && !select && !full) return "All-Inclusive Focus";
  if (ai && (full || resort)) return "Mixed";
  if (full && select) return "Mixed";
  if (select && !full) return "Select-Service Focus";
  if (full || resort) return "Full-Service Focus";
  if (/Integrated Owner|Brand \/ Operator|Integrated Brand/i.test(String(om))) return "Full-Service Focus";
  if (/Third-Party|Hybrid/i.test(String(om))) return "Mixed";
  return "Mixed";
}

export function resolveYearsForOperator(companyName, currentYe) {
  const reg = resolveOperatorYears({ companyName });
  if (reg) return reg;
  if (currentYe != null && Number.isFinite(Number(currentYe))) {
    const ye = Number(currentYe);
    return {
      yearEstablished: ye,
      yearsInBusiness: D4E_AS_OF_YEAR - ye,
      sourceNote: "Existing Profile yearEstablished retained; yearsInBusiness re-derived",
    };
  }
  return null;
}

/** Brand name aliases → Brand Basics lookup keys */
export const BRAND_NAME_ALIASES = Object.freeze({
  iberostar: ["Iberostar", "Iberostar Selection", "Iberostar Waves"],
  "iberostar selection": ["Iberostar Selection", "Iberostar"],
  "iberostar waves": ["Iberostar Waves", "Iberostar"],
  melia: ["Meliá", "Melia"],
  "meliá": ["Meliá", "Melia"],
  "gran meliá": ["Gran Meliá", "Gran Melia"],
  "me by meliá": ["ME by Meliá", "ME"],
  barceló: ["Barceló", "Barcelo"],
  "holiday inn express": ["Holiday Inn Express"],
  "holiday inn": ["Holiday Inn"],
  "best western": ["Best Western"],
  "curio collection": ["Curio Collection by Hilton", "Curio Collection", "Curio"],
  "tribute portfolio": ["Tribute Portfolio"],
  "ac hotels": ["AC Hotels by Marriott", "AC Hotels"],
  "park hyatt": ["Park Hyatt"],
  "grand hyatt": ["Grand Hyatt"],
  "hyatt ziva": ["Hyatt Ziva"],
  "sonesta es suites": ["Sonesta ES Suites"],
  "royal sonesta": ["Royal Sonesta"],
  auberge: ["Auberge", "Auberge Resorts", "Auberge Resorts Collection", "Auberge Collection"],
  "four seasons": ["Four Seasons"],
  "mandarin oriental": ["Mandarin Oriental"],
  rosewood: ["Rosewood"],
  "shangri-la": ["Shangri-La Hotels and Resorts", "Shangri-La"],
  wyndham: ["Wyndham"],
  cyan: ["Cyan"],
  don: ["DON"],
  "grand brizo": ["Grand Brizo"],
  "costa galana": ["Costa Galana"],
  "los cauquenes": ["Los Cauquenes"],
  independent: ["Independent"],
  sirenis: ["Sirenis"],
});

/** Brand Basics records that must exist to complete Profile brands links */
export const BRAND_BASICS_ENSURE = [
  { name: "Auberge", parent: "Auberge Resorts Collection", scale: "Luxury", model: "Lifestyle Brand", service: "Full-Service" },
  { name: "Barceló", parent: "Barceló Hotel Group", scale: "Upper Upscale", model: "Hard Brand", service: "Full-Service" },
  { name: "Meliá", parent: "Meliá Hotels International", scale: "Upper Upscale", model: "Hard Brand", service: "Full-Service" },
  { name: "Gran Meliá", parent: "Meliá Hotels International", scale: "Luxury", model: "Hard Brand", service: "Full-Service" },
  { name: "ME by Meliá", parent: "Meliá Hotels International", scale: "Upper Upscale", model: "Lifestyle Brand", service: "Lifestyle / Boutique" },
];

/** Override existing generic values that fail Writer v2 banned detector */
export const D4E_GENERIC_OVERRIDES = {
  recF2WqLqNVyKGz9E: {
    // Accor — strip banned "Owner-centric"
    managementPhilosophy:
      "Accor management and franchise partnership models: under management agreements Accor provides operational oversight (budgets, accounting, performance, personnel) to maximize owner returns; asset-light positioning privileges hotel management over ownership (group.accor.com Solutions / Overview 2026).",
  },
  rec3TUHT9Z4AnFp5P: {
    // Playa — strip banned "best-in-class"
    missionStatement:
      "Deliver an all-inclusive guest experience with strong direct-guest relationships to improve acquisition cost and repeat business across Playa’s Mexico, Dominican Republic, and Jamaica resorts (Playa public positioning).",
  },
};