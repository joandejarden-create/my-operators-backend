/**
 * Resolve company / owner labels to Owner Target records; score contacts for outreach.
 */
import { normalizeOwnerKey } from "./normalize.js";
import { lookupOwnerFootprint } from "./cala-footprint.js";
import { isNonPersonMailboxEmail } from "./registry-contact-verification.js";

const LEGAL_SUFFIX_RE =
  /\b(sa de cv|s a de c v|sas|s a s|sa|srl|ltda|llc|inc|corp|gmbh|limited|holdings?|group|grupo|plc|ag|bv|nv|lp|llp)\b/g;

/** CoStar contact exports skew toward US brokerage — deprioritize for owner outreach. */
export const BROKER_COMPANY_PATTERN =
  /\b(compass|cushman|wakefield|coldwell banker|coldwell|newmark|cbre|jll|colliers|marcus millichap|eastdil|berkadia|savills|knight frank|avison young|transwestern|lee & associates|lee associates|hff|northmarq|kidder|eastdil secured|real estate services|brokerage)\b/i;

function normalizeForMatch(name) {
  return normalizeOwnerKey(name).replace(LEGAL_SUFFIX_RE, " ").replace(/\s+/g, " ").trim();
}

/**
 * @param {{ id: string, ownerName: string }[]} ownerRecords
 */
export function buildOwnerTargetIndexes(ownerRecords) {
  /** @type {Map<string, { id: string, ownerName: string }>} */
  const byExactName = new Map();
  /** @type {Map<string, { id: string, ownerName: string }>} */
  const byKey = new Map();

  for (const rec of ownerRecords) {
    const ownerName = String(rec.ownerName || "").trim();
    if (!ownerName) continue;
    const row = { id: rec.id, ownerName };
    byExactName.set(ownerName, row);
    byKey.set(normalizeOwnerKey(ownerName), row);
  }
  return { byExactName, byKey };
}

/**
 * @param {string} label
 * @param {Map<string, string[]>} nameAliasIndex
 * @param {ReturnType<buildOwnerTargetIndexes>} ownerIndexes
 */
export function resolveLabelToOwnerTargets(label, nameAliasIndex, ownerIndexes) {
  const raw = String(label || "").trim();
  if (!raw) return [];

  /** @type {Map<string, { id: string, ownerName: string, matchType: string }>} */
  const hits = new Map();

  const addHit = (row, matchType) => {
    if (!row?.id) return;
    if (!hits.has(row.id)) hits.set(row.id, { ...row, matchType });
  };

  for (const candidate of splitCompoundOwnerName(raw)) {
    const key = normalizeOwnerKey(candidate);
    const loose = normalizeForMatch(candidate);
    const direct = ownerIndexes.byKey.get(key);
    if (direct) addHit(direct, "owner_exact");

    if (!direct && loose.length >= 4) {
      for (const [ownerKey, row] of ownerIndexes.byKey) {
        const ownerLoose = normalizeForMatch(row.ownerName);
        if (ownerLoose === loose) addHit(row, "loose_exact");
      }
    }
  }

  if (!hits.size) {
    const norm = normalizeOwnerKey(raw);
    const loose = normalizeForMatch(raw);
    for (const [ownerKey, names] of nameAliasIndex) {
      for (const alias of names) {
        const aliasNorm = normalizeOwnerKey(alias);
        const aliasLoose = normalizeForMatch(alias);
        if (aliasNorm === norm || (loose.length >= 4 && aliasLoose === loose)) {
          const row = ownerIndexes.byKey.get(ownerKey);
          if (row) addHit(row, "alias_exact");
        }
      }
    }
  }

  if (!hits.size && (normalizeOwnerKey(raw).length >= 4 || normalizeForMatch(raw).length >= 4)) {
    const norm = normalizeOwnerKey(raw);
    const loose = normalizeForMatch(raw);
    for (const [ownerKey, row] of ownerIndexes.byKey) {
      const ownerLoose = normalizeForMatch(row.ownerName);
      if (
        (norm.length >= 4 && (ownerKey.includes(norm) || norm.includes(ownerKey))) ||
        (loose.length >= 4 && (ownerLoose.includes(loose) || loose.includes(ownerLoose)))
      ) {
        addHit(row, "partial");
      }
    }
  }

  return [...hits.values()];
}

function splitCompoundOwnerName(ownerName) {
  const raw = String(ownerName || "").trim();
  if (!raw) return [];
  const parts = raw
    .split(/\s*\|\s*/)
    .map((p) => p.replace(/^owner\s*\d+\s*:\s*/i, "").trim())
    .filter(Boolean);
  return parts.length ? parts : [raw];
}

/**
 * @param {object} contact
 * @param {{ calaHotelContact?: string, matchType?: string }} [calaClass]
 */
export function classifyContactRelevance(contact, calaClass = {}) {
  const company = String(contact.company || "").trim();
  const specialty = String(contact.specialty || "").trim();
  const title = String(contact.title || "").trim();
  const country = String(contact.country || "").trim();

  if (BROKER_COMPANY_PATTERN.test(company) || /\bbroker/i.test(specialty)) {
    return "broker";
  }
  if (
    calaClass.calaHotelContact === "yes" ||
    /hotel|hospitality|resort|hoteles|lodging/i.test(specialty) ||
    /hotel|hospitality|resort|hoteles/i.test(company) ||
    /chief|ceo|cfo|coo|president|owner|founder|director general|group ceo/i.test(title)
  ) {
    return "hospitality";
  }
  if (!company && !specialty) return "unknown";
  if (/united states|usa|u\.s\./i.test(country) && calaClass.calaHotelContact !== "yes") {
    return "broker";
  }
  return "other";
}

/**
 * @param {object} contact
 * @param {{ calaHotelContact?: string, matchType?: string }} [calaClass]
 */
export function scoreContactForOwnerPrimary(contact, calaClass = {}) {
  if (classifyContactRelevance(contact, calaClass) === "broker") return -100;

  let score = 0;
  if (calaClass.calaHotelContact === "yes") score += 40;
  if (calaClass.matchType === "owner_exact" || calaClass.matchType === "alias_exact") score += 20;
  if (/hotel|hospitality|resort|lodging/i.test(String(contact.specialty || ""))) score += 25;
  if (/chief|ceo|cfo|coo|president|owner|founder|director general|group ceo|managing director/i.test(String(contact.title || ""))) {
    score += 30;
  }
  if (contact.email) score += 15;
  if (contact.phone) score += 10;
  if (contact.mobilePhone) score += 12;
  if (contact.businessPhone) score += 8;
  if (contact.phoneVerificationTier === "VP2") score += 20;
  else if (contact.phoneVerificationTier === "VP1") score += 15;
  else if (contact.phoneVerificationTier === "VP3") score += 3;
  if (/hotel|hospitality|resort|hoteles/i.test(String(contact.company || ""))) score += 10;
  if (String(contact.sourceFile || "").includes("costar_profile_manual")) score += 35;
  if (String(contact.sourceFile || "").includes("registry_enrichment")) score += 40;
  if (contact.verificationTier === "V1R") score += 50;
  else if (contact.verificationTier === "V2") score += 45;
  else if (contact.verificationTier === "V3") score -= 40;
  if (contact.email && isNonPersonMailboxEmail(contact.email)) score -= 50;
  if (contact.verificationTier === "V1R" || contact.verificationUrl) score += 25;
  if (contact.legalRepresentativeName) score += 15;
  if (/\bformer\b|ex-ceo|ex-president|previous president|retired\b/i.test(String(contact.title || ""))) {
    score -= 100;
  }
  if (
    String(contact.sourceFile || "").includes("costar_profile_manual") &&
    !contact.verificationUrl
  ) {
    score -= 45;
  }
  return score;
}

/** Executive title rank — lower is better. */
export function executiveRank(title) {
  const t = String(title || "").toLowerCase();
  if (/\bceo\b|chief executive|president\b|founder|owner\b|director general|group ceo/.test(t)) return 1;
  if (/\bcfo\b|chief financial/.test(t)) return 2;
  if (/\bcoo\b|chief operating/.test(t)) return 3;
  if (/\bchief\b|svp|evp|managing director/.test(t)) return 4;
  return 9;
}

/**
 * @param {object[]} contacts
 */
export function pickPrimaryContact(contacts) {
  const ranked = contacts
    .map((c) => ({ contact: c, score: scoreContactForOwnerPrimary(c.contact, c.calaClass) }))
    .filter((r) => r.score > 0)
    .sort(
      (a, b) =>
        b.score - a.score ||
        executiveRank(a.contact.title) - executiveRank(b.contact.title) ||
        String(a.contact.name || "").localeCompare(String(b.contact.name || ""))
    );
  return ranked[0]?.contact || null;
}

/**
 * @param {import('./company-profile-enrichments.js').CompanyProfileEnrichment[]} enrichments
 * @param {ReturnType<buildOwnerTargetIndexes>} ownerIndexes
 */
export function buildEnrichmentOwnerIndex(enrichments, ownerIndexes) {
  /** @type {Map<string, import('./company-profile-enrichments.js').CompanyProfileEnrichment>} */
  const byOwnerId = new Map();

  for (const profile of enrichments) {
    const ot = profile.ownerTarget;
    if (!ot) continue;
    if (ot.ownerTargetId) {
      byOwnerId.set(ot.ownerTargetId, profile);
      continue;
    }
    for (const name of ot.preferredNames || [profile.company.name]) {
      const row = ownerIndexes.byKey.get(normalizeOwnerKey(name));
      if (row) byOwnerId.set(row.id, profile);
    }
  }
  return byOwnerId;
}
