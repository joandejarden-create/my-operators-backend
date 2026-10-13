/**
 * Reusable investor / IR document discovery for Contact Intelligence.
 * Learns from teacher pattern: named business emails often live in investor PDFs
 * and IR contact pages, not homepage extracts.
 *
 * Does NOT hardcode people, emails, or successful URLs into rules.
 */

export const INVESTOR_DOC_DISCOVERY_VERSION = "contact-investor-doc-discovery-v1";

const EMAIL_AGGREGATOR_HOST_RE =
  /contactout\.com|apollo\.io|zoominfo\.com|rocketreach\.co|hunter\.io|snov\.io|clearbit\.com|lusha\.com|signalhire\.com|seamless\.ai|anymailfinder\.com|voilanorbert\.com|prospeo\.io/i;

function isEmailAggregatorUrl(url) {
  try {
    return EMAIL_AGGREGATOR_HOST_RE.test(new URL(url).hostname);
  } catch {
    return EMAIL_AGGREGATOR_HOST_RE.test(String(url || ""));
  }
}
const INVESTOR_PATH_RE =
  /investors?|inversionistas?|investor-relations|relaci[oó]n.?con.?inversionistas|webcast|earnings|reportes?|annual.?report|informe.?anual|gobierno.?corporativo|bmv|sec\.gov|edgar/i;

const PDF_RE = /\.pdf(\?|$)/i;

/** Locale-aware query templates — placeholders only; never bake target emails. */
export const INVESTOR_SEARCH_TEMPLATES = Object.freeze({
  en: [
    'site:{domain} (investors OR "investor relations" OR webcast OR "annual report") filetype:pdf',
    'site:{domain} ("investor relations" OR IR) (contact OR director OR email)',
    '"{org_name}" ("investor relations" OR IR) (email OR contact) filetype:pdf',
  ],
  es: [
    'site:{domain} (inversionistas OR "relación con inversionistas" OR webcast OR "informe anual") filetype:pdf',
    'site:{domain} (inversionistas OR IR) (contacto OR director OR correo)',
    '"{org_name}" (inversionistas OR "relación con inversionistas") (correo OR email) filetype:pdf',
  ],
  pt: [
    'site:{domain} (investidores OR "relações com investidores" OR webcast OR "relatório anual") filetype:pdf',
    'site:{domain} (investidores OR RI) (contato OR diretor OR email)',
    '"{org_name}" (investidores OR RI) (email OR contato) filetype:pdf',
  ],
});

export function buildInvestorSearchQueries({
  domain,
  orgName,
  language = "en",
  maxQueries = 3,
} = {}) {
  const host = String(domain || "")
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "")
    .toLowerCase();
  const org = String(orgName || "").trim();
  if (!host && !org) return [];
  const lang = ["en", "es", "pt"].includes(language) ? language : "en";
  const templates = INVESTOR_SEARCH_TEMPLATES[lang] || INVESTOR_SEARCH_TEMPLATES.en;
  return templates
    .slice(0, maxQueries)
    .map((t) =>
      t.replace(/\{domain\}/g, host || "example.com").replace(/\{org_name\}/g, org || host)
    )
    .filter((q) => !q.includes("example.com") || host);
}

export function scoreInvestorDocumentCandidate(url, { title = "", snippet = "" } = {}) {
  const u = String(url || "");
  if (!u || isEmailAggregatorUrl(u)) return { score: 0, reasons: ["rejected_or_empty"] };
  let score = 0;
  const reasons = [];
  if (PDF_RE.test(u)) {
    score += 3;
    reasons.push("pdf");
  }
  if (INVESTOR_PATH_RE.test(u) || INVESTOR_PATH_RE.test(title) || INVESTOR_PATH_RE.test(snippet)) {
    score += 4;
    reasons.push("investor_ir_path_or_label");
  }
  if (/contacto|contact|correo|email/i.test(u + title + snippet)) {
    score += 1;
    reasons.push("contact_signal");
  }
  return { score, reasons };
}

/**
 * Rank organic search hits for investor/IR document follow-up.
 */
export function rankInvestorDocumentUrls(results = [], { limit = 5 } = {}) {
  const ranked = [];
  for (const r of results || []) {
    const url = r.url || r.link;
    if (!url) continue;
    const { score, reasons } = scoreInvestorDocumentCandidate(url, {
      title: r.title,
      snippet: r.snippet || r.description,
    });
    if (score < 3) continue;
    ranked.push({
      url,
      title: r.title || null,
      snippet: r.snippet || r.description || null,
      score,
      reasons,
    });
  }
  ranked.sort((a, b) => b.score - a.score);
  const seen = new Set();
  const out = [];
  for (const row of ranked) {
    const key = String(row.url).split("#")[0].toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
    if (out.length >= limit) break;
  }
  return out;
}

/**
 * Extract explicitly attributed person emails from document text.
 * Requires name token proximity — never returns bare pattern guesses.
 */
export function extractAttributedPersonEmailsFromText(text, { people = [], maxPerPerson = 2 } = {}) {
  const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const GENERIC =
    /^(info|contact|contacto|hello|admin|office|press|media|reservas|reservations|sales|ventas|noreply|no-reply|inversionistas|investors)$/i;
  const t = String(text || "");
  if (!t || !people.length) return [];

  const findings = [];
  for (const person of people) {
    const name = String(person.full_name || person.display_name || person.name || "").trim();
    if (!name) continue;
    const tokens = name
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .split(/\s+/)
      .filter((x) => x.length >= 4);
    if (!tokens.length) continue;

    const emails = t.match(EMAIL_RE) || [];
    const seen = new Set();
    for (const raw of emails) {
      const email = String(raw).toLowerCase();
      if (seen.has(email)) continue;
      const local = email.split("@")[0] || "";
      if (GENERIC.test(local)) continue;
      const idx = t.toLowerCase().indexOf(email);
      if (idx < 0) continue;
      const window = t.slice(Math.max(0, idx - 450), idx + email.length + 450);
      const windowNorm = window
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();
      const hitTokens = tokens.filter((tok) => windowNorm.includes(tok));
      // Compound surnames: require ≥1 strong token; prefer ≥2 when available
      const need = Math.min(2, tokens.length);
      if (hitTokens.length < Math.min(1, need)) continue;
      if (tokens.length >= 2 && hitTokens.length < 1) continue;
      seen.add(email);
      findings.push({
        person_name: name,
        email,
        excerpt: window.replace(/\s+/g, " ").trim().slice(0, 280),
        classification: "first-party_or_document_text",
        attribution_basis: "name_token_proximity_in_document",
        hit_tokens: hitTokens,
      });
      if (seen.size >= maxPerPerson) break;
    }
  }
  return findings;
}

/**
 * Concurrent affiliation helper — person identity vs target-org employment.
 * Matching LinkedIn / name does not auto-fail when another org appears as current title.
 */
export function classifyAffiliationVsTarget({
  returnedCompanyDomain = null,
  returnedCompanyName = null,
  returnedTitle = null,
  targetDomain = null,
  targetOrgName = null,
  linkedinMatches = false,
  nameMatches = false,
} = {}) {
  if (!nameMatches && !linkedinMatches) {
    return {
      class: "PERSON_IDENTITY_NOT_ESTABLISHED",
      target_affiliation: "unknown",
    };
  }
  const td = String(targetDomain || "")
    .replace(/^www\./, "")
    .toLowerCase();
  const rd = String(returnedCompanyDomain || "")
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "")
    .toLowerCase();
  const tn = String(targetOrgName || "").toLowerCase();
  const rn = String(returnedCompanyName || "").toLowerCase();
  const domainMatch = Boolean(td && rd && (rd === td || rd.endsWith(`.${td}`) || td.endsWith(`.${rd}`)));
  const nameOrgMatch =
    Boolean(tn && rn) &&
    (rn.includes(tn.split(/\s+/)[0] || "___") ||
      tn.split(/\s+/).filter((w) => w.length > 3).some((w) => rn.includes(w)));

  if (domainMatch || nameOrgMatch) {
    return {
      class: "PERSON_MATCH_TARGET_AFFILIATION_ALIGNED",
      target_affiliation: "aligned",
      returned_title: returnedTitle,
    };
  }
  if (linkedinMatches || nameMatches) {
    return {
      class: "PERSON_MATCH_TARGET_AFFILIATION_MISMATCH_OR_AMBIGUITY",
      target_affiliation: "mismatch_or_ambiguous_concurrent_possible",
      note: "Person identity supported; current-company field may reflect a concurrent/secondary affiliation. Do not auto-declare a different person.",
      returned_title: returnedTitle,
      returned_company_domain: rd || null,
      target_domain: td || null,
    };
  }
  return { class: "UNRESOLVED", target_affiliation: "unknown" };
}
