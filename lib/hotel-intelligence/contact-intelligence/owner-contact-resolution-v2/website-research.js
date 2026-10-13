/**
 * Deterministic owner-website contact/leadership research.
 */

import { crawlDomainPaths, fetchPage } from "./web-retrieval.js";
import { rankRoleTier, classifySourceClass, createEvidenceFact, isOwnerSideRole } from "./roles-and-sources.js";
import { SOURCE_CLASS } from "./vocabulary.js";

export const WEBSITE_RESEARCH_V2 = "owner-website-research-v2";

export const OWNER_SITE_PATHS = Object.freeze([
  "/",
  "/about",
  "/about-us",
  "/company",
  "/team",
  "/leadership",
  "/management",
  "/contact",
  "/contact-us",
  "/portfolio",
  "/hotels",
  "/projects",
  "/news",
  "/nosotros",
  "/equipo",
  "/liderazgo",
  "/directivos",
  "/contacto",
  "/cartera",
  "/hoteles",
  "/proyectos",
  "/noticias",
  "/sobre",
  "/equipe",
  "/lideranca",
  "/contato",
  "/hoteis",
  "/projetos",
]);

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const PHONE_RE =
  /(?:\+|00)?(?:\d[\s().-]?){8,14}\d/g;
const LINKEDIN_RE = /https?:\/\/(?:www\.)?linkedin\.com\/in\/[a-zA-Z0-9_-]+\/?/gi;
const PERSON_TITLE_RE =
  /([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})\s*[-–,|]\s*([A-Za-zÁÉÍÓÚÑáéíóúñ &]{3,60})/g;

export function scorePathPriority(path) {
  const p = String(path || "").toLowerCase();
  let s = 0;
  if (/team|leadership|liderazgo|equipo|directivos|management/.test(p)) s += 40;
  if (/contact|contacto|contato/.test(p)) s += 35;
  if (/about|nosotros|sobre|company/.test(p)) s += 25;
  if (/portfolio|hotels|hoteles|cartera|projetos|proyectos/.test(p)) s += 15;
  if (p === "/" || p === "") s += 10;
  return s;
}

export function selectPathsForCrawl({ country = null, max = 10 } = {}) {
  const scored = OWNER_SITE_PATHS.map((p) => ({ path: p, score: scorePathPriority(p) }));
  // Prefer ES/PT variants when country hints
  const c = String(country || "").toLowerCase();
  if (/mexico|colombia|dominican|costa\s*rica|spain|argentin|chile|peru/.test(c)) {
    for (const row of scored) {
      if (/nosotros|equipo|liderazgo|contacto|hoteles/.test(row.path)) row.score += 8;
    }
  }
  if (/brazil|brasil|portugal/.test(c)) {
    for (const row of scored) {
      if (/sobre|equipe|contato|hoteis/.test(row.path)) row.score += 8;
    }
  }
  return scored.sort((a, b) => b.score - a.score).slice(0, max).map((r) => r.path);
}

function extractFromText(text, url, domain) {
  const emails = [...new Set((String(text).match(EMAIL_RE) || []))]
    .filter((e) => !/example\.com|wixpress|sentry|schema\.org/i.test(e))
    .filter((e) => {
      const host = e.split("@")[1]?.toLowerCase();
      return !host || host.includes(domain.replace(/^www\./, "").split(".").slice(-2).join(".")) || true;
    })
    .slice(0, 20);

  const phones = [...new Set(String(text).match(PHONE_RE) || [])].slice(0, 12);
  const linkedins = [...new Set(String(text).match(LINKEDIN_RE) || [])].slice(0, 10);

  const people = [];
  let m;
  const re = new RegExp(PERSON_TITLE_RE.source, "g");
  while ((m = re.exec(text)) !== null) {
    const full_name = m[1].trim();
    const title = m[2].trim();
    if (!isOwnerSideRole(title)) continue;
    people.push({
      full_name,
      title,
      role_tier: rankRoleTier(title),
      linkedin_url: null,
      evidence: [
        createEvidenceFact({
          claim: "person_title_on_owner_site",
          source_url: url,
          source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
          extracted_text_or_fact: m[0].slice(0, 200),
        }),
      ],
    });
  }

  // Attach LinkedIn if name token appears near URL (simple)
  for (const li of linkedins) {
    const slug = li.split("/in/")[1]?.replace(/\/$/, "") || "";
    const match = people.find((p) =>
      slug.toLowerCase().includes(String(p.full_name.split(/\s+/)[0] || "").toLowerCase())
    );
    if (match && !match.linkedin_url) match.linkedin_url = li;
  }

  return { emails, phones, linkedins, people };
}

/**
 * Research owner domain for org + person contacts.
 */
export async function researchOwnerWebsite(domain, { country = null, fetchOpts = {} } = {}) {
  const paths = selectPathsForCrawl({ country, max: fetchOpts.maxPaths || 5 });
  const pages = await crawlDomainPaths(domain, paths, {
    ...fetchOpts,
    maxOkPages: 5,
  });

  const org = { general_emails: [], corporate_phones: [], website: `https://${domain}` };
  const people = [];
  const sources_opened = [];

  for (const page of pages) {
    sources_opened.push({ url: page.url, ok: page.ok, provider: page.provider, path: page.path });
    if (!page.ok || !page.text) continue;
    const extracted = extractFromText(page.text, page.url, domain);
    for (const e of extracted.emails) {
      if (/info|contact|contacto|hello|office|press|media|sales|ventas/i.test(e.split("@")[0])) {
        org.general_emails.push(e);
      }
    }
    org.corporate_phones.push(...extracted.phones);
    for (const p of extracted.people) {
      if (!people.find((x) => x.full_name.toLowerCase() === p.full_name.toLowerCase())) {
        people.push(p);
      }
    }
    // Also try mailto-attributed names later in email lane
    for (const e of extracted.emails) {
      if (!/info|contact|contacto|hello|office|press|reservas|reservations/i.test(e.split("@")[0])) {
        // candidate person-local email without name — keep as org for now unless first.last
        if (/^[a-z]+\.[a-z]+@/i.test(e)) {
          const [local] = e.split("@");
          const [a, b] = local.split(".");
          const guessName = `${a[0].toUpperCase()}${a.slice(1)} ${b[0].toUpperCase()}${b.slice(1)}`;
          people.push({
            full_name: guessName,
            title: "Unknown (email-derived)",
            role_tier: 3,
            email_hint: e,
            linkedin_url: null,
            evidence: [
              createEvidenceFact({
                claim: "email_pattern_on_owner_site",
                source_url: page.url,
                source_type: SOURCE_CLASS.OFFICIAL_OWNER_SITE,
                extracted_text_or_fact: e,
              }),
            ],
          });
        }
      }
    }
  }

  people.sort((a, b) => a.role_tier - b.role_tier);
  return {
    domain,
    paths_attempted: paths,
    sources_opened,
    organization_contacts: {
      general_email: [...new Set(org.general_emails)][0] || null,
      corporate_phone: [...new Set(org.corporate_phones)][0] || null,
      website: org.website,
      all_emails: [...new Set(org.general_emails)].slice(0, 10),
      all_phones: [...new Set(org.corporate_phones)].slice(0, 10),
    },
    people: people.slice(0, 15),
    method_id: WEBSITE_RESEARCH_V2,
  };
}
