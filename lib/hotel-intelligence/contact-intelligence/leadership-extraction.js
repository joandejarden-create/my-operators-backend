/**
 * Contact Intelligence V1.4 — leadership / people extraction.
 * Associates each name with its own title; separates board vs operating roles.
 * Fixtures drive regression — names are not hardcoded into discovery.
 */

export const LEADERSHIP_EXTRACTION_VERSION = "contact-leadership-extraction-v1.4";

export const LEADERSHIP_CATEGORY = Object.freeze({
  BOARD_OWNERSHIP: "BOARD_OWNERSHIP",
  OPERATING_EXECUTIVE: "OPERATING_EXECUTIVE",
  DEVELOPMENT_ASSET: "DEVELOPMENT_ASSET",
  UNKNOWN: "UNKNOWN",
});

const ROLE_WORDS =
  "CEO|CFO|COO|Founder|President|Presidente|Chairman|Chief\\s+(?:Executive|Development|Operating|Financial|Investment)\\s+Officer|Managing\\s+Director|Director(?:\\s+General)?|General\\s+Manager|Asset\\s+Manager|Vice\\s+President|VP|Partner|Investor|Consejero(?:\\s+Independiente)?|Miembro|Board\\s+Member|Gerente";

const NAV_NOISE =
  /cookie|sitio|navegaci|configurar|rechazar|aceptar|preferencias|brand site|contacto ri|información financiera|comunicados|estatutos|top of page|bottom of page|all rights reserved|click here to edit|i'm a title|leadership team|our team|about us|hotels expertise|inversionistas|gobierno corporativo|comité de|sign up|submit thanks/i;

/**
 * @param {string} title
 */
export function classifyLeadershipCategory(title) {
  const t = String(title || "");
  if (/consejero|board|presidente|chairman|miembro(?:\s+del\s+consejo)?/i.test(t) && !/ceo|chief|founder|development|asset|operating/i.test(t)) {
    return LEADERSHIP_CATEGORY.BOARD_OWNERSHIP;
  }
  if (/asset|development|desarroll|investment|invers|brand|conversi|affiliation/i.test(t)) {
    return LEADERSHIP_CATEGORY.DEVELOPMENT_ASSET;
  }
  if (/ceo|coo|cfo|founder|chief|managing director|general manager|gerente|operating|partner/i.test(t)) {
    return LEADERSHIP_CATEGORY.OPERATING_EXECUTIVE;
  }
  if (/presidente|president|chairman/i.test(t)) return LEADERSHIP_CATEGORY.BOARD_OWNERSHIP;
  return LEADERSHIP_CATEGORY.UNKNOWN;
}

function cleanName(raw) {
  return String(raw || "")
    .replace(/^(?:P|CI|M)\s+/i, "")
    .replace(/^(?:Lic\.|Ing\.|C\.P\.|CPA)\s+/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

const NAME_PARTICLE = /^(?:de|del|de\s+la|la|y|e|da|do|dos|das|van|von)$/i;

function isPlausiblePersonName(name) {
  const n = cleanName(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (NAV_NOISE.test(n)) return false;
  if (/^(te|ndiente|dependiente|ci|presidente|consejero)\b/i.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 7) return false;
  // Allow ALL CAPS (Dovetail), Title Case with accents, and particles (del, de la, …)
  const okPart = (p) =>
    NAME_PARTICLE.test(p) ||
    /^[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ.'-]+$/.test(p) ||
    /^[A-ZÁÉÍÓÚÑ]{2,}$/.test(p);
  if (!parts.every(okPart)) return false;
  // At least two non-particle tokens
  if (parts.filter((p) => !NAME_PARTICLE.test(p)).length < 2) return false;
  return true;
}

function titleCaseFromCaps(name) {
  if (!/^[A-ZÁÉÍÓÚÑ\s.'-]+$/.test(name)) return name;
  return name
    .toLowerCase()
    .split(/\s+/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

/**
 * GSF-style consejo: "P Lic. Name Name Presidente CI Name Name Consejero Independiente …"
 */
export function extractConsejoLeadership(text) {
  const people = [];
  const idx = text.search(/Consejo de Administraci[oó]n/i);
  if (idx < 0) return people;
  // Stop before next committee if present
  let slice = text.slice(idx, idx + 2500);
  const stop = slice.search(/Comité de Auditor|Comité de Prácticas|Committee|Estatutos Sociales(?!\s*Consejo)/i);
  if (stop > 80) slice = slice.slice(0, stop);

  const roleMap = {
    P: "Presidente",
    CI: "Consejero Independiente",
    M: "Miembro",
  };

  // Marker + optional honorific + name (incl. particles). Stop before title words / next marker.
  const nameToken =
    "(?:(?!Presidente|Consejero|Miembro|Comité)(?:[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚáéíóúñÑ.'-]+|(?:de|del|la|y|e|da|do|dos|das|van|von)))";
  const re = new RegExp(
    `\\b(P|CI|M)\\s+(?:(Lic\\.|Ing\\.|C\\.P\\.|CPA)\\s+)?(${nameToken}(?:\\s+${nameToken}){1,5})\\s*(Presidente|Consejero Independiente|Miembro)?(?=\\s+(?:P|CI|M)\\b|\\s+Comité|\\s*$)`,
    "g"
  );
  let m;
  while ((m = re.exec(slice))) {
    const marker = m[1].toUpperCase();
    const name = cleanName(m[3]);
    if (!isPlausiblePersonName(name)) continue;
    const title = (m[4] || roleMap[marker] || marker).trim();
    people.push({
      display_name: name,
      title,
      leadership_category: classifyLeadershipCategory(title),
      source_context: "consejo_de_administracion",
      extraction_method: "consejo_role_markers",
    });
  }
  return people;
}

/**
 * Dovetail-style: "PHIL HOSPOD Founder & CEO Phil Hospod is…" / "JAMES KOT Chief Development Officer James…"
 */
export function extractCapsNameLeadership(text) {
  const people = [];
  // Max 3 tokens; disallow section headings as name parts (prevents LEADERSHIP TEAM PHIL HOSPOD swallowing)
  const capsName =
    "((?:(?!LEADERSHIP|TEAM|ABOUT|CONTACT|HOTELS|EXPERTISE|SELECT|PRESS)[A-ZÁÉÍÓÚÑ]{2,})(?:[ \\t]+(?:(?!LEADERSHIP|TEAM|ABOUT|CONTACT|HOTELS|EXPERTISE|SELECT|PRESS)[A-ZÁÉÍÓÚÑ]{2,})){0,2})";
  const patterns = [
    new RegExp(
      `\\b${capsName}[ \\t]+(Founder[ \\t]*(?:&|and|\\+|\\/)[ \\t]*CEO|CEO[ \\t]*(?:&|and|\\+|\\/)[ \\t]*Founder)\\b`,
      "g"
    ),
    new RegExp(
      `\\b${capsName}[ \\t]+(Chief[ \\t]+(?:Executive|Development|Operating|Financial|Investment)[ \\t]+Officer)\\b`,
      "g"
    ),
    new RegExp(`\\b${capsName}[ \\t]+((?:${ROLE_WORDS}))\\b`, "g"),
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text))) {
      const caps = m[1].trim();
      if (/LEADERSHIP|TEAM|ABOUT|CONTACT|HOTELS|EXPERTISE|SELECT|PRESS/i.test(caps)) {
        re.lastIndex = m.index + 1;
        continue;
      }
      const title = m[2].replace(/\s+/g, " ").trim();
      const display_name = titleCaseFromCaps(caps);
      if (!isPlausiblePersonName(display_name)) continue;
      people.push({
        display_name,
        title,
        leadership_category: classifyLeadershipCategory(title),
        source_context: "leadership_team_block",
        extraction_method: "caps_name_then_title",
      });
    }
  }
  return people;
}

/**
 * Generic "Name – Title" / "Name, Title" when cleanly delimited.
 */
export function extractDelimitedLeadership(text) {
  const people = [];
  const re = new RegExp(
    `\\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ.'-]+){1,4})\\s*[,–—-]\\s*((?:${ROLE_WORDS})[^.]{0,50})`,
    "gi"
  );
  let m;
  while ((m = re.exec(text))) {
    const display_name = cleanName(m[1]);
    const title = m[2].replace(/\s+/g, " ").trim().slice(0, 80);
    if (!isPlausiblePersonName(display_name) || NAV_NOISE.test(title)) continue;
    people.push({
      display_name,
      title,
      leadership_category: classifyLeadershipCategory(title),
      source_context: "delimited_name_title",
      extraction_method: "delimited",
    });
  }
  return people;
}

/**
 * Prose founder: "founded by Phil Hospod in 2018"
 */
export function extractFoundedByLeadership(text) {
  const people = [];
  const re =
    /\bfounded by\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+(?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ.'-]+|de|del|da|do|van|von)){1,4})\b/gi;
  let m;
  while ((m = re.exec(text))) {
    const display_name = cleanName(m[1]);
    if (!isPlausiblePersonName(display_name)) continue;
    people.push({
      display_name,
      title: "Founder",
      leadership_category: LEADERSHIP_CATEGORY.OPERATING_EXECUTIVE,
      source_context: "founded_by_prose",
      extraction_method: "founded_by",
    });
  }
  return people;
}

/**
 * @param {string} text — already htmlToSearchableText
 * @param {string} pageUrl
 * @param {{ publication_date?: string|null, retrieved_at?: string|null }} [meta]
 */
export function extractLeadershipPeople(text, pageUrl, meta = {}) {
  const retrieved_at = meta.retrieved_at || new Date().toISOString();
  const publication_date = meta.publication_date || null; // undated page ≠ recent appointment proof

  const collected = [
    ...extractConsejoLeadership(text),
    ...extractCapsNameLeadership(text),
    ...extractFoundedByLeadership(text),
    ...extractDelimitedLeadership(text),
  ];

  const byKey = new Map();
  for (const p of collected) {
    const key = p.display_name
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "");
    const prev = byKey.get(key);
    const score = (x) =>
      x.extraction_method === "consejo_role_markers"
        ? 3
        : x.extraction_method === "caps_name_then_title"
          ? 2
          : 1;
    if (!prev || score(p) > score(prev)) {
      byKey.set(key, {
        ...p,
        source_url: pageUrl,
        publication_date,
        retrieved_at,
        role_currency_note: publication_date
          ? null
          : "Source page undated — retrieval date is not appointment evidence.",
      });
    }
  }

  return [...byKey.values()].slice(0, 12);
}

/**
 * Detect contact-form vs empty contact page.
 */
export function classifyContactPageMechanism(html, pageUrl) {
  const h = String(html || "");
  const hasForm =
    /<form[\s>]/i.test(h) &&
    (/type=["']submit["']/i.test(h) || /<button[^>]*type=["']submit["']/i.test(h) || />\s*Submit\s*</i.test(h));
  const hasMailto = /mailto:/i.test(h);
  const hasTel = /tel:/i.test(h);
  const isContactUrl = /contact|contacto|fale|contato/i.test(pageUrl || "");
  if (!isContactUrl && !hasForm && !hasMailto && !hasTel) {
    return { kind: "NOT_CONTACT_PAGE", has_form: false, has_mailto: false, has_tel: false };
  }
  if (hasForm) {
    return {
      kind: "CONTACT_FORM_FALLBACK",
      has_form: true,
      has_mailto: hasMailto,
      has_tel: hasTel,
      label: "Organization contact form (fallback — not phone/email endpoint)",
    };
  }
  if (hasMailto || hasTel) {
    return { kind: "CONTACT_ENDPOINTS", has_form: false, has_mailto: hasMailto, has_tel: hasTel };
  }
  return {
    kind: "CONTACT_PAGE_NO_MECHANISM",
    has_form: false,
    has_mailto: false,
    has_tel: false,
    label: "Contact page present but no form, mailto, or tel mechanism detected",
  };
}
