/**
 * Resolve + validate a customer-facing "learn more about this person" link.
 *
 * Prefer LinkedIn / explicit profile / contact source URLs.
 * Do not promote generic company About pages unless the person's name is
 * evidenced on the page (or the URL was steward-attached on the contact).
 *
 * Broken / unreachable / placeholder URLs are suppressed.
 */
export const GDI_PERSON_SOURCE_LINK_V1 = "gdi-person-source-link-v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

export function isPlaceholderPersonUrl(url) {
  const s = text(url).toLowerCase();
  if (!s) return true;
  return (
    /example\.(com|org|net)|localhost|127\.0\.0\.1|0\.0\.0\.0|about:blank|javascript:|data:|#todo|#tbd|\/tbd\b|\/todo\b|your[-_]?link|link[-_]?here|placeholder|null|undefined/i.test(
      s,
    ) || s.length < 12
  );
}

export function normalizePersonSourceUrl(raw) {
  const s = text(raw);
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  if (/^www\./i.test(s) || /^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}([/?#].*)?$/i.test(s)) {
    return `https://${s}`;
  }
  return "";
}

export function isSafePersonSourceUrl(url) {
  const s = normalizePersonSourceUrl(url);
  if (!s || isPlaceholderPersonUrl(s)) return false;
  try {
    const u = new URL(s);
    if (u.protocol !== "https:" && u.protocol !== "http:") return false;
    if (!u.hostname || !u.hostname.includes(".")) return false;
    return true;
  } catch {
    return false;
  }
}

export function classifyPersonSourceUrl(url) {
  const s = text(url).toLowerCase();
  if (/linkedin\.com\/in\//i.test(s)) return "LINKEDIN_PROFILE";
  if (/linkedin\.com\//i.test(s)) return "LINKEDIN";
  if (/\/(about|nosotros|team|equipo|staff|people|leadership|our-team|bios?)\b/i.test(s)) {
    return "ORG_ABOUT_OR_TEAM";
  }
  if (/\.pdf(\b|$)/i.test(s)) return "DOCUMENT";
  return "PROFILE_OR_SOURCE";
}

export function personSourceLinkLabel(kind) {
  switch (kind) {
    case "LINKEDIN_PROFILE":
    case "LINKEDIN":
      return "LinkedIn profile";
    case "ORG_ABOUT_OR_TEAM":
      return "Company / team page";
    case "DOCUMENT":
      return "Source document";
    default:
      return "Profile / source";
  }
}

function nameTokens(personName) {
  return text(personName)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-zA-Z]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 3);
}

export function htmlMentionsPerson(html, personName) {
  const body = String(html || "");
  if (!body || !text(personName)) return false;
  // Prefer visible text over raw markup/scripts for person evidence.
  const visible = body
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ");
  const hay = visible
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const full = text(personName)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  if (full && hay.includes(full)) return true;
  const tokens = nameTokens(personName);
  if (tokens.length < 2) return false;
  // Require first+last near each other (not just both somewhere on a large About page).
  const first = tokens[0].toLowerCase();
  const last = tokens[tokens.length - 1].toLowerCase();
  const near = new RegExp(
    `${first}.{0,40}${last}|${last}.{0,40}${first}`,
    "i",
  );
  return near.test(hay);
}

/**
 * Collect candidate URLs from a contact slot + opportunity.
 * Order = preference.
 */
export function collectPersonSourceUrlCandidates(slot = {}, opportunity = {}) {
  const pc = opportunity.primaryContact || {};
  const attr = opportunity.contactAttribution || {};
  const evidence = Array.isArray(pc.evidenceUrls) ? pc.evidenceUrls : [];
  const raw = [
    slot.linkedInUrl,
    slot.profileUrl,
    slot.sourceUrl,
    pc.linkedinUrl,
    pc.linkedInUrl,
    pc.profileUrl,
    pc.sourceUrl,
    ...evidence,
    attr.relationshipEvidenceUrl,
    slot.officialContactUrl,
    pc.officialContactUrl,
  ];
  const out = [];
  const seen = new Set();
  for (const r of raw) {
    const url = normalizePersonSourceUrl(r);
    if (!isSafePersonSourceUrl(url)) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(url);
  }
  return out;
}

/**
 * Live reachability check. LinkedIn is shape-trusted (often bot-blocked).
 * Org/about/document pages require HTTP OK; HTML pages should mention the person
 * when a personName is provided (except explicit LinkedIn).
 */
export async function verifyPersonSourceLink(url, opts = {}) {
  const personName = text(opts.personName);
  const timeoutMs = Number(opts.timeoutMs || 8000);
  const normalized = normalizePersonSourceUrl(url);
  if (!isSafePersonSourceUrl(normalized)) {
    return {
      ok: false,
      url: normalized || text(url),
      reason: "INVALID_URL_SHAPE",
    };
  }
  const kind = classifyPersonSourceUrl(normalized);
  if (kind === "LINKEDIN_PROFILE" || kind === "LINKEDIN") {
    // LinkedIn commonly blocks automated fetch; still a valid customer click-through.
    // Require /in/ profile shape so company pages are not treated as person links.
    if (kind !== "LINKEDIN_PROFILE") {
      return {
        ok: false,
        url: normalized,
        kind,
        reason: "LINKEDIN_NOT_PERSON_PROFILE",
      };
    }
    return {
      ok: true,
      url: normalized,
      kind,
      label: personSourceLinkLabel(kind),
      verifiedVia: "SHAPE_LINKEDIN",
      httpStatus: null,
      nameMentioned: null,
    };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const requireName =
    Boolean(personName) &&
    (kind === "ORG_ABOUT_OR_TEAM" || kind === "PROFILE_OR_SOURCE" || kind === "DOCUMENT");
  try {
    // Always GET when we may need a name check — HEAD alone often omits useful
    // content-type / body and previously allowed About pages through as HTTP_OK.
    let res = await fetch(normalized, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": "DealalityPersonSourceLinkCheck/1.0",
        Accept: "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8",
      },
    });
    clearTimeout(timer);
    if (!res.ok) {
      return {
        ok: false,
        url: normalized,
        kind,
        reason: "HTTP_NOT_OK",
        httpStatus: res.status,
      };
    }
    const ctype = String(res.headers.get("content-type") || "");
    let nameMentioned = null;
    if (/text\/html/i.test(ctype) || requireName) {
      const html = await res.text();
      if (personName) {
        nameMentioned = htmlMentionsPerson(html, personName);
        if (!nameMentioned) {
          return {
            ok: false,
            url: normalized,
            kind,
            reason: "PERSON_NAME_NOT_FOUND_ON_PAGE",
            httpStatus: res.status,
            nameMentioned: false,
          };
        }
      }
    } else if (requireName) {
      return {
        ok: false,
        url: normalized,
        kind,
        reason: "NO_HTML_BODY_FOR_NAME_CHECK",
        httpStatus: res.status,
      };
    }
    // Never promote org/about/generic pages on reachability alone.
    if (requireName && nameMentioned !== true) {
      return {
        ok: false,
        url: normalized,
        kind,
        reason: "PERSON_NAME_REQUIRED",
        httpStatus: res.status,
        nameMentioned: false,
      };
    }
    return {
      ok: true,
      url: normalized,
      kind,
      label: personSourceLinkLabel(kind),
      verifiedVia: nameMentioned === true ? "HTTP_AND_NAME" : "HTTP_OK",
      httpStatus: res.status,
      nameMentioned,
    };
  } catch (err) {
    clearTimeout(timer);
    return {
      ok: false,
      url: normalized,
      kind,
      reason: "FETCH_FAILED",
      message: String(err?.message || err),
    };
  }
}

/**
 * Pick the first verified person source link from candidates.
 */
export async function resolveVerifiedPersonSourceLink(slot = {}, opportunity = {}, opts = {}) {
  const personName = text(slot.name || opportunity.primaryContact?.name || opts.personName);
  const candidates = collectPersonSourceUrlCandidates(slot, opportunity);
  const failures = [];
  for (const url of candidates) {
    const verified = await verifyPersonSourceLink(url, {
      personName,
      timeoutMs: opts.timeoutMs,
    });
    if (verified.ok) {
      return {
        ok: true,
        personName: personName || null,
        link: {
          url: verified.url,
          kind: verified.kind,
          label: verified.label,
          verifiedVia: verified.verifiedVia,
        },
        failures,
      };
    }
    failures.push(verified);
  }
  return {
    ok: false,
    personName: personName || null,
    link: null,
    failures,
    reason: candidates.length ? "NO_VERIFIED_CANDIDATE" : "NO_CANDIDATE_URLS",
  };
}

/**
 * Sync shape-only resolution for UI when async verify is unavailable.
 * Still rejects placeholders; does not claim HTTP verification.
 */
export function resolvePersonSourceLinkShapeOnly(slot = {}, opportunity = {}) {
  const candidates = collectPersonSourceUrlCandidates(slot, opportunity);
  for (const url of candidates) {
    const kind = classifyPersonSourceUrl(url);
    // Shape-only: allow LinkedIn + explicit profile/source on the contact slot/PC.
    // Do not auto-promote attribution About pages without verify.
    const onContact =
      [
        slot.linkedInUrl,
        slot.profileUrl,
        slot.sourceUrl,
        opportunity.primaryContact?.linkedinUrl,
        opportunity.primaryContact?.linkedInUrl,
        opportunity.primaryContact?.profileUrl,
        opportunity.primaryContact?.sourceUrl,
      ]
        .map(normalizePersonSourceUrl)
        .filter(Boolean)
        .some((u) => u.toLowerCase().replace(/\/$/, "") === url.toLowerCase().replace(/\/$/, ""));
    if (kind === "LINKEDIN_PROFILE" || kind === "LINKEDIN" || onContact) {
      return {
        ok: true,
        url,
        kind,
        label: personSourceLinkLabel(kind),
        verifiedVia: "SHAPE_ONLY",
      };
    }
  }
  return { ok: false, url: null, reason: "NO_SHAPE_SAFE_CONTACT_URL" };
}

export default {
  GDI_PERSON_SOURCE_LINK_V1,
  isPlaceholderPersonUrl,
  normalizePersonSourceUrl,
  isSafePersonSourceUrl,
  classifyPersonSourceUrl,
  personSourceLinkLabel,
  htmlMentionsPerson,
  collectPersonSourceUrlCandidates,
  verifyPersonSourceLink,
  resolveVerifiedPersonSourceLink,
  resolvePersonSourceLinkShapeOnly,
};
