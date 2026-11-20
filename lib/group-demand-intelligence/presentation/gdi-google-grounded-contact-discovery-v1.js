/**
 * Google-grounded (Gemini + google_search) contact discovery for GDI.
 *
 * PRIMARY public research step when GEMINI_API_KEY is configured.
 * AI text + snippets are discovery leads only — channels persist only after
 * authoritative source fetch + named-person association (existing hierarchy).
 *
 * Does not scrape consumer Google AI Mode / browser sessions.
 * Does not invent emails, phones, or LinkedIn URLs.
 */

import {
  runGeminiVisibilityPrompt,
  extractGeminiUsage,
  estimateGeminiCost,
} from "../../ai-visibility/providers/gemini.js";
import { fetchPublicHtmlSafe } from "./gdi-safe-outbound-fetch-v1.js";

export const GDI_GOOGLE_GROUNDED_CONTACT_DISCOVERY_V1 =
  "gdi-google-grounded-contact-discovery-v1";

export const LINKEDIN_PROFILE_MATCH = Object.freeze({
  VERIFIED_LINKEDIN_PROFILE: "VERIFIED_LINKEDIN_PROFILE",
  CANDIDATE_LINKEDIN_PROFILE: "CANDIDATE_LINKEDIN_PROFILE",
  PROFILE_MATCH_UNCERTAIN: "PROFILE_MATCH_UNCERTAIN",
  NO_PROFILE_FOUND: "NO_PROFILE_FOUND",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function keyPresent(name, env = process.env) {
  return Boolean(text(env[name]));
}

export function inspectGoogleGroundedContactCredentials(env = process.env) {
  const gemini =
    keyPresent("GEMINI_API_KEY", env) || keyPresent("FDD_GEMINI_API_KEY", env);
  return {
    version: GDI_GOOGLE_GROUNDED_CONTACT_DISCOVERY_V1,
    geminiConfigured: gemini,
    envVarPreferred: keyPresent("GEMINI_API_KEY", env)
      ? "GEMINI_API_KEY"
      : keyPresent("FDD_GEMINI_API_KEY", env)
        ? "FDD_GEMINI_API_KEY"
        : null,
    groundingTool: "google_search",
    scrapesConsumerAiMode: false,
  };
}

/**
 * Classify a LinkedIn /in/ URL against the named person + organization.
 * Name-only matches stay CANDIDATE / UNCERTAIN — never auto-verified.
 */
export function classifyLinkedInProfileMatch(args = {}) {
  const url = text(args.url || args.linkedinUrl);
  const personName = text(args.personName || args.name);
  const org = text(args.organization || args.org);
  const title = text(args.title || args.role);
  const evidenceBlob = stripAccents(
    `${args.evidenceText || ""} ${args.snippet || ""} ${args.titleEvidence || ""}`,
  ).toLowerCase();

  if (!url || !/linkedin\.com\/in\//i.test(url)) {
    return {
      status: LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
      url: null,
      reasons: ["no_linkedin_in_url"],
    };
  }

  const nameKey = stripAccents(personName).toLowerCase();
  const parts = nameKey.split(/\s+/).filter((p) => p.length > 1);
  const first = parts[0] || "";
  const last = parts.length ? parts[parts.length - 1] : "";
  const slug = (() => {
    try {
      const u = new URL(url);
      return decodeURIComponent(u.pathname.replace(/^\/in\//i, "").replace(/\/$/, ""))
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
    } catch {
      return "";
    }
  })();

  const reasons = [];
  const nameInSlug =
    first &&
    last &&
    slug.includes(first.slice(0, 4)) &&
    slug.includes(last.slice(0, 4));
  if (nameInSlug) reasons.push("name_tokens_in_slug");

  const orgKey = stripAccents(org).toLowerCase();
  const orgTokens = orgKey
    .split(/\s+/)
    .filter((t) => t.length > 3 && !/^(de|del|la|las|los|the|and|y)$/i.test(t))
    .slice(0, 4);
  const orgInEvidence = orgTokens.some((t) => evidenceBlob.includes(t));
  if (orgInEvidence) reasons.push("organization_in_evidence");

  const titleKey = stripAccents(title).toLowerCase();
  const titleTokens = titleKey
    .split(/\s+/)
    .filter((t) => t.length > 4)
    .slice(0, 3);
  const titleInEvidence = titleTokens.some((t) => evidenceBlob.includes(t));
  if (titleInEvidence) reasons.push("title_in_evidence");

  if (nameInSlug && (orgInEvidence || titleInEvidence)) {
    return {
      status: LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE,
      url,
      reasons,
    };
  }
  if (nameInSlug || orgInEvidence) {
    return {
      status: LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE,
      url,
      reasons: reasons.length ? reasons : ["weak_name_or_org_signal"],
    };
  }
  return {
    status: LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN,
    url,
    reasons: ["url_present_without_strong_identity_match"],
  };
}

export function buildGoogleGroundedContactPrompt(person = {}, organization = {}) {
  const name = text(person.name || person.fullName);
  const title = text(person.title || person.role);
  const org = text(
    organization.name || organization.organizationName || person.organization,
  );
  const domain = text(organization.domain || person.domain);
  const nameVariants = [];
  const cleaned = stripAccents(name);
  if (cleaned && cleaned !== name) nameVariants.push(cleaned);
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) nameVariants.push(`${parts[0]} ${parts[parts.length - 1]}`);

  return {
    promptId: "gdi_google_grounded_contact_v1",
    text: [
      `Find the publicly listed professional contact information for ${name}${org ? ` at ${org}` : ""}.`,
      title ? `Known role/title: ${title}.` : null,
      domain ? `Prefer official pages on ${domain} and related organizational websites.` : null,
      "Search official directories, staff profiles, hospital/government contact pages, event organizer pages, and institutional directories.",
      nameVariants.length
        ? `Also consider name variants without accents or with alternate surname order: ${nameVariants.join("; ")}.`
        : null,
      "Use Spanish and English terms as appropriate (correo, teléfono, extensión, anexo, directorio, contacto, LinkedIn).",
      "",
      "Identify when publicly available:",
      "- Verified business email for this named person (not a generic inbox unless that is all that exists — label it)",
      "- Office telephone and extension(s)",
      "- Direct business number if distinct from the switchboard",
      "- Public LinkedIn profile URL for this exact person at this organization",
      "- Current role and employer confirmation",
      "",
      "Provide the ORIGINAL source URLs (official pages), not only search-result wrappers.",
      "Do not infer missing emails or invent telephone numbers or LinkedIn URLs.",
      "If a fact is uncertain, say so and still list candidate source URLs for verification.",
      "",
      "Format a short structured block:",
      "EMAIL: ...",
      "PHONE: ...",
      "EXTENSIONS: ...",
      "LINKEDIN: ...",
      "SOURCES: url1 ; url2",
      "NOTES: ...",
    ]
      .filter(Boolean)
      .join("\n"),
  };
}

/** Progressive follow-up when first grounded answer lacks verifiable channels. */
export function buildGoogleGroundedFollowUpPrompt(
  person = {},
  organization = {},
  prior = {},
) {
  const name = text(person.name || person.fullName);
  const org = text(
    organization.name || organization.organizationName || person.organization,
  );
  const domain = text(organization.domain || person.domain);
  const priorUrls = (prior.sourceUrls || []).slice(0, 6).join(" ; ");
  const missing = [];
  if (!prior.candidates?.email) missing.push("business email");
  if (!prior.candidates?.phone) missing.push("office telephone and extension");
  if (!prior.candidates?.linkedInUrl) missing.push("LinkedIn profile URL");

  return {
    promptId: "gdi_google_grounded_contact_followup_v1",
    text: [
      `Continue researching ${name}${org ? ` at ${org}` : ""}.`,
      missing.length
        ? `Still missing independently listed: ${missing.join(", ")}.`
        : "Confirm official source URLs for the contact details already suggested.",
      domain ? `Focus on site:${domain} directories and official contact pages.` : null,
      "Find the official staff directory / directorio / contacto page that lists this person by name.",
      priorUrls
        ? `Prior candidate URLs (may include wrappers — prefer the final official page): ${priorUrls}`
        : null,
      "Return ORIGINAL source URLs plus EMAIL / PHONE / EXTENSIONS / LINKEDIN when publicly listed.",
      "Do not invent missing information.",
      "",
      "EMAIL: ...",
      "PHONE: ...",
      "EXTENSIONS: ...",
      "LINKEDIN: ...",
      "SOURCES: url1 ; url2",
      "NOTES: ...",
    ]
      .filter(Boolean)
      .join("\n"),
  };
}

export function isGoogleGroundingWrapperUrl(url) {
  return /vertexaisearch\.cloud\.google|google\.com\/search|googleapis\.com\/|googleusercontent\.com\/grounding/i.test(
    String(url || ""),
  );
}

/**
 * Parse candidate leads from Gemini text. These are NOT verified channels.
 */
export function parseGoogleGroundedContactCandidates(rawText = "", grounding = {}) {
  const textBody = text(rawText);
  const urls = [];
  const seen = new Set();
  const pushUrl = (u) => {
    const s = text(u);
    if (!s || !/^https?:\/\//i.test(s)) return;
    const key = s.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) return;
    seen.add(key);
    urls.push(s);
  };

  for (const r of grounding.searchResults || []) pushUrl(r.url);
  for (const c of grounding.citations || []) pushUrl(c.url);

  const urlInText = textBody.match(/https?:\/\/[^\s\)\]\>\"']+/gi) || [];
  for (const u of urlInText) {
    pushUrl(u.replace(/[.,;:]+$/, ""));
  }

  const emailMatch = textBody.match(
    /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i,
  );
  const phoneMatch = textBody.match(
    /(?:\+?\s*52[\s.-]*)?(?:\(?\d{2,3}\)?[\s.-]*)?\d{3,4}[\s.-]*\d{4}/,
  );
  const extMatch = textBody.match(
    /(?:ext(?:ensi[oó]n)?\.?|anexo)\s*[:=]?\s*([\d][\d\s/]*)/i,
  );
  const linkedInMatch = textBody.match(
    /https?:\/\/(?:[\w.-]+\.)?linkedin\.com\/in\/[A-Za-z0-9\-_%]+\/?/i,
  );

  const extensions = [];
  if (extMatch?.[1]) {
    for (const d of String(extMatch[1]).split(/[^\d]+/)) {
      if (d.length >= 3 && d.length <= 6) extensions.push(d);
    }
  }

  return {
    candidateEmail: emailMatch ? emailMatch[0].toLowerCase() : null,
    candidatePhone: phoneMatch ? text(phoneMatch[0]) : null,
    candidateExtensions: extensions,
    candidateLinkedInUrl: linkedInMatch ? linkedInMatch[0] : null,
    sourceUrls: urls,
    webSearchQueries: grounding.searchMetadata?.webSearchQueries || [],
  };
}

/**
 * Resolve Google grounding wrapper / redirect URLs to final official destinations.
 * Keeps SSRF-safe hop validation via fetchPublicHtmlSafe.
 */
export async function resolveGroundingRedirectUrls(urls = [], opts = {}) {
  const out = [];
  const seen = new Set();
  const redirectMap = [];
  for (const raw of urls || []) {
    const u = text(raw);
    if (!u) continue;
    let finalUrl = u;
    if (isGoogleGroundingWrapperUrl(u) || opts.resolveAll === true) {
      try {
        const fetched = await fetchPublicHtmlSafe(u, {
          timeoutMs: Number(opts.timeoutMs || 12000),
          maxBytes: Number(opts.maxBytes || 64_000),
          maxRedirects: Number(opts.maxRedirects || 5),
          fetchImpl: opts.fetchImpl,
          skipDns: opts.skipDns === true,
          allowInsecureTlsRetry: opts.allowInsecureTlsRetry !== false,
          headers: { Accept: "text/html,application/xhtml+xml,*/*" },
        });
        if (fetched?.ok && fetched.url) {
          finalUrl = text(fetched.url);
          if (finalUrl !== u) {
            redirectMap.push({ from: u, to: finalUrl, ok: true });
          }
        } else if (fetched?.reason === "too_many_redirects" && fetched.url) {
          finalUrl = text(fetched.url);
          redirectMap.push({
            from: u,
            to: finalUrl,
            ok: false,
            reason: fetched.reason,
          });
        } else {
          redirectMap.push({
            from: u,
            to: null,
            ok: false,
            reason: fetched?.reason || "resolve_failed",
          });
        }
      } catch (err) {
        redirectMap.push({
          from: u,
          to: null,
          ok: false,
          reason: String(err?.message || err).slice(0, 120),
        });
      }
    }
    // Prefer non-wrapper finals; keep original if resolve failed
    const push = isGoogleGroundingWrapperUrl(finalUrl) ? u : finalUrl;
    const key = push.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(push);
  }
  return { urls: out, redirectMap };
}

/**
 * Confirm a Google AI candidate email/phone appears on fetched HTML near the person.
 * AI text alone is never authority — page association is required.
 */
export function verifyGoogleCandidateOnHtml(html, personName, candidates = {}) {
  const body = String(html || "");
  if (!body) {
    return { email: null, phone: null, phoneExtensions: [], reasons: ["empty_html"] };
  }
  const name = text(personName);
  const nameKey = stripAccents(name).toLowerCase();
  const htmlNorm = body
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const personPresent =
    !nameKey ||
    nameKey
      .split(/\s+/)
      .filter((p) => p.length > 2)
      .slice(0, 2)
      .every((p) => htmlNorm.includes(p));

  const out = {
    email: null,
    phone: null,
    phoneExtensions: [],
    reasons: [],
    personPresent,
  };
  if (!personPresent) {
    out.reasons.push("PERSON_MATCH_FAILED");
    return out;
  }

  const candEmail = text(candidates.email).toLowerCase();
  if (candEmail && htmlNorm.includes(candEmail.toLowerCase())) {
    out.email = candEmail;
    out.reasons.push("candidate_email_found_on_source");
  }

  const candPhone = text(candidates.phone);
  const candDigits = candPhone.replace(/\D/g, "");
  if (candDigits.length >= 10) {
    const htmlDigits = body.replace(/\D/g, "");
    if (
      htmlDigits.includes(candDigits) ||
      htmlDigits.includes(candDigits.slice(-10))
    ) {
      out.phone = candPhone;
      out.reasons.push("candidate_phone_found_on_source");
    }
  }

  for (const ext of candidates.phoneExtensions || []) {
    const e = String(ext || "");
    if (e.length >= 3 && new RegExp(`\\b${e}\\b`).test(body)) {
      out.phoneExtensions.push(e);
    }
  }
  if (out.phoneExtensions.length) {
    out.reasons.push("candidate_extensions_found_on_source");
  }
  return out;
}

async function executeGroundedPrompt(prompt, args, apiKey) {
  return runGeminiVisibilityPrompt({
    prompt,
    model:
      args.model ||
      args.env?.AI_VISIBILITY_GEMINI_MODEL ||
      args.env?.GDI_GOOGLE_CONTACT_GEMINI_MODEL ||
      process.env.AI_VISIBILITY_GEMINI_MODEL ||
      process.env.GDI_GOOGLE_CONTACT_GEMINI_MODEL,
    enableWebSearch: true,
    apiKey,
    fetchImpl: args.fetchImpl,
    timeoutMs: Number(args.timeoutMs || 45000),
  });
}

function packGroundedResult(result, person, organization, creds, extras = {}) {
  const grounding = {
    citations: result.citations || [],
    searchResults: result.searchResults || [],
    searchMetadata: result.searchMetadata || {},
  };
  const parsed = parseGoogleGroundedContactCandidates(result.text || "", grounding);
  const linkedIn = classifyLinkedInProfileMatch({
    url: parsed.candidateLinkedInUrl,
    personName: person.name || person.fullName,
    organization: organization.name || organization.organizationName,
    title: person.title || person.role,
    evidenceText: result.text || "",
  });
  const usage = result.usage || extractGeminiUsage(result.raw || {});
  const costEst = estimateGeminiCost(usage, { model: result.model });
  const costUsd =
    typeof costEst?.amountUsd === "number" ? costEst.amountUsd : 0;

  return {
    ok: Boolean(
      parsed.sourceUrls.length ||
        parsed.candidateEmail ||
        parsed.candidatePhone ||
        linkedIn.url,
    ),
    reason: extras.reason || "GOOGLE_GROUNDED_CANDIDATES",
    provider: "gemini",
    model: result.model,
    groundingUsed: Boolean(
      result.providerMeta?.webSearchUsed || grounding.searchResults?.length,
    ),
    sourceUrls: parsed.sourceUrls,
    candidates: {
      email: parsed.candidateEmail,
      phone: parsed.candidatePhone,
      phoneExtensions: parsed.candidateExtensions,
      linkedInUrl: linkedIn.url,
      liProfileMatchStatus: linkedIn.status,
      liProfileMatchReasons: linkedIn.reasons,
    },
    linkedIn,
    webSearchQueries: parsed.webSearchQueries,
    usage,
    costUsd: Number.isFinite(costUsd) ? costUsd : 0,
    latencyMs: result.latencyMs || null,
    textPreview: text(result.text).slice(0, 500),
    credentials: creds,
    persistenceAuthority: "NONE_UNTIL_SOURCE_VERIFIED",
    ...extras,
  };
}

/**
 * Google-grounded discovery with optional progressive follow-up + redirect resolve.
 * Returns candidate URLs + lead fields; callers must verify via page fetch.
 */
export async function runGoogleGroundedContactDiscovery(args = {}) {
  const env = args.env || process.env;
  const creds = inspectGoogleGroundedContactCredentials(env);
  const person = args.person || {};
  const organization = args.organization || {};
  const name = text(person.name || person.fullName);

  if (!name) {
    return {
      ok: false,
      reason: "NO_NAMED_PERSON",
      skipped: true,
      sourceUrls: [],
      costUsd: 0,
    };
  }
  if (!creds.geminiConfigured && !args.apiKey) {
    return {
      ok: false,
      reason: "GEMINI_NOT_CONFIGURED",
      skipped: true,
      sourceUrls: [],
      costUsd: 0,
      credentials: creds,
    };
  }
  if (args.authorizePaidSearch !== true && args.force !== true) {
    return {
      ok: false,
      reason: "GOOGLE_SEARCH_NOT_AUTHORIZED",
      skipped: true,
      sourceUrls: [],
      costUsd: 0,
      credentials: creds,
    };
  }

  const apiKey =
    text(args.apiKey) ||
    text(env.GEMINI_API_KEY) ||
    text(env.FDD_GEMINI_API_KEY);

  let result;
  try {
    result = await executeGroundedPrompt(
      buildGoogleGroundedContactPrompt(person, organization),
      { ...args, env },
      apiKey,
    );
  } catch (err) {
    return {
      ok: false,
      reason: "GEMINI_CALL_FAILED",
      error: String(err?.message || err).slice(0, 240),
      sourceUrls: [],
      costUsd: 0,
      credentials: creds,
      provider: "gemini",
    };
  }

  let packed = packGroundedResult(result, person, organization, creds, {
    geminiCalls: 1,
  });
  let totalCost = Number(packed.costUsd || 0);
  const allQueries = [...(packed.webSearchQueries || [])];

  const incomplete =
    !packed.candidates?.email ||
    !packed.candidates?.phone ||
    !(packed.sourceUrls || []).some((u) => !isGoogleGroundingWrapperUrl(u));

  if (incomplete && args.allowFollowUp !== false) {
    try {
      const follow = await executeGroundedPrompt(
        buildGoogleGroundedFollowUpPrompt(person, organization, packed),
        { ...args, env },
        apiKey,
      );
      const packed2 = packGroundedResult(follow, person, organization, creds, {
        reason: "GOOGLE_GROUNDED_FOLLOWUP",
        geminiCalls: 1,
      });
      totalCost += Number(packed2.costUsd || 0);
      allQueries.push(...(packed2.webSearchQueries || []));
      // Merge: prefer filled candidates; union URLs
      const urlSet = new Set([
        ...(packed.sourceUrls || []),
        ...(packed2.sourceUrls || []),
      ]);
      packed = {
        ...packed,
        ok: packed.ok || packed2.ok,
        reason: packed2.ok ? "GOOGLE_GROUNDED_WITH_FOLLOWUP" : packed.reason,
        sourceUrls: [...urlSet],
        candidates: {
          email: packed.candidates.email || packed2.candidates.email,
          phone: packed.candidates.phone || packed2.candidates.phone,
          phoneExtensions:
            packed.candidates.phoneExtensions?.length
              ? packed.candidates.phoneExtensions
              : packed2.candidates.phoneExtensions,
          linkedInUrl:
            packed.candidates.linkedInUrl || packed2.candidates.linkedInUrl,
          liProfileMatchStatus:
            packed.linkedIn?.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE
              ? packed.linkedIn.status
              : packed2.candidates.liProfileMatchStatus,
          liProfileMatchReasons:
            packed.linkedIn?.reasons || packed2.candidates.liProfileMatchReasons,
        },
        linkedIn:
          packed.linkedIn?.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE
            ? packed.linkedIn
            : packed2.linkedIn || packed.linkedIn,
        textPreview: packed2.textPreview || packed.textPreview,
        geminiCalls: 2,
        followUpUsed: true,
      };
    } catch {
      packed.followUpUsed = false;
      packed.followUpError = true;
    }
  }

  // Resolve grounding wrappers → official destinations before verification.
  let redirectMeta = { urls: packed.sourceUrls || [], redirectMap: [] };
  if (args.resolveRedirects !== false && (packed.sourceUrls || []).length) {
    redirectMeta = await resolveGroundingRedirectUrls(packed.sourceUrls, {
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns === true,
      timeoutMs: args.redirectTimeoutMs || 12000,
      allowInsecureTlsRetry: args.allowInsecureTlsRetry !== false,
    });
  }

  return {
    ...packed,
    sourceUrls: redirectMeta.urls,
    unresolvedWrapperUrls: (packed.sourceUrls || []).filter((u) =>
      isGoogleGroundingWrapperUrl(u),
    ),
    redirectMap: redirectMeta.redirectMap,
    webSearchQueries: allQueries,
    costUsd: totalCost,
    persistenceAuthority: "NONE_UNTIL_SOURCE_VERIFIED",
  };
}

export default {
  GDI_GOOGLE_GROUNDED_CONTACT_DISCOVERY_V1,
  LINKEDIN_PROFILE_MATCH,
  inspectGoogleGroundedContactCredentials,
  classifyLinkedInProfileMatch,
  buildGoogleGroundedContactPrompt,
  buildGoogleGroundedFollowUpPrompt,
  parseGoogleGroundedContactCandidates,
  isGoogleGroundingWrapperUrl,
  resolveGroundingRedirectUrls,
  verifyGoogleCandidateOnHtml,
  runGoogleGroundedContactDiscovery,
};
