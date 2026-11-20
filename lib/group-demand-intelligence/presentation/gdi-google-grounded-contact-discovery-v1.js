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

  return {
    promptId: "gdi_google_grounded_contact_v1",
    text: [
      "You are assisting hotel sales contact research.",
      "Find publicly documented business contact details for this named person.",
      "Prefer official organizational directories, staff pages, and institutional contact pages.",
      "Return concise factual findings with original source URLs when possible.",
      "",
      `Full name: ${name}`,
      title ? `Job title: ${title}` : null,
      org ? `Organization: ${org}` : null,
      domain ? `Known organization domain: ${domain}` : null,
      "",
      "Locate when publicly available:",
      "- Business email address for this named person",
      "- Business / office telephone number",
      "- Telephone extension(s) (Ext., Extensión, Anexo)",
      "- Public LinkedIn profile URL for this exact person at this organization",
      "- Current role and employer confirmation",
      "- Official source / directory URLs",
      "",
      "Use Spanish and English search terms as appropriate for Mexico / LATAM.",
      "Do not invent emails, phones, extensions, or LinkedIn URLs.",
      "If uncertain, say so and still list candidate source URLs.",
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
 * One Google-grounded discovery call for a named person.
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

  const prompt = buildGoogleGroundedContactPrompt(person, organization);
  const apiKey =
    text(args.apiKey) ||
    text(env.GEMINI_API_KEY) ||
    text(env.FDD_GEMINI_API_KEY);

  let result;
  try {
    result = await runGeminiVisibilityPrompt({
      prompt,
      model: args.model || env.AI_VISIBILITY_GEMINI_MODEL || env.GDI_GOOGLE_CONTACT_GEMINI_MODEL,
      enableWebSearch: true,
      apiKey,
      fetchImpl: args.fetchImpl,
      timeoutMs: Number(args.timeoutMs || 45000),
    });
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

  const grounding = {
    citations: result.citations || [],
    searchResults: result.searchResults || [],
    searchMetadata: result.searchMetadata || {},
  };

  const parsed = parseGoogleGroundedContactCandidates(result.text || "", grounding);

  const linkedIn = classifyLinkedInProfileMatch({
    url: parsed.candidateLinkedInUrl,
    personName: name,
    organization: organization.name || organization.organizationName,
    title: person.title || person.role,
    evidenceText: result.text || "",
  });

  const usage = result.usage || extractGeminiUsage(result.raw || {});
  const costEst = estimateGeminiCost(usage, { model: result.model });
  const costUsd =
    typeof costEst?.amountUsd === "number" ? costEst.amountUsd : 0;

  return {
    ok: Boolean(parsed.sourceUrls.length || parsed.candidateEmail || parsed.candidatePhone),
    reason: "GOOGLE_GROUNDED_CANDIDATES",
    provider: "gemini",
    model: result.model,
    groundingUsed: Boolean(result.providerMeta?.webSearchUsed || grounding.searchResults?.length),
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
    // Explicit: AI output is not persistence authority
    persistenceAuthority: "NONE_UNTIL_SOURCE_VERIFIED",
  };
}

export default {
  GDI_GOOGLE_GROUNDED_CONTACT_DISCOVERY_V1,
  LINKEDIN_PROFILE_MATCH,
  inspectGoogleGroundedContactCredentials,
  classifyLinkedInProfileMatch,
  buildGoogleGroundedContactPrompt,
  parseGoogleGroundedContactCandidates,
  runGoogleGroundedContactDiscovery,
};
