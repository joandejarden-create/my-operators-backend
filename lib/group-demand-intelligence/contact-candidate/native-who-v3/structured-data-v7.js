/**
 * Native WHO V7 — dynamic page detection + structured data person extract.
 */

import { isStrictPersonName } from "./person-boundary.js";

/**
 * Detect likely client-rendered / empty HTML pages.
 */
export function detectDynamicPageV7(html = "", text = "") {
  const h = String(html || "");
  const t = String(text || "").trim();
  const reasons = [];
  if (t.length < 400) reasons.push("low_visible_text");
  if (/__NEXT_DATA__|__NUXT__|window\.__INITIAL_STATE__|data-reactroot|ng-app|id="root"|id="app"/i.test(h)) {
    reasons.push("app_shell");
  }
  if (/application\/ld\+json/i.test(h)) reasons.push("json_ld_present");
  if (/<script[^>]+type=["']application\/json["']/i.test(h)) reasons.push("json_script");
  if (/cvent\.com|eventsair|certain\.com|passkey/i.test(h)) reasons.push("event_platform");
  if (/sgcaptcha|cf-challenge|captcha|challenge-platform|Just a moment/i.test(h)) {
    reasons.push("access_challenge");
  }
  return {
    likelyDynamic:
      reasons.includes("low_visible_text") ||
      reasons.includes("app_shell") ||
      reasons.includes("event_platform") ||
      reasons.includes("access_challenge"),
    hasStructuredData:
      reasons.includes("json_ld_present") || reasons.includes("json_script") || /__NEXT_DATA__/i.test(h),
    reasons,
    needsRenderRecovery:
      reasons.includes("access_challenge") ||
      (reasons.includes("low_visible_text") && t.length < 200),
  };
}

function pushPerson(out, name, role, sourceUrl, extra = {}) {
  if (!isStrictPersonName(name)) return;
  out.push({
    name,
    role: role || null,
    email: extra.email || null,
    phone: extra.phone || null,
    sourceUrl,
    sectionHint: extra.sectionHint || "STAFF",
    evidenceQuote: extra.evidenceQuote || role || name,
    fromHtml: true,
    fromStructuredData: Boolean(extra.fromStructuredData),
    fromRenderedMarkdown: Boolean(extra.fromRenderedMarkdown),
    gdiContactRole: extra.gdiContactRole || "UNKNOWN",
    relevance: "OPERATIONAL_CONTACT",
  });
}

/**
 * Extract people from Context.dev / rendered Markdown staff pages.
 * Supports:
 * - role then ### [Name](url)  (Potomac)
 * - ## Name then role line     (NIST Meet the Staff)
 * - Name line near role line   (AFCEA board / event officers)
 */
export function extractPeopleFromMarkdownV7(markdown = "", sourceUrl = "") {
  const md = String(markdown || "");
  const people = [];
  const lines = md
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean);

  const ROLE_LINE =
    /\b(director|vice president|co-?vp|president|manager|coordinator|administrator|officer|treasurer|secretary|chair|lead|program manager|tournament|cups|summit|conference|meetings|housing|registration)\b/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
      .replace(/\[Permalink[^\]]*\]\([^)]+\)/gi, "")
      .replace(/\*+/g, "")
      .trim();

    // ### [Name](url) or ## Name
    const linked = line.match(/^#{1,4}\s*\[([A-Z][^\]|]+?)\]\([^)]+\)/);
    const headed = line.match(/^#{1,4}\s+([A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3})\s*$/);
    const bareName =
      !linked &&
      !headed &&
      /^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3}$/.test(line)
        ? line
        : null;
    const name = linked?.[1]?.trim() || headed?.[1]?.trim() || bareName;
    if (!name || !isStrictPersonName(name)) continue;

    // Prefer role on next line (NIST: ## Name \n Director of …)
    let role = null;
    const next = (lines[i + 1] || "").replace(/\*+/g, "").trim();
    const prev = (lines[i - 1] || "").replace(/\*+/g, "").trim();
    if (ROLE_LINE.test(next) && next.length <= 140 && !/^#{1,4}\s/.test(next) && !/\*\*/.test(lines[i + 1] || "")) {
      role = next;
    } else if (ROLE_LINE.test(prev) && prev.length <= 140 && !/^#{1,4}\s/.test(prev) && !/\*\*/.test(lines[i - 1] || "")) {
      role = prev;
    } else {
      for (let j = i - 1; j >= Math.max(0, i - 3); j--) {
        const p = lines[j].replace(/\*+/g, "").trim();
        if (/^#{1,4}\s/.test(p) || /^\[/.test(p) || /view bio|permalink|partner/i.test(p)) continue;
        if (ROLE_LINE.test(p) && p.length >= 4 && p.length <= 140) {
          role = p;
          break;
        }
      }
    }

    pushPerson(people, name, role, sourceUrl, {
      evidenceQuote: `${role || ""} ${name}`.trim().slice(0, 160),
      sectionHint: /board|director/i.test(sourceUrl) && /vice president|summit|tournament|director of/i.test(role || "")
        ? "EVENT_TEAM"
        : "STAFF",
      fromRenderedMarkdown: true,
      gdiContactRole: /tournament|cups|conference|meetings|housing|registration|summit/i.test(role || "")
        ? "CONFERENCE_DIRECTOR"
        : /executive director|director of/i.test(role || "")
          ? "EVENT_OWNER"
          : /vice president|co-?vp/i.test(role || "")
            ? "EVENT_OWNER"
            : "UNKNOWN",
    });
  }

  const mailRe = /\[([A-Z][^\]|]+?)\]\(mailto:([^)]+)\)/g;
  let mm;
  while ((mm = mailRe.exec(md))) {
    const name = mm[1].trim();
    if (!isStrictPersonName(name)) continue;
    pushPerson(people, name, null, sourceUrl, {
      email: mm[2],
      fromRenderedMarkdown: true,
      evidenceQuote: `${name} ${mm[2]}`,
    });
  }

  const seen = new Set();
  return people
    .filter((p) => {
      const k = p.name.toLowerCase();
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    })
    .slice(0, 20);
}

/**
 * Extract people from JSON-LD, __NEXT_DATA__, and inline JSON blobs.
 */
export function extractStructuredPeopleV7(html = "", sourceUrl = "") {
  const people = [];
  const h = String(html || "");

  const ldRe = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = ldRe.exec(h))) {
    try {
      walkJsonForPeople(JSON.parse(m[1]), people, sourceUrl);
    } catch {
      /* ignore */
    }
  }

  const next = h.match(/<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i);
  if (next) {
    try {
      walkJsonForPeople(JSON.parse(next[1]), people, sourceUrl);
    } catch {
      /* ignore */
    }
  }

  const appJson = /<script[^>]*type=["']application\/json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let am;
  let count = 0;
  while ((am = appJson.exec(h)) && count < 4) {
    count += 1;
    try {
      const raw = am[1].trim();
      if (raw.length < 20 || raw.length > 200_000) continue;
      walkJsonForPeople(JSON.parse(raw), people, sourceUrl);
    } catch {
      /* ignore */
    }
  }

  const seen = new Set();
  return people
    .filter((p) => {
      const k = p.name.toLowerCase();
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    })
    .slice(0, 20);
}

function walkJsonForPeople(node, out, sourceUrl, depth = 0) {
  if (!node || depth > 8) return;
  if (Array.isArray(node)) {
    for (const item of node.slice(0, 80)) walkJsonForPeople(item, out, sourceUrl, depth + 1);
    return;
  }
  if (typeof node !== "object") return;

  const type = String(node["@type"] || node.type || "");
  const name = node.name || node.fullName || node.displayName || null;
  const role =
    node.jobTitle ||
    node.title ||
    node.role ||
    node.position ||
    (Array.isArray(node.jobTitle) ? node.jobTitle[0] : null);

  if (/Person|Employee|Contact/i.test(type) && name) {
    pushPerson(out, String(name).trim(), role ? String(role) : null, sourceUrl, {
      email: node.email || null,
      phone: node.telephone || node.phone || null,
      evidenceQuote: `${name} ${role || ""}`.slice(0, 160),
      sectionHint: "STAFF",
      fromStructuredData: true,
    });
  }

  if (name && role && isStrictPersonName(String(name).trim())) {
    pushPerson(out, String(name).trim(), String(role).trim(), sourceUrl, {
      email: node.email || null,
      evidenceQuote: `${name}, ${role}`.slice(0, 160),
      fromStructuredData: true,
    });
  }

  for (const v of Object.values(node)) {
    if (v && typeof v === "object") walkJsonForPeople(v, out, sourceUrl, depth + 1);
  }
}
