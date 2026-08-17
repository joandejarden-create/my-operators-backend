/**
 * Deterministic robots.txt parser (Phase 3C.1).
 * No LLM interpretation. Exact user-agent matching + wildcard fallback.
 */

export const ROBOTS_PARSER_VERSION = "ai_visibility_robots_parser_v1";

/**
 * Parse robots.txt content into structured groups.
 * @param {string} content
 */
export function parseRobotsTxt(content) {
  const lines = String(content || "")
    .split(/\r?\n/)
    .map((l) => l.trim());

  const groups = [];
  let current = { agents: [], rules: [], crawlDelay: null, sitemaps: [] };
  const globalSitemaps = [];
  const errors = [];

  for (const line of lines) {
    if (!line || line.startsWith("#")) continue;
    const idx = line.indexOf(":");
    if (idx === -1) {
      errors.push(`malformed_line:${line.slice(0, 40)}`);
      continue;
    }
    const directive = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();

    if (directive === "user-agent") {
      if (current.agents.length && current.rules.length) {
        groups.push(current);
        current = { agents: [], rules: [], crawlDelay: null, sitemaps: [] };
      }
      current.agents.push(value);
    } else if (directive === "allow" || directive === "disallow") {
      current.rules.push({ type: directive, path: value || "" });
    } else if (directive === "crawl-delay") {
      const n = Number(value);
      current.crawlDelay = Number.isFinite(n) ? n : value;
    } else if (directive === "sitemap") {
      globalSitemaps.push(value);
    } else {
      errors.push(`unknown_directive:${directive}`);
    }
  }
  if (current.agents.length) groups.push(current);

  return {
    version: ROBOTS_PARSER_VERSION,
    groups,
    sitemaps: globalSitemaps,
    errors,
    malformed: errors.some((e) => e.startsWith("malformed_line")),
  };
}

function agentMatches(requestAgent, groupAgent) {
  const req = String(requestAgent || "").toLowerCase();
  const ga = String(groupAgent || "").toLowerCase();
  if (ga === "*") return true;
  return req === ga || req.includes(ga);
}

/**
 * Find applicable rule group for user-agent (exact match preferred, then wildcard).
 */
export function findRobotsGroup(parsed, userAgent) {
  const groups = parsed?.groups || [];
  let exact = null;
  let wildcard = null;
  for (const g of groups) {
    for (const agent of g.agents) {
      if (agent === "*") wildcard = g;
      else if (agentMatches(userAgent, agent)) exact = g;
    }
  }
  return exact || wildcard || null;
}

/**
 * Check if path is allowed for user-agent.
 * Longest matching rule wins (Allow can override Disallow per robots convention).
 */
export function isPathAllowed(parsed, userAgent, requestPath) {
  const group = findRobotsGroup(parsed, userAgent);
  if (!group) return { allowed: true, reason: "no_matching_group_default_allow" };

  const path = normalizeRobotsPath(requestPath);
  let bestMatch = null;
  let bestLen = -1;

  for (const rule of group.rules) {
    const rulePath = rule.path || "";
    if (rulePath === "") {
      if (rule.type === "disallow") {
        if (bestLen < 0) bestMatch = { allowed: true, rule };
      }
      continue;
    }
    if (path.startsWith(rulePath) && rulePath.length > bestLen) {
      bestLen = rulePath.length;
      bestMatch = {
        allowed: rule.type === "allow",
        rule,
      };
    }
  }

  if (!bestMatch) return { allowed: true, reason: "no_rule_match_default_allow" };
  return {
    allowed: bestMatch.allowed,
    matchedRule: bestMatch.rule,
    group: group.agents,
  };
}

function normalizeRobotsPath(p) {
  let path = String(p || "/");
  if (!path.startsWith("/")) path = `/${path}`;
  return path;
}

/**
 * Evaluate OAI-SearchBot robots access for a domain path.
 */
export function evaluateOaiSearchBotAccess(parsed, requestPath = "/") {
  const result = isPathAllowed(parsed, "OAI-SearchBot", requestPath);
  const group = findRobotsGroup(parsed, "OAI-SearchBot");
  const hasExplicit = Boolean(group?.rules?.length);

  let status = "no_explicit_directive";
  if (hasExplicit) {
    status = result.allowed ? "robots_access_allowed" : "robots_access_blocked";
  }

  return {
    userAgent: "OAI-SearchBot",
    status,
    allowed: result.allowed,
    hasExplicitDirective: hasExplicit,
    matchedRule: result.matchedRule || null,
    RULE: "robots permission != actual crawl",
  };
}

/**
 * Extract sitemap URLs from parsed robots.
 */
export function extractSitemapsFromRobots(parsed) {
  const fromGroups = (parsed?.groups || []).flatMap((g) => g.sitemaps || []);
  return [...new Set([...(parsed?.sitemaps || []), ...fromGroups])];
}
