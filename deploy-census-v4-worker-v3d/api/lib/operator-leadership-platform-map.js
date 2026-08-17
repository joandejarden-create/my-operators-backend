/**
 * Operator Setup — Leadership Platform child table (Explorer org/depth/languages/cadence/markets/owner model).
 * One linked child table per operator; `section` discriminates row shape.
 *
 * Table: Operator Setup - Leadership Platform (override via AIRTABLE_OPERATOR_SETUP_LEADERSHIP_PLATFORM_TABLE)
 */

export const LEADERSHIP_PLATFORM_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_LEADERSHIP_PLATFORM_TABLE ||
  "Operator Setup - Leadership Platform";

/** Airtable single-select values — must match base options exactly. */
export const LEADERSHIP_PLATFORM_SECTIONS = {
  orgStructure: "Organization Structure",
  teamDepth: "Team Depth",
  language: "Language",
  governanceCadence: "Governance Cadence",
  teamMarket: "Team Market",
  ownerRelationship: "Owner Relationship",
};

export const TEAM_DEPTH_OPTIONS = [
  "Strong",
  "Very Strong",
  "Moderate / Strong",
  "Emerging / Strong",
];

export const MAP_LEADERSHIP_PLATFORM = {
  section: "section",
  displayOrder: "display_order",
  title: "title",
  subtitle: "subtitle",
  body: "body",
  extra: "extra",
  depth: "depth",
};

const SECTION_BY_API_KEY = Object.fromEntries(
  Object.entries(LEADERSHIP_PLATFORM_SECTIONS).map(([k, v]) => [k, v])
);

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

const PROPER_SMALL_WORDS = new Set([
  "a",
  "an",
  "the",
  "and",
  "or",
  "of",
  "del",
  "de",
  "la",
  "el",
  "y",
]);

const PROPER_ACRONYMS = new Set([
  "cala",
  "he",
  "it",
  "ota",
  "hr",
  "kpi",
  "latam",
  "ssc",
  "fb",
  "f&b",
  "pms",
  "rms",
  "crs",
  "bi",
  "qa",
  "pip",
  "mbr",
  "loi",
  "ap",
  "ar",
]);

function properCaseWord(word, { isFirst = false } = {}) {
  const w = nz(word);
  if (!w) return "";
  const lower = w.toLowerCase();
  if (lower === "u.s." || lower === "us") return lower === "us" ? "U.S." : "U.S.";
  if (PROPER_ACRONYMS.has(lower)) {
    if (lower === "f&b" || lower === "fb") return "F&B";
    return w.toUpperCase();
  }
  if (!isFirst && PROPER_SMALL_WORDS.has(lower)) return lower;
  if (w.includes("-")) {
    return w
      .split("-")
      .map((part, i) => properCaseWord(part, { isFirst: isFirst && i === 0 }))
      .join("-");
  }
  return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
}

function properCasePhrase(text) {
  const s = nz(text);
  if (!s) return "";
  if (/\s*&\s*/.test(s)) {
    return s
      .split(/\s*&\s*/)
      .map((part) => properCasePhrase(part.trim()))
      .join(" & ");
  }
  if (/\s*\/\s*/.test(s)) {
    return s
      .split(/\s*\/\s*/)
      .map((part) => properCasePhrase(part.trim()))
      .join(" / ");
  }
  const words = s.split(/\s+/);
  return words.map((word, i) => properCaseWord(word, { isFirst: i === 0 })).join(" ");
}

/** Airtable `subtitle` — middle-dot segments and slash phrases. */
export function properCaseSubtitle(raw) {
  const s = nz(raw);
  if (!s) return "";
  if (s.includes("·")) {
    return s
      .split("·")
      .map((part) => properCasePhrase(part.trim()))
      .join(" · ");
  }
  return properCasePhrase(s);
}

/** Airtable `extra` — comma-separated tags or leader names. */
export function properCaseExtra(raw) {
  const s = nz(raw);
  if (!s) return "";
  if (Array.isArray(raw)) {
    return raw
      .map((item) => properCasePhrase(nz(item)))
      .filter(Boolean)
      .join(", ");
  }
  return s
    .split(/[,;\n|]+/)
    .map((part) => properCasePhrase(part.trim()))
    .filter(Boolean)
    .join(", ");
}

function properCaseBodyPart(text) {
  const s = nz(text);
  if (!s) return "";
  if (/[.!?]/.test(s)) {
    return s
      .split(/(?<=[.!?])\s+/)
      .map((chunk) => properCasePhrase(chunk.trim()))
      .filter(Boolean)
      .join(" ");
  }
  if (/,/.test(s)) {
    return s
      .split(/,\s*/)
      .map((part) => properCasePhrase(part.trim()))
      .filter(Boolean)
      .join(", ");
  }
  return properCasePhrase(s);
}

/** Airtable `body` — descriptions, relevance, support, experience (prose or comma lists). */
export function properCaseBody(raw) {
  const s = nz(raw);
  if (!s) return "";
  if (s.includes("—")) {
    return s
      .split("—")
      .map((part) => properCaseBodyPart(part.trim()))
      .join("—");
  }
  return properCaseBodyPart(s);
}

function parseTagsCsv(raw) {
  const t = nz(raw);
  if (!t) return [];
  if (t.charAt(0) === "[") {
    try {
      const p = JSON.parse(t);
      return Array.isArray(p) ? p.map((x) => properCasePhrase(nz(x))).filter(Boolean) : [];
    } catch {
      return [];
    }
  }
  return t
    .split(/[,;\n|]+/)
    .map((x) => properCasePhrase(nz(x)))
    .filter(Boolean);
}

function tagsToStorage(tags) {
  if (!tags) return "";
  if (Array.isArray(tags)) return properCaseExtra(tags);
  return properCaseExtra(nz(tags));
}

function sectionFromRowFields(f) {
  const raw = nz(f[MAP_LEADERSHIP_PLATFORM.section]);
  const entry = Object.entries(LEADERSHIP_PLATFORM_SECTIONS).find(([, label]) => label === raw);
  return entry ? entry[0] : "";
}

/** Airtable child row → Explorer / prefill shape for one section. */
export function mapLeadershipPlatformRowFromAirtable(record) {
  const f = (record && record.fields) || record || {};
  const sectionKey = sectionFromRowFields(f);
  const title = nz(f[MAP_LEADERSHIP_PLATFORM.title]);
  const subtitle = properCaseSubtitle(f[MAP_LEADERSHIP_PLATFORM.subtitle]);
  const body = properCaseBody(f[MAP_LEADERSHIP_PLATFORM.body]);
  const extra = properCaseExtra(f[MAP_LEADERSHIP_PLATFORM.extra]);
  const depth = nz(f[MAP_LEADERSHIP_PLATFORM.depth]);
  const displayOrder = Number(f[MAP_LEADERSHIP_PLATFORM.displayOrder]) || 0;

  if (sectionKey === "orgStructure") {
    return {
      sectionKey,
      displayOrder,
      explorer: {
        title,
        description: body,
        tags: parseTagsCsv(extra),
      },
    };
  }
  if (sectionKey === "teamDepth") {
    return {
      sectionKey,
      displayOrder,
      explorer: {
        function: title,
        leadRole: subtitle,
        depth: depth || "Strong",
        relevance: body,
      },
    };
  }
  if (sectionKey === "language") {
    return {
      sectionKey,
      displayOrder,
      explorer: {
        language: title,
        proficiency: subtitle,
        support: body,
      },
    };
  }
  if (sectionKey === "governanceCadence") {
    return {
      sectionKey,
      displayOrder,
      explorer: { title, description: body },
    };
  }
  if (sectionKey === "teamMarket") {
    return {
      sectionKey,
      displayOrder,
      explorer: {
        market: title,
        experience: body,
        leaders: extra,
      },
    };
  }
  if (sectionKey === "ownerRelationship") {
    return {
      sectionKey,
      displayOrder,
      explorer: {
        title,
        value: subtitle,
        description: body,
      },
    };
  }
  return null;
}

/** Form/API row → Airtable child fields. */
export function mapLeadershipPlatformRowToAirtable(row, displayOrder) {
  const sectionKey = nz(row.sectionKey || row.section);
  const sectionLabel = SECTION_BY_API_KEY[sectionKey];
  if (!sectionLabel) return null;

  const out = {
    [MAP_LEADERSHIP_PLATFORM.section]: sectionLabel,
    [MAP_LEADERSHIP_PLATFORM.displayOrder]: displayOrder,
  };

  if (sectionKey === "orgStructure") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.title);
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.description);
    out[MAP_LEADERSHIP_PLATFORM.extra] = tagsToStorage(row.tags);
    return out;
  }
  if (sectionKey === "teamDepth") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.function);
    out[MAP_LEADERSHIP_PLATFORM.subtitle] = properCaseSubtitle(row.leadRole);
    out[MAP_LEADERSHIP_PLATFORM.depth] = TEAM_DEPTH_OPTIONS.includes(nz(row.depth))
      ? nz(row.depth)
      : "Strong";
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.relevance);
    return out;
  }
  if (sectionKey === "language") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.language);
    out[MAP_LEADERSHIP_PLATFORM.subtitle] = properCaseSubtitle(row.proficiency);
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.support);
    return out;
  }
  if (sectionKey === "governanceCadence") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.title);
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.description);
    return out;
  }
  if (sectionKey === "teamMarket") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.market);
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.experience);
    out[MAP_LEADERSHIP_PLATFORM.extra] = properCaseExtra(row.leaders);
    return out;
  }
  if (sectionKey === "ownerRelationship") {
    out[MAP_LEADERSHIP_PLATFORM.title] = nz(row.title);
    out[MAP_LEADERSHIP_PLATFORM.subtitle] = properCaseSubtitle(row.value);
    out[MAP_LEADERSHIP_PLATFORM.body] = properCaseBody(row.description);
    return out;
  }
  return null;
}

/**
 * @param {import("airtable").Records<any>} rows
 * @returns {{ orgStructure: object[], teamDepth: object[], languages: object[], governanceCadence: object[], teamMarkets: object[], ownerRelationship: object[] }}
 */
export function mapLeadershipPlatformRowsForDetail(rows) {
  const buckets = {
    orgStructure: [],
    teamDepth: [],
    languages: [],
    governanceCadence: [],
    teamMarkets: [],
    ownerRelationship: [],
  };
  const bucketKey = {
    orgStructure: "orgStructure",
    teamDepth: "teamDepth",
    language: "languages",
    governanceCadence: "governanceCadence",
    teamMarket: "teamMarkets",
    ownerRelationship: "ownerRelationship",
  };

  (rows || [])
    .map(mapLeadershipPlatformRowFromAirtable)
    .filter(Boolean)
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .forEach((mapped) => {
      const key = bucketKey[mapped.sectionKey];
      if (key && mapped.explorer) buckets[key].push(mapped.explorer);
    });

  return buckets;
}

/**
 * @param {object} body - intake body with leadershipPlatform object
 */
export function buildLeadershipPlatformAirtableRows(body) {
  const lp = body && body.leadershipPlatform;
  if (!lp || typeof lp !== "object") return [];

  const sections = [
    ["orgStructure", lp.orgStructure],
    ["teamDepth", lp.teamDepth],
    ["language", lp.languages],
    ["governanceCadence", lp.governanceCadence],
    ["teamMarket", lp.teamMarkets],
    ["ownerRelationship", lp.ownerRelationship],
  ];

  const out = [];
  sections.forEach(([sectionKey, list]) => {
    if (!Array.isArray(list)) return;
    list.forEach((row, idx) => {
      const mapped = mapLeadershipPlatformRowToAirtable(
        { ...row, sectionKey },
        idx + 1
      );
      if (mapped) out.push(mapped);
    });
  });
  return out;
}

/** Mirror child-table arrays onto legacy JSON prefill keys for backward compatibility. */
export function applyLeadershipPlatformToLegacyJsonPrefill(prefill, platform) {
  if (!prefill || !platform) return prefill;
  if (platform.orgStructure && platform.orgStructure.length) {
    prefill.lead_org_structure_json = JSON.stringify(platform.orgStructure);
  }
  if (platform.teamDepth && platform.teamDepth.length) {
    prefill.lead_team_depth_json = JSON.stringify(platform.teamDepth);
  }
  if (platform.languages && platform.languages.length) {
    prefill.lead_language_capability_json = JSON.stringify(platform.languages);
  }
  if (platform.governanceCadence && platform.governanceCadence.length) {
    prefill.lead_governance_cadence_json = JSON.stringify(platform.governanceCadence);
  }
  if (platform.teamMarkets && platform.teamMarkets.length) {
    prefill.lead_team_markets_json = JSON.stringify(platform.teamMarkets);
  }
  if (platform.ownerRelationship && platform.ownerRelationship.length) {
    prefill.lead_owner_relationship_json = JSON.stringify(platform.ownerRelationship);
  }
  prefill.leadershipPlatform = platform;
  return prefill;
}
