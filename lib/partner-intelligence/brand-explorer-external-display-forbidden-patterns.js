/**

 * Forbidden owner-facing strings for incomplete Brand Explorer profiles.

 * Shared by v37C-R2 UI proof test and v38 quality lock.

 */



export const FORBIDDEN_EXTERNAL_DISPLAY_STRINGS = Object.freeze([

  "Scenario cards will appear",

  "overview.scenario",

  "Slots materials.gallery",

  "Output Note",

  "internal review",

  "supports internal review",

  "source data",

  "neighborhood focus",

  "boutique design",

  "conversion-friendly.",

  "disclosure document",

  "confirm every line",

  "Source:",

  "Sources:",

  "FDD",

  "LOI",

  "Item 19",

  "Brand-Verified Content",

]);



/** Standalone rough keyword bullet (e.g. brandValueProposition split). */

export const FORBIDDEN_STANDALONE_BULLET_PATTERNS = Object.freeze([

  /<li>\s*IHG\s*<\/li>/i,

  /<li>\s*Accor\s*<\/li>/i,

  /<li>\s*neighborhood focus\s*<\/li>/i,

  /<li>\s*boutique design\s*<\/li>/i,

  /<li>\s*conversion-friendly\.?\s*<\/li>/i,

]);



/**

 * http/https in staging/fallback sections — exclude known safe href contexts (brand website, images).

 */

export const FORBIDDEN_URL_IN_STAGING_PATTERNS = Object.freeze([

  /oe-dd--empty[^<]*https?:\/\//i,

  /be-atelier-placeholder[^<]*https?:\/\//i,

  /scenario-card[^<]*https?:\/\//i,

  /<p[^>]*>\s*https?:\/\//i,

  /<li[^>]*>\s*https?:\/\//i,

]);



/**

 * @param {string} html

 * @param {{ companyValidated?: boolean, expectLocked?: boolean }} [options]

 * @returns {{ forbiddenStringsFound: string[], matches: Array<{ pattern: string, snippet: string }> }}

 */

export function scanRenderedHtmlForForbiddenStrings(html, options = {}) {

  const text = nz(html);

  const forbiddenStringsFound = [];

  const matches = [];



  for (const needle of FORBIDDEN_EXTERNAL_DISPLAY_STRINGS) {

    if (needle === "Brand-Verified Content" && options.companyValidated === true) continue;

    if (text.includes(needle)) {

      forbiddenStringsFound.push(needle);

      matches.push({ pattern: needle, snippet: extractSnippet(text, needle) });

    }

  }



  for (const re of FORBIDDEN_STANDALONE_BULLET_PATTERNS) {

    const hit = text.match(re);

    if (hit) {

      forbiddenStringsFound.push(hit[0]);

      matches.push({ pattern: re.source, snippet: hit[0] });

    }

  }



  for (const re of FORBIDDEN_URL_IN_STAGING_PATTERNS) {

    const hit = text.match(re);

    if (hit) {

      forbiddenStringsFound.push("http/https in staging section");

      matches.push({ pattern: re.source, snippet: hit[0].slice(0, 120) });

    }

  }



  if (options.expectLocked) {

    if (text.includes("—")) {

      forbiddenStringsFound.push("—");

      matches.push({ pattern: "—", snippet: "—" });

    }

    if (/&nbsp;/.test(text)) {

      forbiddenStringsFound.push("&nbsp;");

      matches.push({ pattern: "&nbsp;", snippet: "&nbsp;" });

    }

  }



  return {

    forbiddenStringsFound: [...new Set(forbiddenStringsFound)],

    matches,

  };

}



function nz(v) {

  return v == null ? "" : String(v);

}



function extractSnippet(text, needle) {

  const idx = text.indexOf(needle);

  if (idx < 0) return needle;

  const start = Math.max(0, idx - 40);

  const end = Math.min(text.length, idx + needle.length + 40);

  return text.slice(start, end).replace(/\s+/g, " ");

}


