/**
 * Permanent rule: Brand Explorer property-facing images must belong to the
 * brand on screen — never a sibling flag under the same parent company.
 *
 * Example fail: Hilton Hotels & Resorts photography (Hilton Panama exterior
 * with Hilton signage) used on Waldorf Astoria Hotels & Resorts.
 *
 * Contract: BE_PERMANENT_SAME_BRAND_IMAGE_AFFINITY_V1
 */

export const BE_PERMANENT_SAME_BRAND_IMAGE_AFFINITY_V1 =
  "BE_PERMANENT_SAME_BRAND_IMAGE_AFFINITY_V1";

export const SAME_BRAND_IMAGE_FAIL = Object.freeze({
  SIBLING_BRAND: "sibling_or_foreign_brand_image",
  PARENT_FLAGSHIP_SUBSTITUTION: "parent_flagship_substituted_for_sub_brand",
  MISSING_BRAND_CONTEXT: "missing_brand_context_for_affinity_check",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function haystack(url, meta = {}) {
  return [
    nz(url),
    nz(meta.imageUrl),
    nz(meta.propertyName),
    nz(meta.propertyKey),
    nz(meta.caption),
    nz(meta.title),
    nz(meta.body),
    nz(meta.label),
    nz(meta.source),
    nz(meta.sourcePageUrl),
    nz(meta.role),
  ]
    .join("\n")
    .toLowerCase();
}

/**
 * Brand-token allowlist + sibling rejectors.
 * When brandHints match, keep even if a loose sibling substring appears.
 * When siblingRe matches and brandHints do not, reject.
 */
export const BRAND_IMAGE_AFFINITY_BY_SLUG = Object.freeze({
  "waldorf-astoria": {
    brandHints:
      /waldorf|\/im\/en\/[a-z0-9]{5,10}wa\/|hilton\.com\/en\/hotels\/[a-z0-9]+wa-|waldorf%20astoria|waldorf-astoria/i,
    siblingRe:
      /\bhilton hotels?\b|hilton panama|hilton cancun|hilton bogota|ptyhfhh|cunmchh|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh)\/|doubletree|curio|tapestry|signia|conrad(?!\s*hilton\b)|hampton|homewood|home2|garden-inn|motto|canopy|tempo|lxr|spark|tru-by|embassy|graduate-by-hilton/i,
  },
  "conrad-hotels-resorts": {
    brandHints: /conrad|\/im\/en\/[a-z0-9]{5,10}cn\/|hilton\.com\/en\/hotels\/[a-z0-9]+cn-/i,
    siblingRe:
      /waldorf|signia|lxr|doubletree|curio|tapestry|hampton|homewood|home2|garden-inn|motto|canopy|tempo|spark|tru-by|embassy|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh|wa|dt|hx|gi|hw|ht|ru|pe)\//i,
  },
  "lxr-hotels-resorts": {
    brandHints: /\blxr\b|\/im\/en\/[a-z0-9]{5,10}lx\/|hilton\.com\/en\/hotels\/[a-z0-9]+lx-/i,
    siblingRe:
      /waldorf|conrad|signia|doubletree|curio|tapestry|hampton|homewood|home2|garden-inn|motto|canopy|tempo|spark|tru-by|embassy|hilton hotels|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh|wa|cn|dt)\//i,
  },
  "signia-by-hilton": {
    brandHints: /signia|\/im\/en\/[a-z0-9]{5,10}sa\/|hilton\.com\/en\/hotels\/[a-z0-9]+sa-/i,
    siblingRe:
      /waldorf|conrad|lxr|doubletree|curio|tapestry|hampton|homewood|home2|garden-inn|motto|canopy|tempo|spark|tru-by|embassy|hilton hotels|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh|wa|cn)\//i,
  },
  "hilton-hotels-resorts": {
    brandHints:
      /hilton\.com\/en\/hotels\/[a-z]{3,5}(?:hh|hf|fhh|chh|bchh)-|\/im\/en\/[a-z0-9]{5,8}(?:hh|hf|fhh|chh|bchh)\/|hilton-panama|hilton-cancun|hilton-bogota|hilton hotels and resorts|hilton%20hotels%20and%20resorts/i,
    siblingRe:
      /doubletree|curio|tapestry|signia|conrad|waldorf|hampton|homewood|home2|home-2|tru-by|spark|embassy|garden-inn|motto|canopy|tempo|lxr|graduate-by-hilton/i,
  },
  "doubletree-by-hilton": {
    brandHints: /doubletree|\/im\/en\/[a-z0-9]{5,8}dt\/|hilton\.com\/en\/hotels\/[a-z0-9]+dt-/i,
    siblingRe:
      /curio|tapestry|signia|waldorf|conrad|hampton|homewood|home2|garden-inn|motto|canopy|tempo|lxr|spark|tru-by|embassy|hilton hotels|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh)\//i,
  },
  "hampton-by-hilton": {
    brandHints: /hampton|\/im\/en\/[a-z0-9]{5,8}hx\/|hilton\.com\/en\/hotels\/[a-z0-9]+hx-/i,
    siblingRe:
      /tru-by|spark|garden-inn|home2|homewood|doubletree|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|embassy|hilton hotels|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh|dt|gi|hw|ht|ru|pe)\//i,
  },
  "hilton-garden-inn": {
    brandHints: /garden.?inn|\/im\/en\/[a-z0-9]{5,8}gi\/|hilton\.com\/en\/hotels\/[a-z0-9]+gi-/i,
    siblingRe:
      /hampton|doubletree|homewood|home2|tru-by|spark|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|embassy|hilton hotels|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh|hx|dt|hw|ht|ru|pe)\//i,
  },
  "home2-suites-by-hilton": {
    brandHints: /home2|home-2|\/im\/en\/[a-z0-9]{5,8}ht\/|hilton\.com\/en\/hotels\/[a-z0-9]+ht-/i,
    siblingRe:
      /homewood|hampton|spark|tru-by|embassy|doubletree|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|garden-inn|hilton hotels/i,
  },
  "homewood-suites-by-hilton": {
    brandHints: /homewood|\/im\/en\/[a-z0-9]{5,8}hw\/|hilton\.com\/en\/hotels\/[a-z0-9]+hw-/i,
    siblingRe:
      /home2|home-2|hampton|spark|garden-inn|tru-by|embassy|doubletree|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|hilton hotels/i,
  },
  "spark-by-hilton": {
    brandHints: /spark.?by.?hilton|\/im\/en\/[a-z0-9]{5,8}pe\/|hilton\.com\/en\/hotels\/[a-z0-9]+pe-/i,
    siblingRe:
      /tru-by|hampton|home2|homewood|garden-inn|doubletree|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|embassy|hilton hotels/i,
  },
  "tru-by-hilton": {
    brandHints: /(?<!s)tru.?by.?hilton|\/im\/en\/[a-z0-9]{5,8}ru\/|hilton\.com\/en\/hotels\/[a-z0-9]+ru-/i,
    siblingRe:
      /spark|hampton|home2|homewood|garden-inn|doubletree|curio|tapestry|signia|conrad|waldorf|motto|canopy|tempo|lxr|embassy|hilton hotels/i,
  },
  "canopy-by-hilton": {
    brandHints: /canopy|\/im\/en\/[a-z0-9]{5,8}py\/|hilton\.com\/en\/hotels\/[a-z0-9]+py-/i,
    siblingRe:
      /motto|tempo|curio|tapestry|doubletree|hampton|garden-inn|waldorf|conrad|signia|lxr|hilton hotels/i,
  },
  "motto-by-hilton": {
    brandHints: /motto|\/im\/en\/[a-z0-9]{5,8}wm\/|hilton\.com\/en\/hotels\/[a-z0-9]+wm-/i,
    siblingRe:
      /canopy|tempo|curio|tapestry|doubletree|hampton|garden-inn|waldorf|conrad|signia|lxr|hilton hotels/i,
  },
  "tempo-by-hilton": {
    brandHints: /tempo.?by.?hilton|\/im\/en\/[a-z0-9]{5,8}tp\/|hilton\.com\/en\/hotels\/[a-z0-9]+tp-/i,
    siblingRe:
      /canopy|motto|curio|tapestry|doubletree|hampton|garden-inn|waldorf|conrad|signia|lxr|hilton hotels/i,
  },
  "tapestry-collection-by-hilton": {
    brandHints: /tapestry|\/im\/en\/[a-z0-9]{5,8}up\/|hilton\.com\/en\/hotels\/[a-z0-9]+up-/i,
    siblingRe:
      /curio|doubletree|hampton|garden-inn|motto|canopy|tempo|waldorf|conrad|signia|lxr|hilton hotels/i,
  },
  "curio-collection-by-hilton": {
    brandHints: /curio|\/im\/en\/[a-z0-9]{5,8}qq\/|hilton\.com\/en\/hotels\/[a-z0-9]+qq-/i,
    siblingRe:
      /tapestry|doubletree|hampton|garden-inn|motto|canopy|tempo|waldorf|conrad|signia|lxr|hilton hotels/i,
  },
  // Marriott family — flagship vs soft/select siblings
  westin: {
    brandHints: /\bwestin\b|cache\.marriott\.com.*westin|westin-/i,
    siblingRe:
      /\bsheraton\b|\britz.?carlton\b|\bst\.?\s*regis\b|\bedition\b|\brenaisance\b|\brenaisance\b|jw.?marriott|w hotels|autograph|tribute|moxy|aloft|element|courtyard|fairfield|residence.?inn|springhill|towneplace|four.?points/i,
  },
  sheraton: {
    brandHints: /\bsheraton\b|cache\.marriott\.com.*sheraton|sheraton-/i,
    siblingRe:
      /\bwestin\b|\britz.?carlton\b|\bst\.?\s*regis\b|\bedition\b|jw.?marriott|w hotels|autograph|tribute|moxy|aloft|courtyard|fairfield|residence.?inn|springhill|towneplace/i,
  },
  "hyatt-regency": {
    brandHints: /hyatt.?regency|assets\.hyatt\.com.*regency/i,
    siblingRe:
      /\bpark.?hyatt\b|\bgrand.?hyatt\b|\bandaz\b|\bthompson\b|\bhyatt.?place\b|\bhyatt.?house\b|\balila\b|\bhyatt.?centric\b|\bunbound\b|\bjdv\b/i,
  },
  "thompson-hotels": {
    brandHints: /\bthompson\b/i,
    siblingRe:
      /\bhyatt.?regency\b|\bpark.?hyatt\b|\bgrand.?hyatt\b|\bandaz\b|\bhyatt.?place\b|\bhyatt.?house\b|\balila\b|\bhyatt.?centric\b/i,
  },
  "hyatt-centric": {
    brandHints: /hyatt.?centric/i,
    siblingRe:
      /\bhyatt.?regency\b|\bpark.?hyatt\b|\bgrand.?hyatt\b|\bandaz\b|\bthompson\b|\bhyatt.?place\b|\bhyatt.?house\b|\balila\b/i,
  },
});

/**
 * Generic sibling cues that must never appear for a different brand when the
 * target brand name is present in the slug (covers brands without a dedicated
 * affinity row — still blocks obvious flagship Hilton substitution).
 */
const GENERIC_HILTON_FLAGSHIP_RE =
  /hilton panama|hilton cancun|hilton bogota|ptyhfhh|cunmchh|hilton%20hotels%20and%20resorts|\/im\/en\/[a-z0-9]+(?:hh|hf|fhh|chh|bchh)\//i;

const GENERIC_MARRIOTT_SIBLING_POOL_RE =
  /wave14-westin-gallery|wave14-sheraton-gallery|hilton-hotels-and-resorts-gallery/i;

/**
 * Returns whether an image/caption blob is acceptable for the target brand.
 */
export function assessSameBrandImageAffinity(slug, url, meta = {}) {
  const s = nz(slug).toLowerCase();
  if (!s) {
    return {
      ok: false,
      code: SAME_BRAND_IMAGE_FAIL.MISSING_BRAND_CONTEXT,
      detail: "missing_slug",
    };
  }
  const text = haystack(url, meta);
  if (!text) {
    return { ok: true, code: null, detail: null };
  }

  const row = BRAND_IMAGE_AFFINITY_BY_SLUG[s];
  if (row) {
    const urlOnly = nz(url || meta.imageUrl);
    // Captions are often rewritten to the target brand while the DAM path still
    // encodes a sibling property (Hilton Panama on Waldorf). Prefer URL hints.
    const hintOnUrl = row.brandHints.test(urlOnly);
    const hintOnText = row.brandHints.test(text);
    const siblingOnUrl = row.siblingRe.test(urlOnly);
    const siblingOnText = row.siblingRe.test(text);

    if (siblingOnUrl && !hintOnUrl) {
      return {
        ok: false,
        code: SAME_BRAND_IMAGE_FAIL.SIBLING_BRAND,
        detail: "sibling_token_in_url_without_brand_hint",
      };
    }
    if (siblingOnText && !hintOnText && !hintOnUrl) {
      return {
        ok: false,
        code: SAME_BRAND_IMAGE_FAIL.SIBLING_BRAND,
        detail: "sibling_token_without_brand_hint",
      };
    }
    // Hilton flagship DAM paths are never OK for non-flagship Hilton brands
    // even when captions were rewritten to the target brand name.
    if (
      s !== "hilton-hotels-resorts" &&
      /hilton|conrad|waldorf|lxr|signia|canopy|motto|tempo|tapestry|curio|hampton|doubletree|garden|home2|homewood|spark|tru/.test(
        s
      ) &&
      GENERIC_HILTON_FLAGSHIP_RE.test(urlOnly || text) &&
      !hintOnUrl
    ) {
      return {
        ok: false,
        code: SAME_BRAND_IMAGE_FAIL.PARENT_FLAGSHIP_SUBSTITUTION,
        detail: "hilton_flagship_dam_path",
      };
    }
    return { ok: true, code: null, detail: null };
  }

  // Brands without a dedicated row: still reject obvious Hilton flagship DAM
  // substitution when the slug is clearly not hilton-hotels-resorts.
  if (s !== "hilton-hotels-resorts" && GENERIC_HILTON_FLAGSHIP_RE.test(text)) {
    // Allow if the brand's own distinctive token is also present (soft).
    const slugTokens = s.split("-").filter((t) => t.length > 3 && !/by|and|the|hotel|hotels|resorts|collection/.test(t));
    const hasOwn = slugTokens.some((t) => text.includes(t));
    if (!hasOwn) {
      return {
        ok: false,
        code: SAME_BRAND_IMAGE_FAIL.PARENT_FLAGSHIP_SUBSTITUTION,
        detail: "generic_hilton_flagship_without_brand_token",
      };
    }
  }

  if (GENERIC_MARRIOTT_SIBLING_POOL_RE.test(nz(meta.fixturePath) || nz(meta.poolPath) || "")) {
    const pool = nz(meta.fixturePath || meta.poolPath);
    if (pool && !pool.toLowerCase().includes(s.replace(/-/g, "-"))) {
      // Caller may pass fixture path; reject when path names a different brand.
      const poolSlug = pool.match(/wave\d+[a-z]?-([a-z0-9-]+)-gallery/i)?.[1] || "";
      if (poolSlug && poolSlug !== s && !s.includes(poolSlug) && !poolSlug.includes(s)) {
        return {
          ok: false,
          code: SAME_BRAND_IMAGE_FAIL.SIBLING_BRAND,
          detail: `fixture_pool_mismatch:${poolSlug}`,
        };
      }
    }
  }

  return { ok: true, code: null, detail: null };
}

export function isSameBrandImage(slug, url, meta = {}) {
  return assessSameBrandImageAffinity(slug, url, meta).ok;
}

/**
 * Fixture path is same-brand only when the file name encodes this slug
 * (or an exact known alias). Sibling International Reference pools are rejected.
 */
export function isExactBrandGalleryFixture(slug, fixtureRelPath) {
  const s = nz(slug).toLowerCase();
  const p = nz(fixtureRelPath).toLowerCase().replace(/\\/g, "/");
  if (!s || !p) return false;
  if (p.includes(`-${s}-gallery`) || p.includes(`/${s}-gallery`) || p.endsWith(`${s}-gallery-pool.json`)) {
    return true;
  }
  // Presentation packs: brand-explorer-presentation-{slug}-gallery.json
  if (p.includes(`presentation-${s}`) || p.includes(`presentation-${s.replace(/-hotels$/, "")}`)) {
    return true;
  }
  // Known slug aliases ↔ fixture stems
  const aliases = {
    "hilton-hotels-resorts": "hilton-hotels-and-resorts",
    "conrad-hotels-resorts": "conrad",
    "lxr-hotels-resorts": "lxr",
    "waldorf-astoria": "waldorf-astoria",
    westin: "westin",
    sheraton: "sheraton",
  };
  const alias = aliases[s];
  if (alias && (p.includes(`-${alias}-gallery`) || p.includes(`/${alias}-gallery`))) return true;
  return false;
}
