/**
 * Brand-family universe map + adapter ROI ranking.
 */

import { createHash } from "node:crypto";
import { inferBrandFamily, normName } from "../census-autopilot-v2/identity-dedupe.js";
import { FAMILY_ADAPTER_META, ADAPTER_CLASS } from "./constants.js";

const SOFT_RE =
  /curio|tapestry|autograph|tribute|design hotels|mgallery|unbound|lxr|joia|voco|canopy|tempo |spark by|ascend |cambria|collection by|preferred hotels|leading hotels/i;

const EXTRA_FAMILY_RE = [
  [/barcel[oó]/i, "Barcelo"],
  [/\briu\b/i, "RIU"],
  [/iberostar/i, "Iberostar"],
  [/palladium/i, "Palladium"],
  [/\bh10\b/i, "H10"],
  [/bah[ií]a\s*principe|bahia\s*principe/i, "Bahia Principe"],
  [/amresorts|hyatt\s+ziva|hyatt\s+zilara|secrets\s+|dreams\s+|breathless/i, "AMResorts"],
  [/preferred hotels/i, "Preferred"],
  [/leading hotels of the world|\blhw\b/i, "LHW"],
  [/best western|bwh\s+hotel/i, "Best Western"],
  [/radisson/i, "Radisson"],
];

export function classifyFamilyExtended(name, existingFamily) {
  if (existingFamily && existingFamily !== "Independent" && existingFamily !== "Unknown") {
    if (SOFT_RE.test(String(name || ""))) {
      return { family: existingFamily, class: ADAPTER_CLASS.SOFT_BRAND, soft: true };
    }
    const meta = FAMILY_ADAPTER_META[existingFamily];
    return {
      family: existingFamily,
      class: meta?.class || ADAPTER_CLASS.NO_ADAPTER,
      soft: false,
    };
  }
  const inferred = inferBrandFamily(name);
  if (inferred && inferred !== "Independent") {
    const meta = FAMILY_ADAPTER_META[inferred];
    const soft = SOFT_RE.test(String(name || ""));
    return {
      family: inferred,
      class: soft ? ADAPTER_CLASS.SOFT_BRAND : meta?.class || ADAPTER_CLASS.NO_ADAPTER,
      soft,
    };
  }
  for (const [re, fam] of EXTRA_FAMILY_RE) {
    if (re.test(String(name || ""))) {
      return {
        family: fam,
        class: SOFT_RE.test(String(name || "")) ? ADAPTER_CLASS.SOFT_BRAND : ADAPTER_CLASS.NO_ADAPTER,
        soft: SOFT_RE.test(String(name || "")),
      };
    }
  }
  if (!name || String(name).length < 3) {
    return { family: "Unknown", class: ADAPTER_CLASS.UNKNOWN, soft: false };
  }
  return { family: "Independent", class: ADAPTER_CLASS.INDEPENDENT, soft: false };
}

/**
 * @param {object[]} candidates
 * @param {object[]} vicRecords
 */
export function buildBrandFamilyUniverse(candidates, vicRecords) {
  const byFamily = new Map();
  const vicByFamily = new Map();
  for (const v of vicRecords) {
    const f = v.family || "Independent";
    vicByFamily.set(f, (vicByFamily.get(f) || 0) + 1);
  }

  for (const c of candidates) {
    const name = c.origin_name || c.name;
    const { family, class: cls, soft } = classifyFamilyExtended(name, c.family);
    if (!byFamily.has(family)) {
      byFamily.set(family, {
        family,
        adapter_class: cls,
        soft_brand_share: 0,
        candidate_count: 0,
        existing_verified_count: 0,
        new_candidate_count: 0,
        capabilities: FAMILY_ADAPTER_META[family] || {
          directory: false,
          property_detail: false,
          structured: false,
          property_id: null,
          rooms_native: "UNKNOWN",
          address: false,
          coordinates: false,
          phone: false,
          amenities: false,
          status: false,
        },
      });
    }
    const row = byFamily.get(family);
    row.candidate_count += 1;
    if (soft) row.soft_brand_share += 1;
    const isVic =
      String(c.candidate_origin || "").includes("VERIFIED") ||
      String(c.candidate_id || "").startsWith("cand_vic_");
    if (isVic) row.existing_verified_count += 1;
    else row.new_candidate_count += 1;
  }

  // Merge VIC counts if candidates under-count
  for (const [f, n] of vicByFamily) {
    if (!byFamily.has(f)) {
      byFamily.set(f, {
        family: f,
        adapter_class: FAMILY_ADAPTER_META[f]?.class || ADAPTER_CLASS.NO_ADAPTER,
        soft_brand_share: 0,
        candidate_count: n,
        existing_verified_count: n,
        new_candidate_count: 0,
        capabilities: FAMILY_ADAPTER_META[f] || {},
      });
    } else {
      const row = byFamily.get(f);
      row.existing_verified_count = Math.max(row.existing_verified_count, n);
    }
  }

  const families = [...byFamily.values()].sort((a, b) => b.candidate_count - a.candidate_count);

  const classCounts = {};
  let withOfficialPath = 0;
  for (const f of families) {
    classCounts[f.adapter_class] = (classCounts[f.adapter_class] || 0) + f.candidate_count;
    if (
      f.adapter_class === ADAPTER_CLASS.STRONG_NATIVE ||
      f.adapter_class === ADAPTER_CLASS.PARTIAL ||
      f.adapter_class === ADAPTER_CLASS.SOFT_BRAND
    ) {
      // soft brands still often have parent native path
      if (f.adapter_class !== ADAPTER_CLASS.SOFT_BRAND || FAMILY_ADAPTER_META[f.family]) {
        withOfficialPath += f.candidate_count;
      }
    }
    if (f.adapter_class === ADAPTER_CLASS.NO_ADAPTER && f.capabilities?.directory) {
      withOfficialPath += f.candidate_count;
    }
  }

  const total = families.reduce((s, f) => s + f.candidate_count, 0) || 1;

  return {
    version: "brand-family-universe-v2.2",
    total_candidates: total,
    class_counts: classCounts,
    pct_with_official_native_path: Math.round((100 * withOfficialPath) / total),
    families,
  };
}

/**
 * ROI = hotels_affected × expected_completeness_gain × (1 / research_cost)
 */
export function rankAdapterRoi(universe) {
  const ranked = (universe.families || [])
    .filter((f) => f.family !== "Independent" && f.family !== "Unknown")
    .map((f) => {
      const meta = f.capabilities || {};
      let gain = 0.15;
      if (f.adapter_class === ADAPTER_CLASS.STRONG_NATIVE) gain = 0.55;
      else if (f.adapter_class === ADAPTER_CLASS.PARTIAL) gain = 0.4;
      else if (f.adapter_class === ADAPTER_CLASS.NO_ADAPTER && meta.directory) gain = 0.35;
      else if (f.adapter_class === ADAPTER_CLASS.SOFT_BRAND) gain = 0.25;
      else gain = 0.12;

      // Rooms gain bonus when rooms_native is not FIRST_PARTY-only
      if (meta.rooms_native && !/FIRST_PARTY/.test(String(meta.rooms_native))) gain += 0.08;

      let cost = 3; // medium
      if (f.adapter_class === ADAPTER_CLASS.STRONG_NATIVE) cost = 1;
      else if (f.adapter_class === ADAPTER_CLASS.PARTIAL) cost = 2;
      else if (meta.directory) cost = 2.5;
      else cost = 4;

      const hotels = f.candidate_count;
      const roi = (hotels * gain) / cost;
      return {
        family: f.family,
        hotels_affected: hotels,
        expected_completeness_gain: Number(gain.toFixed(2)),
        research_cost: cost,
        roi_score: Math.round(roi * 10) / 10,
        adapter_class: f.adapter_class,
        action:
          f.adapter_class === ADAPTER_CLASS.STRONG_NATIVE
            ? "strengthen_rooms_and_property_id"
            : f.adapter_class === ADAPTER_CLASS.PARTIAL
              ? "strengthen_structured_detail"
              : meta.directory
                ? "build_lightweight_directory_adapter"
                : "defer_or_first_party",
      };
    })
    .sort((a, b) => b.roi_score - a.roi_score);

  return {
    version: "adapter-roi-ranking-v2.2",
    ranking_formula: "hotels_affected × expected_completeness_gain ÷ research_cost",
    top_10: ranked.slice(0, 10),
    all: ranked,
  };
}

export function buildOfficialCapabilityMap(universe) {
  return {
    version: "official-source-capability-map-v2.2",
    families: (universe.families || []).map((f) => {
      const c = f.capabilities || {};
      return {
        family: f.family,
        candidate_count: f.candidate_count,
        directory: Boolean(c.directory),
        sitemap: ["IHG", "Hilton", "Choice", "Marriott", "Accor", "Hyatt"].includes(f.family),
        json_ld: ["Accor", "Marriott", "Hyatt", "IHG"].includes(f.family) ? "possible" : "unknown",
        embedded_state: ["IHG", "Marriott", "Hyatt", "Choice"].includes(f.family),
        graphql: f.family === "Hilton",
        rest: ["IHG", "Choice"].includes(f.family) ? "partial" : false,
        search_api: false,
        property_api: f.family === "Hilton" ? "graphql" : false,
        pdf_fact_sheet: ["Marriott", "Hilton", "IHG", "Hyatt"].includes(f.family) ? "occasional" : "rare",
        development_directory: ["Hilton", "Marriott", "IHG"].includes(f.family) ? "partial" : false,
        hotel_detail_endpoint: Boolean(c.property_detail),
        property_id_field: c.property_id || null,
        rooms_availability: c.rooms_native || "UNKNOWN",
        address: Boolean(c.address),
        coordinates: Boolean(c.coordinates),
        phone: Boolean(c.phone),
        amenities: Boolean(c.amenities),
        status: Boolean(c.status),
      };
    }),
  };
}

export function pidKey(country, name) {
  return `pid_${createHash("sha1").update(`${country}|${normName(name)}`).digest("hex").slice(0, 16)}`;
}
