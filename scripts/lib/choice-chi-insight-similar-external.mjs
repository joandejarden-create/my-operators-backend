/**
 * External illustrative peers for insight.similar (not Choice portfolio brands).
 * Title = competitor brand; Body = (parent · positioning note); sort 1–4.
 */

/** @typedef {{ title: string, body: string, sort: number }} SimilarPeer */

/** @type {Record<string, SimilarPeer[]>} */
export const EXTERNAL_SIMILAR_BY_BRAND = {
  "Comfort Inn & Suites": [
    { title: "Hampton by Hilton", body: "(Hilton · upper-midscale breakfast-led)", sort: 1 },
    { title: "Fairfield by Marriott", body: "(Marriott · select-service conversion mainstream)", sort: 2 },
    { title: "Holiday Inn Express", body: "(IHG · upper-midscale limited service)", sort: 3 },
    { title: "Best Western Plus", body: "(Best Western · upper-midscale regional network)", sort: 4 },
  ],
  "Sleep Inn": [
    { title: "Days Inn", body: "(Wyndham · value midscale highway)", sort: 1 },
    { title: "Super 8", body: "(Wyndham · economy-midscale conversion)", sort: 2 },
    { title: "Red Roof Inn", body: "(Red Roof · economy conversion mainstream)", sort: 3 },
    { title: "Motel 6", body: "(G6 Hospitality · economy highway)", sort: 4 },
  ],
  "Quality Inn": [
    { title: "Hampton by Hilton", body: "(Hilton · upper-midscale limited service)", sort: 1 },
    { title: "La Quinta", body: "(Wyndham · midscale highway and suburban)", sort: 2 },
    { title: "Fairfield by Marriott", body: "(Marriott · select-service mainstream)", sort: 3 },
    { title: "Holiday Inn Express", body: "(IHG · upper-midscale breakfast-led)", sort: 4 },
  ],
  "Cambria Hotels": [
    { title: "AC Hotel", body: "(Marriott · lifestyle upscale select-service)", sort: 1 },
    { title: "Hyatt Place", body: "(Hyatt · upscale select-service mainstream)", sort: 2 },
    { title: "Hilton Garden Inn", body: "(Hilton · upscale meetings-capable box)", sort: 3 },
    { title: "Aloft", body: "(Marriott · lifestyle select-service)", sort: 4 },
  ],
  "MainStay Suites": [
    { title: "Residence Inn", body: "(Marriott · extended-stay flagship)", sort: 1 },
    { title: "Homewood Suites", body: "(Hilton · extended-stay with breakfast)", sort: 2 },
    { title: "Hyatt House", body: "(Hyatt · extended-stay kitchen suites)", sort: 3 },
    { title: "Staybridge Suites", body: "(IHG · extended-stay all-suites)", sort: 4 },
  ],
  "Ascend Hotel Collection": [
    { title: "Autograph Collection", body: "(Marriott · soft-brand independent portfolio)", sort: 1 },
    { title: "Curio Collection", body: "(Hilton · soft-brand upscale independent)", sort: 2 },
    { title: "Tapestry Collection", body: "(Hilton · upper-upscale collection)", sort: 3 },
    { title: "Unbound Collection", body: "(Hyatt · soft-brand lifestyle collection)", sort: 4 },
  ],
  Clarion: [
    { title: "Holiday Inn", body: "(IHG · full-service upper-midscale)", sort: 1 },
    { title: "Best Western Plus", body: "(Best Western · upper-midscale meetings-capable)", sort: 2 },
    { title: "Wyndham Garden", body: "(Wyndham · upper-midscale suburban/urban)", sort: 3 },
    { title: "Delta Hotels", body: "(Marriott · four-star conversion-friendly)", sort: 4 },
  ],
  "Clarion Pointe": [
    { title: "Tru by Hilton", body: "(Hilton · midscale lifestyle micro-hotel)", sort: 1 },
    { title: "Moxy", body: "(Marriott · playful urban select-service)", sort: 2 },
    { title: "avid hotels", body: "(IHG · value-forward midscale)", sort: 3 },
    { title: "Spark by Hilton", body: "(Hilton · economy-midscale conversion)", sort: 4 },
  ],
  "Econo Lodge": [
    { title: "Motel 6", body: "(G6 Hospitality · economy highway)", sort: 1 },
    { title: "Super 8", body: "(Wyndham · economy conversion)", sort: 2 },
    { title: "Red Roof Inn", body: "(Red Roof · economy price-sensitive)", sort: 3 },
    { title: "Travelodge", body: "(Wyndham · economy corridor)", sort: 4 },
  ],
  "Rodeway Inn": [
    { title: "Motel 6", body: "(G6 Hospitality · economy highway)", sort: 1 },
    { title: "Super 8", body: "(Wyndham · economy franchise mainstream)", sort: 2 },
    { title: "Red Roof Inn", body: "(Red Roof · economy conversion)", sort: 3 },
    { title: "Days Inn", body: "(Wyndham · value economy-midscale)", sort: 4 },
  ],
  "Suburban Studios": [
    { title: "Extended Stay America", body: "(ESA · economy extended-stay)", sort: 1 },
    { title: "InTown Suites", body: "(InTown · weekly-stay economy)", sort: 2 },
    { title: "Studio 6", body: "(G6 Hospitality · extended-stay value)", sort: 3 },
    { title: "Candlewood Suites", body: "(IHG · extended-stay limited service)", sort: 4 },
  ],
  "Park Inn by Choice": [
    { title: "Holiday Inn", body: "(IHG · mainstream full-service upper-midscale)", sort: 1 },
    { title: "DoubleTree by Hilton", body: "(Hilton · full-service conversion mainstream)", sort: 2 },
    { title: "Crowne Plaza", body: "(IHG · meetings-led upscale)", sort: 3 },
    { title: "Best Western Plus", body: "(Best Western · upper-midscale network)", sort: 4 },
  ],
  "Country Inn & Suites by Radisson (Choice)": [
    { title: "Hampton by Hilton", body: "(Hilton · upper-midscale breakfast-led)", sort: 1 },
    { title: "Fairfield by Marriott", body: "(Marriott · select-service suburban)", sort: 2 },
    { title: "Holiday Inn Express", body: "(IHG · upper-midscale limited service)", sort: 3 },
    { title: "Hyatt Place", body: "(Hyatt · upscale select-service)", sort: 4 },
  ],
  "Everhome Suites": [
    { title: "Residence Inn", body: "(Marriott · extended-stay flagship)", sort: 1 },
    { title: "Homewood Suites", body: "(Hilton · extended-stay with breakfast)", sort: 2 },
    { title: "Element", body: "(Marriott · extended-stay eco-minded)", sort: 3 },
    { title: "Candlewood Suites", body: "(IHG · extended-stay kitchen suites)", sort: 4 },
  ],
  "Radisson RED by Choice": [
    { title: "Moxy", body: "(Marriott · playful urban select-service)", sort: 1 },
    { title: "Aloft", body: "(Marriott · lifestyle select-service)", sort: 2 },
    { title: "Hotel Indigo", body: "(IHG · boutique lifestyle)", sort: 3 },
    { title: "Canopy by Hilton", body: "(Hilton · lifestyle upscale select)", sort: 4 },
  ],
  "Radisson Individuals by Choice": [
    { title: "Autograph Collection", body: "(Marriott · soft-brand independent)", sort: 1 },
    { title: "Curio Collection", body: "(Hilton · soft-brand upscale independent)", sort: 2 },
    { title: "Tapestry Collection", body: "(Hilton · upper-upscale collection)", sort: 3 },
    { title: "Tribute Portfolio", body: "(Marriott · independent boutique collection)", sort: 4 },
  ],
  "Radisson Collection by Choice": [
    { title: "Luxury Collection", body: "(Marriott · luxury collection flagship)", sort: 1 },
    { title: "Waldorf Astoria", body: "(Hilton · luxury iconic)", sort: 2 },
    { title: "St. Regis", body: "(Marriott · luxury heritage)", sort: 3 },
    { title: "Conrad", body: "(Hilton · luxury urban/resort)", sort: 4 },
  ],
  "Park Plaza by Choice": [
    { title: "Crowne Plaza", body: "(IHG · meetings-led upscale)", sort: 1 },
    { title: "Sheraton", body: "(Marriott · classic full-service network)", sort: 2 },
    { title: "Hyatt Regency", body: "(Hyatt · convention upscale)", sort: 3 },
    { title: "Delta Hotels", body: "(Marriott · four-star conversion-friendly)", sort: 4 },
  ],
  "Radisson Inn & Suites": [
    { title: "Holiday Inn Express", body: "(IHG · upper-midscale limited service)", sort: 1 },
    { title: "Hampton by Hilton", body: "(Hilton · upper-midscale breakfast-led)", sort: 2 },
    { title: "Fairfield by Marriott", body: "(Marriott · select-service new-build)", sort: 3 },
    { title: "Hyatt Place", body: "(Hyatt · upscale select-service)", sort: 4 },
  ],
  "WoodSpring Suites": [
    { title: "Extended Stay America", body: "(ESA · economy extended-stay)", sort: 1 },
    { title: "InTown Suites", body: "(InTown · weekly-stay value)", sort: 2 },
    { title: "Studio 6", body: "(G6 Hospitality · extended-stay economy)", sort: 3 },
    { title: "Value Place", body: "(independent · weekly-stay economy)", sort: 4 },
  ],
};

// Profile / alternate Airtable names → canonical peer list
EXTERNAL_SIMILAR_BY_BRAND["Park Inn by Radisson (Choice)"] =
  EXTERNAL_SIMILAR_BY_BRAND["Park Inn by Choice"];
EXTERNAL_SIMILAR_BY_BRAND["Country Inn & Suites by Radisson"] =
  EXTERNAL_SIMILAR_BY_BRAND["Country Inn & Suites by Radisson (Choice)"];
EXTERNAL_SIMILAR_BY_BRAND["Radisson RED  (Choice)"] = EXTERNAL_SIMILAR_BY_BRAND["Radisson RED by Choice"];
EXTERNAL_SIMILAR_BY_BRAND["Radisson Individual (Choice)"] =
  EXTERNAL_SIMILAR_BY_BRAND["Radisson Individuals by Choice"];
EXTERNAL_SIMILAR_BY_BRAND["Radisson Collection  (Choice)"] =
  EXTERNAL_SIMILAR_BY_BRAND["Radisson Collection by Choice"];
EXTERNAL_SIMILAR_BY_BRAND["Park Plaza (Choice)"] = EXTERNAL_SIMILAR_BY_BRAND["Park Plaza by Choice"];

const SKIP_BRANDS = new Set(["Radisson by Choice", "Radisson Blu by Choice"]);

/**
 * @param {string} brandName
 * @returns {SimilarPeer[] | null}
 */
export function externalSimilarPeersForBrand(brandName) {
  if (SKIP_BRANDS.has(brandName)) return null;
  return EXTERNAL_SIMILAR_BY_BRAND[brandName] || null;
}

/**
 * @param {{ title?: string, body?: string }[]} rows
 */
export function insightSimilarRowsNeedFix(rows) {
  if (!rows.length) return true;
  return rows.some((r) => {
    const title = String(r.title || "").trim();
    const body = String(r.body || "");
    if (!title) return true;
    if (/Compare diligence to|same parent company/i.test(body)) return true;
    return false;
  });
}
