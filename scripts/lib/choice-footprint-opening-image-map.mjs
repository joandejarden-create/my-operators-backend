/**
 * Property page URL → hoteldam hero image (Airtable can fetch via upload-bytes, not direct Choice URL).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { IMAGE_BY_SOURCE_URL } from "./case-study-image-url-map.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROBED_MAP_PATH = path.resolve(__dirname, "../../fixtures/choice-footprint-opening-hoteldam-map.json");

/** @type {Record<string, string>} */
let PROBED_BY_PAGE = {};
try {
  if (fs.existsSync(PROBED_MAP_PATH)) {
    PROBED_BY_PAGE = JSON.parse(fs.readFileSync(PROBED_MAP_PATH, "utf8"));
  }
} catch (err) {
  console.warn("[choice-footprint-opening-image-map] could not load probed map:", err.message);
}

/** @type {Record<string, string>} */
const EXTRA_BY_PROPERTY_PAGE = {
  "https://www.choicehotels.com/en-mx/mexico/puebla/radisson-hotels/mxpue":
    "https://www.choicehotels.com/hoteldam/mx/mxpue/images/1280/MXPUEExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/en-mx/mexico/san-luis-potosi/radisson-hotels/slpap":
    "https://www.choicehotels.com/hoteldam/mx/slpap/images/1280/SLPAPExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/suriname/paramaribo/radisson-hotels/sr001":
    "https://www.choicehotels.com/hoteldam/sr/sr001/images/1280/SR001ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/es-mx/panama/panama/radisson-hotels/pn018":
    "https://www.choicehotels.com/hoteldam/pn/pn018/images/1280/PN018AerialTemp1_1.jpg",
  "https://www.choicehotels.com/argentina/san-carlos-de-bariloche/radisson-blu-hotels/aa022":
    "https://www.choicehotels.com/hoteldam/aa/aa022/images/1280/AA022ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/chile/santiago/radisson-blu-hotels/cl012":
    "https://www.choicehotels.com/hoteldam/cl/cl012/images/1280/CL012ExteriorTemp01_1.jpg",
  "https://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007":
    "https://www.choicehotels.com/hoteldam/aw/aw007/images/2048/AW007Exterior5_1.JPG",
};

/** @type {Record<string, string>} */
const BY_PROPERTY_ID = {};

function normalizePageUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    u.hash = "";
    u.search = "";
    let s = u.toString();
    if (s.endsWith("/")) s = s.slice(0, -1);
    return s;
  } catch {
    return String(url || "").trim();
  }
}

function propertyIdFromPageUrl(url) {
  const parts = normalizePageUrl(url).split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function seedMaps() {
  const sources = { ...IMAGE_BY_SOURCE_URL, ...EXTRA_BY_PROPERTY_PAGE, ...PROBED_BY_PAGE };
  for (const [pageUrl, imageUrl] of Object.entries(sources)) {
    const norm = normalizePageUrl(pageUrl);
    if (norm && imageUrl) BY_PROPERTY_ID[propertyIdFromPageUrl(norm)] = imageUrl;
  }
}

seedMaps();

/**
 * @param {string} propertyPageUrl
 * @returns {string}
 */
export function resolveFootprintOpeningImageUrl(propertyPageUrl) {
  const norm = normalizePageUrl(propertyPageUrl);
  if (!norm) return "";
  const direct =
    IMAGE_BY_SOURCE_URL[norm] || EXTRA_BY_PROPERTY_PAGE[norm] || PROBED_BY_PAGE[norm];
  if (direct) return direct;
  const pid = propertyIdFromPageUrl(norm);
  return BY_PROPERTY_ID[pid] || "";
}
