import {
  brandIndexUrl,
  fetchHiltonLocationsPage,
  extractHotelsFromPageData,
  normalizeHiltonDirectoryHotel,
} from "../lib/hilton-brand-directory-extract.js";
import { formatAmenitiesText } from "../lib/hilton-amenity-map.js";

const ctyhocn = (process.argv[2] || "PUJMIQQ").toUpperCase();

// Try DR country page on curio collection
const urls = [
  "https://www.hilton.com/en/locations/dominican-republic/curio-collection/",
  brandIndexUrl("curio-collection"),
];

for (const url of urls) {
  try {
    console.log("\nFetching", url);
    const page = await fetchHiltonLocationsPage(url);
    const hotels = extractHotelsFromPageData(page.pageData);
    const hit = hotels.find((h) => String(h.ctyhocn || "").toUpperCase() === ctyhocn);
    if (!hit) {
      console.log("not on page, hotels:", hotels.length);
      continue;
    }
    const norm = normalizeHiltonDirectoryHotel(hit, { sourceUrl: url });
    console.log("name:", norm.name);
    console.log("amenityIds:", norm.amenityIds);
    console.log("amenities text:", formatAmenitiesText(norm.amenityIds));
    break;
  } catch (e) {
    console.log("error:", e.message);
  }
}
