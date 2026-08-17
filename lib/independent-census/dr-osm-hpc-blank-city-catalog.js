/**
 * Curated City (+ State) backfill for DR OSM HPC rows with blank City.
 * Keys = Property Identity Key (osm_do_*).
 */
import { resolveDominicanRepublicStateRegion } from "./dominican-republic-state-region.js";

export const DR_OSM_HPC_BLANK_CITY_CATALOG = Object.freeze({
  osm_do_way_539003044: "Bávaro", // Bahía Príncipe Bouganville
  osm_do_way_471926026: "Bávaro", // Grand Bahia Principe Turquesa
  osm_do_way_406649862: "Santo Domingo", // Hodelpa Caribe Colonial
  osm_do_node_4660748808: "Bávaro", // Grand Bahia Principe Aquamarine
  osm_do_way_255806141: "Bávaro", // Barceló Dominican Beach
  osm_do_way_1040966966: "Santiago", // Hodelpa Garden Suites
  osm_do_node_6468314516: "Juan Dolio", // Emotions by Hodelpa
  osm_do_way_233446403: "Santiago", // Hodelpa Gran Almirante
  osm_do_way_312593390: "Santiago", // Hodelpa Garden Court
  osm_do_node_13081433749: "Bayahibe", // Viva Dominicus Beach
  osm_do_way_545028233: "Juan Dolio", // Casa Hemingway
  osm_do_node_3501119758: "Bayahibe", // Viva Dominicus Palace
  osm_do_node_4901959222: "Punta Cana", // Hotel Riu República
  osm_do_way_199921835: "Las Terrenas", // Gran Bahia Principe El Portillo
  // Villa Ibiscus / ambiguous brand — leave steward (no invent)
});

/**
 * @param {object} fields
 * @returns {{ ok: boolean, patch: Record<string,string>, reason: string }}
 */
export function buildBlankCityBackfillProposal(fields) {
  const key = String(fields["Property Identity Key"] || "").trim();
  const city = String(fields.City || "").trim();
  if (city && !/^unknown$/i.test(city)) {
    return { ok: false, patch: {}, reason: "city_already_set" };
  }
  const resolved = DR_OSM_HPC_BLANK_CITY_CATALOG[key];
  if (!resolved) {
    return { ok: false, patch: {}, reason: "no_catalog_city" };
  }
  /** @type {Record<string, string>} */
  const patch = { City: resolved };
  const st = resolveDominicanRepublicStateRegion(resolved);
  if (st.ok && st.province) patch["State / Region"] = st.province;
  return {
    ok: true,
    patch,
    reason: "dr_osm_hpc_blank_city_catalog",
  };
}
