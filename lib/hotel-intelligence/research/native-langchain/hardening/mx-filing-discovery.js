/**
 * Packet 2.8C-2 — Mexico public-company / issuer-aware filing discovery.
 * Organization + issuer aware — NOT hard-coded GSF URLs.
 */

export const MX_FILING_DISCOVERY_VERSION = "mx-filing-discovery-v1";

/**
 * Build issuer-aware discovery queries for MX public / corporate ownership research.
 */
export function buildMxFilingDiscoveryQueries({
  hotel_name,
  company,
  propco,
  aliases = [],
  year = new Date().getFullYear(),
} = {}) {
  const co = company || hotel_name || "";
  const queries = [
    `"${co}" "reporte anual" ${year}`,
    `"${co}" "reporte anual" ${(year - 1)}`,
    `"${co}" "hechos relevantes" hotel OR hoteles`,
    `"${co}" BMV OR CNBV portafolio OR hoteles`,
    `"${co}" "estados financieros" subsidiaria`,
    hotel_name ? `"${hotel_name}" "${co}" reporte OR portafolio` : null,
    propco ? `"${propco}" hotel OR inmueble OR subsidiaria` : null,
    `"${co}" "partes relacionadas" hotel`,
    ...aliases.slice(0, 2).map((a) => `"${a}" "${co}" adquisición OR venta OR portafolio`),
  ].filter(Boolean);

  return {
    version: MX_FILING_DISCOVERY_VERSION,
    issuer: co,
    hotel_name: hotel_name || null,
    propco: propco || null,
    queries: [...new Set(queries)].slice(0, 8),
    source_families: ["bmv", "issuer_ir", "annual_report", "hecho_relevante", "subsidiary_note"],
    hard_coded_issuer_urls: false,
  };
}

export function scoreMxFilingUrl(url = "", title = "") {
  const u = `${url} ${title}`.toLowerCase();
  let score = 0;
  if (/bmv\.com\.mx/.test(u)) score += 30;
  if (/reporte.?anual|annual.?report/.test(u)) score += 25;
  if (/hecho.?relevante/.test(u)) score += 20;
  if (/\.pdf($|\?)/.test(u)) score += 15;
  if (/investor|inversionista|ri\./.test(u)) score += 12;
  if (/booking|tripadvisor|expedia|facebook|linkedin\.com\/posts/.test(u)) score -= 40;
  return score;
}
