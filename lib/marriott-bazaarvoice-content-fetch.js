/**
 * Marriott hotel marketing copy via public Bazaarvoice products API (MARSHA product id).
 * Works server-side when marriott.com overview pages are Akamai-blocked.
 */

export const MARRIOTT_BAZAARVOICE_PASSKEY =
  process.env.MARRIOTT_BAZAARVOICE_PASSKEY ||
  "canCX9lvC812oa4Y6HYf4gmWK5uszkZCKThrdtYkZqcYE";

export const MARRIOTT_BAZAARVOICE_PRODUCTS_URL =
  "https://api.bazaarvoice.com/data/products.json";

export const MARRIOTT_CONTENT_SOURCE_BAZAARVOICE = "marriott_bazaarvoice";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string[]} marshaCodes
 * @param {object} [opts]
 * @param {number} [opts.batchSize]
 * @param {number} [opts.delayMs]
 */
export async function fetchMarriottBazaarvoiceProducts(marshaCodes, opts = {}) {
  const batchSize = Math.max(1, opts.batchSize ?? 10);
  const delayMs = opts.delayMs ?? 200;
  const unique = [...new Set(marshaCodes.map((m) => String(m || "").trim().toUpperCase()).filter(Boolean))];

  /** @type {Map<string, { marshaCode: string, description: string, name: string, website: string, source: string }>} */
  const byMarsha = new Map();

  for (let i = 0; i < unique.length; i += batchSize) {
    const batch = unique.slice(i, i + batchSize).map((m) => m.toLowerCase());
    const params = new URLSearchParams({
      passkey: MARRIOTT_BAZAARVOICE_PASSKEY,
      apiversion: "5.5",
      filter: `id:eq:${batch.join(",")}`,
      limit: String(batch.length),
    });
    const res = await fetch(`${MARRIOTT_BAZAARVOICE_PRODUCTS_URL}?${params}`);
    if (!res.ok) {
      throw new Error(`Bazaarvoice HTTP ${res.status} for batch ${batch.join(",")}`);
    }
    const json = await res.json();
    for (const row of json.Results || []) {
      const marsha = String(row.Id || "").trim().toUpperCase();
      if (!marsha) continue;
      byMarsha.set(marsha, {
        marshaCode: marsha,
        description: String(row.Description || "").trim(),
        name: String(row.Name || "").trim(),
        website: String(row.ProductPageUrl || "").trim(),
        source: MARRIOTT_CONTENT_SOURCE_BAZAARVOICE,
      });
    }
    if (delayMs > 0 && i + batchSize < unique.length) await sleep(delayMs);
  }

  return byMarsha;
}

/**
 * @param {string} marshaCode
 */
export async function fetchMarriottBazaarvoiceProduct(marshaCode) {
  const marsha = String(marshaCode || "").trim().toUpperCase();
  if (!marsha) return null;
  const map = await fetchMarriottBazaarvoiceProducts([marsha], { batchSize: 1, delayMs: 0 });
  return map.get(marsha) || null;
}
