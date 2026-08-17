/**
 * Generic support + contradiction query generator.
 * Do not hardcode hotel-specific expected outcomes.
 */

/**
 * @param {object} hotel
 * @param {object} [opts]
 */
export function generateResearchQueries(hotel, opts = {}) {
  const name = String(hotel.name || hotel.hotelName || "").trim();
  const brand = String(hotel.currentBrand || hotel.affiliation || opts.currentBrand || "").trim();
  const status = String(hotel.currentStatus || hotel.status || opts.currentStatus || "").trim();
  const city = String(hotel.city || "").trim();
  const country = String(hotel.country || "").trim();
  const parent = String(hotel.currentParent || hotel.parentCompany || opts.currentParent || "").trim();
  const operator = String(hotel.currentOperator || hotel.managementCompany || "").trim();
  const brandFamily = String(opts.brandFamily || hotel.brandFamily || "").trim();

  const geo = [city, country].filter(Boolean).join(" ");
  const base = [name, geo].filter(Boolean).join(" ");

  /** @type {string[]} */
  const support = [];
  /** @type {string[]} */
  const contradiction = [];

  if (brand) {
    support.push(`${base} ${brand}`);
    support.push(`${name} ${brand} official`);
    if (brandFamily) support.push(`${name} ${brandFamily} directory`);
  }
  if (/pipeline/i.test(status)) {
    support.push(`${name} ${brand} pipeline`);
    support.push(`${name} opening ${brand}`.trim());
    support.push(`${name} development ${brandFamily || brand}`.trim());
    contradiction.push(`${name} now open`);
    contradiction.push(`${name} reservations`);
    contradiction.push(`${name} booking`);
    contradiction.push(`${name} opened`);
    contradiction.push(`${name} book now`);
    contradiction.push(`${name} accepting reservations`);
  }
  if (/^open$/i.test(status) || /operating/i.test(status)) {
    support.push(`${name} ${brand} open`);
    support.push(`${name} book ${brand}`.trim());
    contradiction.push(`${name} closed`);
    contradiction.push(`${name} permanently closed`);
    contradiction.push(`${name} coming soon`);
    contradiction.push(`${name} pipeline`);
  }

  // Brand / reflag disproof paths (generic alternate-affiliation probes)
  contradiction.push(`${name} reflag`);
  contradiction.push(`${name} new brand`);
  contradiction.push(`${name} renamed`);
  contradiction.push(`${name} independent`);
  contradiction.push(`${name} current official site`);
  if (brandFamily === "marriott" || /marriott|tribute|autograph|design hotels/i.test(brand)) {
    contradiction.push(`${name} Autograph Collection`);
    contradiction.push(`${name} Design Hotels`);
    contradiction.push(`${name} Tribute Portfolio`);
    contradiction.push(`${name} Marriott new brand`);
  }
  if (brandFamily === "ihg" || /indigo|kimpton|ihg/i.test(brand + parent)) {
    contradiction.push(`${name} Hotel Indigo`);
    contradiction.push(`${name} Kimpton`);
    contradiction.push(`${name} IHG`);
  }
  if (brandFamily === "choice" || /choice|radisson individual/i.test(brand + parent)) {
    contradiction.push(`${name} Choice Hotels`);
    contradiction.push(`${name} Radisson Individuals`);
    contradiction.push(`${name} Faranda`);
  }

  contradiction.push(`${name} operator`);
  if (operator) {
    support.push(`${name} managed by ${operator}`);
    contradiction.push(`${name} operator change`);
  }
  if (parent) {
    support.push(`${name} ${parent}`);
    contradiction.push(`${name} parent company`);
  }

  return {
    supportQueries: uniq(support),
    contradictionQueries: uniq(contradiction),
  };
}

/**
 * @param {string[]} list
 */
function uniq(list) {
  const seen = new Set();
  /** @type {string[]} */
  const out = [];
  for (const raw of list) {
    const s = String(raw || "")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || seen.has(s.toLowerCase())) continue;
    seen.add(s.toLowerCase());
    out.push(s);
  }
  return out;
}
