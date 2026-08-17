#!/usr/bin/env node
const SLUG = "pujac-ac-hotel-punta-cana";
const paths = [
  `/content/marriott-hws/na/en-us/hotels/p/${SLUG}/overview/jcr:content.1.json`,
  `/content/marriott-hws/na/en-us/hotels/p/${SLUG}/overview/jcr:content.infinity.json`,
  `/content/marriott-hws/na/en-us/hotels/p/${SLUG}/overview.model.json`,
  `/services/marriott-hws/propertyOverview/?marsha=PUJAC&locale=en-US`,
];
const H = { "User-Agent": "Mozilla/5.0", Accept: "application/json,text/html" };
for (const p of paths) {
  const url = `https://www.marriott.com${p}`;
  const r = await fetch(url, { headers: H });
  const t = await r.text();
  console.log(r.status, p.slice(-50));
  if (r.status === 200 && t.length > 100 && !/^<!DOCTYPE/i.test(t.slice(0, 20))) {
    console.log(t.slice(0, 500));
  }
}
