#!/usr/bin/env node
/**
 * Fetch property-specific pages and extract hotel-named images only.
 * Strict: URL path or nearby context must include hotel/property token.
 */
import fs from "fs";

fs.mkdirSync("data/operator-gallery-research/strict", { recursive: true });

async function page(key, url) {
  const r = await fetch(url, {
    headers: {
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
      accept: "text/html",
    },
    redirect: "follow",
  });
  const html = await r.text();
  fs.writeFileSync(`data/operator-gallery-research/strict/${key}.html`, html);
  const og =
    (html.match(/property=["']og:image["'][^>]*content=["']([^"']+)/i) ||
      html.match(/content=["']([^"']+)["'][^>]*property=["']og:image["']/i) ||
      [])[1] || null;
  const imgs = [
    ...new Set(
      [...html.matchAll(/https?:\/\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) =>
        m[0].replace(/&amp;/g, "&")
      )
    ),
  ].filter((x) => !/icon|logo|favicon|sprite|pixel|avatar|tracking|spacer|banner-sale|rewards/i.test(x));
  console.log(key, r.status, "og", !!og, "imgs", imgs.length, "→", r.url);
  if (og) console.log("  OG", og);
  imgs.slice(0, 8).forEach((u) => console.log(" ", u.slice(0, 140)));
  fs.writeFileSync(
    `data/operator-gallery-research/strict/${key}.json`,
    JSON.stringify({ status: r.status, finalUrl: r.url, og, imgs: imgs.slice(0, 40) }, null, 2)
  );
  return { og, imgs, status: r.status, finalUrl: r.url };
}

const pages = [
  // Arriva / Crown / Sensira / Vista property pages
  ["arriva-home", "https://www.arrivahotels.mx/en/arriva-hospitality-group"],
  ["cp-cun", "https://www.crownparadise.com/crown-paradise-club-cancun/"],
  ["cp-cun-en", "https://www.crownparadise.com/en/hotels/crown-paradise-club-cancun/"],
  ["cp-pv", "https://www.crownparadise.com/crown-paradise-club-puerto-vallarta/"],
  ["cp-golden", "https://www.crownparadise.com/crown-paradise-golden/"],
  ["cp-golden2", "https://www.crownparadise.com/en/hotels/crown-paradise-golden-resort-puerto-vallarta/"],
  ["sensira-home", "https://sensiraresorts.com/en/"],
  ["vista-oro", "https://www.vistahoteles.com.mx/"],
  ["vista-oro2", "https://www.vistaplayadeoro.com/"],
  // Tafer VDP Cancun
  ["vdp-cun", "https://www.villadelpalmarcancun.com/"],
  ["vdp-cun2", "https://www.villadelpalmar.com/cancun/"],
  ["mousai-pv", "https://www.mousaihotels.com/puerto-vallarta/"],
  ["mousai-cun", "https://www.mousaihotels.com/cancun/"],
  // Atlantica real gallery photos (not banners)
  ["ahi-jardins", "https://www.letsatlantica.com.br/hotel/transamerica-executive-jardins"],
  ["ahi-berrini", "https://www.letsatlantica.com.br/hotel/transamerica-berrini"],
  ["ahi-faria", "https://www.letsatlantica.com.br/hotel/transamerica-executive-faria-lima"],
  ["ahi-congonhas", "https://www.letsatlantica.com.br/hotel/transamerica-executive-congonhas"],
  ["ahi-paulista", "https://www.letsatlantica.com.br/hotel/transamerica-executive-paulista"],
  ["ahi-perdizes", "https://www.letsatlantica.com.br/hotel/transamerica-executive-perdizes"],
  // Playa exteriors
  ["playa-ziva-cun", "https://www.playaresorts.com/hyatt-ziva-cancun"],
  ["playa-zilara-cun", "https://www.playaresorts.com/hyatt-zilara-cancun"],
  ["playa-ziva-cabo", "https://www.playaresorts.com/hyatt-ziva-los-cabos"],
  ["playa-ziva-pv", "https://www.playaresorts.com/hyatt-ziva-puerto-vallarta"],
  ["playa-zilara-rm", "https://www.playaresorts.com/hyatt-zilara-riviera-maya"],
  ["playa-ziva-rh", "https://www.playaresorts.com/hyatt-ziva-rose-hall"],
  // Brittain exteriors
  ["br-breakers", "https://www.breakers.com/"],
  ["br-caribbean", "https://www.caribbeanresort.com/"],
  ["br-compass", "https://www.compasscove.com/"],
  ["br-grande", "https://www.grandecaymanresort.com/"],
  ["br-paradise", "https://www.paradiseresortmb.com/"],
  ["br-ocean", "https://www.oceanreefmyrtlebeach.com/"],
];

for (const [k, u] of pages) {
  try {
    await page(k, u);
  } catch (e) {
    console.log(k, "ERR", e.message);
  }
}
