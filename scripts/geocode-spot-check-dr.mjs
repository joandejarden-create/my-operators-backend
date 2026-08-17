#!/usr/bin/env node
const DELAY = 1100;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

const checks = [
  ["Montaña Redonda", "Montaña Redonda Miches", 18.895, -69.576],
  ["Bahía Príncipe Grand Samaná", "Bahia Principe Luxury Samana Los Cacaos", 19.1977, -69.2709],
  ["Scape Park at Cap Cana", "Scape Park Cap Cana", 18.483, -68.362],
  ["ICC Punta Cana", "ICC Punta Cana convention center", 18.5425, -68.3769],
  ["Hospiten Bavaro", "Hospiten Bavaro Medical Center", 18.6803, -68.421],
  ["Acropolis Convention Center", "Acropolis Convention Center Santo Domingo", 18.4612, -69.9408],
  ["Hotel El Embajador", "Hotel El Embajador Santo Domingo", 18.4689, -69.9425],
  ["Coco Bongo Punta Cana", "Coco Bongo Punta Cana", 18.5595, -68.3715],
  ["Zemi Miches", "Zemi Miches All Inclusive Resort", 18.988, -69.041],
  ["Fiesta Resort Juan Dolio", "Fiesta Resort Juan Dolio", 18.436, -69.43],
  ["Metro Country Club Juan Dolio", "Metro Country Club Juan Dolio", 18.435, -69.425],
  ["Lago Enriquillo", "Lago Enriquillo visitor center", 18.53, -71.6],
  ["Cabo Rojo Pedernales", "Cabo Rojo Pedernales Dominican Republic", 17.9, -71.667],
  ["Las Galeras", "Las Galeras beach Samana", 19.297, -69.207],
  ["Playa Juan Dolio", "Playa Juan Dolio", 18.424, -69.417],
  ["Bávaro Beach", "Playa Bavaro Punta Cana", 18.6825, -68.4358],
  ["Barceló Bávaro Convention", "Barcelo Bavaro Convention Center", 18.6861, -68.4497],
  ["BlueMall Punta Cana", "BlueMall Punta Cana", 18.5586, -68.3881],
  ["CIDAC Santo Domingo", "CIDAC Santo Domingo", 18.4872, -69.9594],
  ["Estadio Quisqueya", "Estadio Quisqueya Santo Domingo", 18.4839, -69.8997],
  ["Samaná Whale Watching", "Santa Barbara de Samana waterfront", 19.2056, -69.3364],
  ["Samaná Eco Corridor", "Las Terrenas Samana peninsula", 19.298, -69.452],
  ["Costa Esmeralda corridor", "Costa Esmeralda Miches", 18.975, -69.055],
  ["Caucedo Port", "DP World Caucedo port", 18.4242, -69.6167],
  ["Central Romana", "Central Romana La Romana", 18.418, -68.965],
  ["Jarabacoa hub", "Jarabacoa Dominican Republic", 19.1167, -70.6333],
  ["Constanza gateway", "Constanza Dominican Republic", 18.9167, -70.7333],
  ["Rancho Baiguate", "Rancho Baiguate Jarabacoa", 19.11, -70.64],
];

for (const [label, q, clat, clng] of checks) {
  const url =
    "https://nominatim.openstreetmap.org/search?" +
    new URLSearchParams({
      q: q + ", Dominican Republic",
      format: "json",
      limit: "1",
      countrycodes: "do",
    });
  const res = await fetch(url, {
    headers: { "User-Agent": "deal-capture-proxy/1.0 (radar coordinate audit)" },
  });
  const hits = await res.json();
  const h = hits[0];
  if (!h) {
    console.log("NO HIT |", label);
    await sleep(DELAY);
    continue;
  }
  const lat = Number(h.lat);
  const lng = Number(h.lon);
  const drift = haversineKm(clat, clng, lat, lng);
  console.log(`${drift.toFixed(1)} km | ${label}`);
  console.log(`  cur: ${clat}, ${clng}`);
  console.log(`  osm: ${lat}, ${lng} | ${h.display_name.slice(0, 95)}`);
  await sleep(DELAY);
}
