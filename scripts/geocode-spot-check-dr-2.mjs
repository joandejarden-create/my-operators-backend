#!/usr/bin/env node
const DELAY = 1100;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const checks = [
  ["Ocean World", "Ocean World Adventure Park Puerto Plata"],
  ["Playa Dorada", "Playa Dorada Puerto Plata"],
  ["Agora Mall", "Agora Mall Santo Domingo"],
  ["Teatro Nacional", "Teatro Nacional Eduardo Brito Santo Domingo"],
  ["Hospiten Bavaro", "Hospiten Bavaro"],
  ["ICC Punta Cana", "Hard Rock Hotel Punta Cana convention"],
  ["Acropolis", "Acropolis Centro de Convenciones Santo Domingo"],
  ["Barceló Convention", "Barcelo Bavaro Palace Convention Center"],
  ["BlueMall", "Blue Mall Punta Cana"],
  ["CIDAC", "Centro Internacional de Ferias y Congresos CIDAC"],
  ["Zemi Miches", "Zemi Miches Hilton"],
  ["Fiesta Resort", "Fiesta Hotel Resort Juan Dolio"],
  ["Metro Country Club", "Metro Country Club Juan Dolio"],
  ["Bayahibe Marina", "Bayahibe marina Saona"],
  ["Whale watching pier", "Muelle Samana whale watching"],
  ["PUCMM Santiago", "PUCMM campus Santiago"],
  ["Santiago Monument", "Monumento Santiago Restauracion"],
  ["Plaza Lama", "Plaza Lama Santiago"],
  ["Jarabacoa", "Jarabacoa town center"],
  ["Constanza", "Constanza town Dominican Republic"],
  ["Rancho Baiguate", "Rancho Baiguate Jarabacoa"],
  ["Lago Enriquillo", "Parque Nacional Lago Enriquillo"],
  ["Caucedo", "Puerto Caucedo DP World"],
  ["La Romana Cruise", "Casa de Campo cruise port La Romana"],
];

for (const [label, q] of checks) {
  const url =
    "https://nominatim.openstreetmap.org/search?" +
    new URLSearchParams({ q: q + ", Dominican Republic", format: "json", limit: "1", countrycodes: "do" });
  const hits = await (
    await fetch(url, { headers: { "User-Agent": "deal-capture-proxy/1.0" } })
  ).json();
  const h = hits[0];
  if (!h) console.log("NO HIT |", label);
  else console.log(label + ":", h.lat, h.lon, "|", h.display_name.slice(0, 85));
  await sleep(DELAY);
}
