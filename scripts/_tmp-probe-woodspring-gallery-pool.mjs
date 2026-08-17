const props = ["flf21", "ncb10", "nc936"];

const nameFns = (P, p) => [
  `${P}ExteriorTemp01_1.jpg`,
  `${P}ExteriorTemp1.jpg`,
  `${P}Exterior01_1.jpg`,
  `${P}Exterior02_1.jpg`,
  `${P}Exterior03_1.jpg`,
  `${P}Exterior1_1.jpg`,
  `${P}Exterior1.jpg`,
  `${P}Exterior2_1.jpg`,
  `${P}Exterior3_1.jpg`,
  `${P}Exterior5_1.JPG`,
  `${P}AerialTemp1_1.jpg`,
  `${P}GuestRoom1_1.jpg`,
  `${P}GuestRoom2_1.jpg`,
  `${P}GuestRoom1.jpg`,
  `${P}Kitchen1_1.jpg`,
  `${P}Kitchen2_1.jpg`,
  `${P}Suite1_1.jpg`,
  `${P}Suite2_1.jpg`,
  `${P}Lobby1_1.jpg`,
  `${P}LivingRoom1_1.jpg`,
  `Exterior1.JPG`,
  `${p}exterior2_1.jpg`,
];

async function ok(url) {
  try {
    const res = await fetch(url, { redirect: "follow", signal: AbortSignal.timeout(10000) });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "";
    return ct.includes("image") ? url : "";
  } catch {
    return "";
  }
}

const all = [];
for (const pid of props) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const p = pid.toLowerCase();
  const found = [];
  for (const name of nameFns(P, p)) {
    for (const size of ["1280", "2048", "480"]) {
      const url = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`;
      const hit = await ok(url);
      if (hit) {
        found.push(hit);
        all.push(hit);
        break;
      }
    }
  }
  console.log(`\n${pid} (${found.length}):`);
  for (const u of found) console.log(" ", u);
}
console.log(`\nTOTAL DISTINCT: ${new Set(all).size}`);
