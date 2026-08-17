const props = ["flf21", "ncb10", "nc936"];

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

const sizes = ["1280", "2048", "480", "640"];
const bases = [
  "ExteriorTemp01_1.jpg",
  "ExteriorTemp1.jpg",
  "Exterior01_1.jpg",
  "Exterior02_1.jpg",
  "Exterior03_1.jpg",
  "Exterior04_1.jpg",
  "Exterior1_1.jpg",
  "Exterior2_1.jpg",
  "Exterior3_1.jpg",
  "Exterior1.jpg",
  "Exterior2.jpg",
  "Exterior5_1.JPG",
  "AerialTemp1_1.jpg",
  "GuestRoom1_1.jpg",
  "GuestRoom2_1.jpg",
  "Kitchen1_1.jpg",
  "Suite1_1.jpg",
  "Lobby1_1.jpg",
  "exterior2_1.jpg",
  "Exterior1.JPG",
];

const all = new Set();
for (const pid of props) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const p = pid.toLowerCase();
  const found = [];
  for (const base of bases) {
    for (const prefix of [`${P}`, `${p}`, ""]) {
      const name = prefix ? `${prefix}${base}` : base;
      for (const size of sizes) {
        const url = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`;
        const hit = await ok(url);
        if (hit && !all.has(hit.toLowerCase())) {
          all.add(hit.toLowerCase());
          found.push(hit);
          break;
        }
      }
    }
  }
  console.log(`${pid}: ${found.length}`);
  found.forEach((u) => console.log(" ", u));
}
console.log("TOTAL", all.size);
