const pid = "nc936";
const cc = pid.slice(0, 2);
const P = pid.toUpperCase();
const p = pid.toLowerCase();
const sizes = ["1280", "2048", "480", "640"];
const names = [
  `${P}ExteriorTemp01_1.jpg`,
  `${P}ExteriorTemp1.jpg`,
  `${P}Exterior01_1.jpg`,
  `${P}Exterior1_1.jpg`,
  `${P}Exterior1.jpg`,
  `${P}Hexterior01_1.jpeg`,
  `${P}TerraceTemp001_1.jpg`,
  `${P}PoolCourtyard4_1.JPG`,
  `${P}AerialTemp1_1.jpg`,
  `${P}GuestRoom1_1.jpg`,
  `${P}GuestRoomTemp01_1.jpg`,
  `${P}Suite1_1.jpg`,
  `${P}Kitchen1_1.jpg`,
  `Exterior1.JPG`,
  `${p}exterior2_1.jpg`,
  `${P}Exterior5_1.JPG`,
  `${P}Exterior2_1.jpg`,
  `${P}Exterior3_1.jpg`,
  `${P}Lobby1_1.jpg`,
];

async function firstOk(url) {
  try {
    const res = await fetch(url, { redirect: "follow", signal: AbortSignal.timeout(12000) });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "";
    return ct.includes("image") ? url : "";
  } catch {
    return "";
  }
}

for (const size of sizes) {
  for (const name of names) {
    const url = `https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`;
    const ok = await firstOk(url);
    if (ok) {
      console.log("OK", ok);
      process.exit(0);
    }
  }
}
console.log("MISS nc936");
