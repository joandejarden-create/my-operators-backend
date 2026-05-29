/** Quick parallel probe for standard hoteldam exterior filenames. */
const gaps = process.argv.slice(2);
if (!gaps.length) {
  console.error("Usage: node scripts/probe-hoteldam-quick.mjs do032 cb012 ...");
  process.exit(1);
}

function candidates(pid) {
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const p = pid.toLowerCase();
  const sizes = ["1280", "2048", "480"];
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
    `Exterior1.JPG`,
    `${p}exterior2_1.jpg`,
    `${P}Exterior5_1.JPG`,
  ];
  const out = [];
  for (const size of sizes) {
    for (const name of names) {
      out.push(`https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`);
    }
  }
  return out;
}

async function firstOk(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: AbortSignal.timeout(12000),
    });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("image")) return "";
    return url;
  } catch {
    return "";
  }
}

async function probe(pid) {
  for (const url of candidates(pid)) {
    const ok = await firstOk(url);
    if (ok) return { pid, url: ok };
  }
  return { pid, url: "" };
}

const results = await Promise.all(gaps.map((p) => probe(p.toLowerCase())));
for (const r of results) {
  console.log(r.url ? `OK ${r.pid} ${r.url}` : `MISS ${r.pid}`);
}
