#!/usr/bin/env node
import fs from "fs";

async function get(u) {
  const r = await fetch(u, { headers: { "user-agent": "Mozilla/5.0 Chrome/122" }, redirect: "follow" });
  const b = Buffer.from(await r.arrayBuffer());
  return { status: r.status, len: b.length, type: r.headers.get("content-type") };
}

function extractLets(html) {
  return [
    ...new Set(
      [...html.matchAll(/https:\/\/letsimage\.s3[^"'\\\s]+/gi)].map((m) => m[0].replace(/&amp;/g, "&"))
    ),
  ].filter((u) => !/\.svg($|\?)|favicon|logo|lets-atlantica|v1\.png|design-sem-nome|frame-132131/i.test(u));
}

const checks = [];

// Vista Playa de Oro — property-prefixed assets
const vpo = JSON.parse(fs.readFileSync("data/operator-gallery-research/strict/vista-oro2.json", "utf8"));
const vpoFull = vpo.imgs.filter((u) => /VPOM/i.test(u) && !/-\d{2,4}x\d{2,4}/.test(u));
console.log("VPO candidates", vpoFull.slice(0, 10));

// Atlantica — non-banner hotel folder images
for (const k of ["ahi-jardins", "ahi-berrini", "ahi-faria", "ahi-congonhas", "ahi-paulista", "ahi-perdizes"]) {
  const html = fs.readFileSync(`data/operator-gallery-research/strict/${k}.html`, "utf8");
  const imgs = extractLets(html);
  console.log("\n" + k);
  imgs.slice(0, 10).forEach((u) => console.log(" ", u.slice(0, 130)));
}

// Playa — prefer Aerial / General_Resort / Exterior in path
for (const k of ["playa-ziva-cun", "playa-zilara-cun", "playa-ziva-cabo", "playa-ziva-pv", "playa-ziva-rh"]) {
  const j = JSON.parse(fs.readFileSync(`data/operator-gallery-research/strict/${k}.json`, "utf8"));
  const good = j.imgs.filter((u) =>
    /Aerial|General_Resort|exterior|Exterior|facade|Facade|panoram|resort-0|hero/i.test(u)
  );
  console.log("\n" + k, "good", good.length);
  good.slice(0, 6).forEach((u) => console.log(" ", u.slice(0, 140)));
}

// Brittain — exterior-ish
for (const k of ["br-breakers", "br-caribbean", "br-compass", "br-grande", "br-paradise", "br-ocean"]) {
  const j = JSON.parse(fs.readFileSync(`data/operator-gallery-research/strict/${k}.json`, "utf8"));
  const good = j.imgs.filter(
    (u) =>
      !/accommodation|amenit|food|games|special|golf|cornhole|water\.png|food\.png|games\.png|navigation|mobile/i.test(
        u
      ) && !/-\d{2,4}x\d{2,4}/.test(u)
  );
  console.log("\n" + k);
  console.log("  og", j.og);
  good.slice(0, 6).forEach((u) => console.log(" ", u.slice(0, 130)));
}

const verify = [
  "https://vistaplayadeoro.com/wp-content/uploads/2024/07/VPOM-HabEstandar-1.webp",
  "https://letsimage.s3.amazonaws.com/editor/atlantica/pt/hotel/ht-berrini/1732823488785-bmsc4544.jpg",
  "https://letsimage.s3.amazonaws.com/editor/atlantica/imgs/1779806727386-img_6333_4_5_6_7.jpg",
  "https://letsimage.s3.amazonaws.com/editor/atlantica/imgs/1781617308904-9.jpg",
  "https://letsimage.s3.amazonaws.com/editor/atlantica/imgs/1735915886692-eventos.jpg",
  "https://www.resortsbyhyatt.com/styled/preview//storage/media/Hyatt_Ziva_Rose_Hall/GENERAL-RESORT/Hyatt-Ziva-Rose-Hall-Aerial.Jpg",
  "https://playa-cms-assets.s3.amazonaws.com/media/Hyatt_ziva_cancun/hyatt-ziva-cancun-aerial-10.jpg",
  "https://playa-media.imgix.net/mediavalet/medialibrary-4454e06b53b64519ba3d815bb8b551e5/f9a06468388e4098872a2c253b6a7c70/f9a06468388e4098872a2c253b6a7c70/Original/Hyatt-Ziva-Puerto-Vallarta-Aerial-5.jpg",
  "https://a.storyblok.com/f/285826016720786/2000x1302/8004012767/garza-blanca-los-puerto-vallarta-property.jpg",
  "https://sensiraresorts.com/images/hero/SEN_VentaPremier_2026_Web_Hero.webp",
  "https://sensiraresorts.com/assets/CamasBalinesas-AreaPlayaFamiliar-DXI-cCqt.webp",
];

console.log("\nverify:");
for (const u of verify) {
  try {
    const r = await get(u);
    console.log(r.status, String(r.len).padStart(8), u.split("/").pop().slice(0, 70));
  } catch (e) {
    console.log("ERR", e.message);
  }
}
