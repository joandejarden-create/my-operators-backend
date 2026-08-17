#!/usr/bin/env node
const code = "71034";
const info = await (
  await fetch(`https://images.bestwestern.com/bwi/brochures/${code}/propertyInfo_en.txt`, {
    headers: { "User-Agent": "Mozilla/5.0" },
  })
).json();
const first = (info.Media || []).find((m) => m.ImagePath);
console.log("sample media", first);
const paths = [
  `https://images.bestwestern.com/bwi/brochures/${code}/${first.ImagePath}`,
  `https://images.bestwestern.com/bwi/brochures/${code}/photos/${first.ImagePath}`,
  `https://images.bestwestern.com/bwi/brochures/${code}/Images/${first.ImagePath}`,
  `https://images.bestwestern.com/bwi/brochures/${code}/image/${first.ImagePath}`,
  `https://images.bestwestern.com/bimg/propertyimages/large/${code}/${first.ImagePath}`,
  `https://images.bestwestern.com/bimg/hotelImages/${code}/${first.ImagePath}`,
  `https://images.bestwestern.com/bwi/propertyImages/${code}/${first.ImagePath}`,
  `https://images.bestwestern.com/bwi/brochures/${code}/Gallery/${first.ImagePath}`,
];
for (const url of paths) {
  try {
    const res = await fetch(url, {
      method: "HEAD",
      headers: { "User-Agent": "Mozilla/5.0" },
      redirect: "follow",
    });
    console.log(res.status, res.headers.get("content-type"), url);
  } catch (err) {
    console.log("err", url, err.message);
  }
}
