const props = [
  ["Orlando", "https://www.choicehotels.com/florida/orlando/woodspring-hotels/flf21", "flf21"],
  ["Charlotte", "https://www.choicehotels.com/north-carolina/charlotte/woodspring-hotels/ncb10", "ncb10"],
  ["Raleigh", "https://www.choicehotels.com/north-carolina/raleigh/woodspring-hotels/nc936", "nc936"],
];
const ua =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function extractImages(html) {
  const imgs = [
    ...html.matchAll(/https?:\/\/[^\s"'<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\s"'<>]*)?/gi),
  ].map((m) => m[0]);
  return [...new Set(imgs)];
}

for (const [name, url, code] of props) {
  const res = await fetch(url, {
    headers: { "User-Agent": ua, Accept: "text/html,application/xhtml+xml" },
    redirect: "follow",
  });
  const html = await res.text();
  const imgs = extractImages(html);
  const hotelLike = imgs.filter(
    (u) =>
      /hotel|property|exterior|room|suite|gallery|dam\//i.test(u) &&
      !/logo|icon|sprite|choice-logo/i.test(u)
  );
  console.log("---", name, res.status, "len", html.length, "denied", /access denied/i.test(html));
  console.log("hotelLike", hotelLike.slice(0, 10));
  console.log("all", imgs.slice(0, 10));
}
