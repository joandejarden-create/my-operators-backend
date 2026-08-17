import { load as loadCheerio } from "cheerio";

const urls = [
  "https://www.designhotels.com/hotels/mexico/mexico-city/condesa-df/",
  "https://www.designhotels.com/hotels/mexico/baja-california-sur/san-jose-del-cabo/nest-baja/",
  "https://www.designhotels.com/hotels/mexico/mexico-city/downtown-mexico/",
];

function parse(html) {
  const $ = loadCheerio(html);
  const text = $.text();
  const rooms = text.match(/Number of rooms:\s*(\d+)/i)?.[1];
  const descCandidates = [];
  $("h2, h3, p").each((_, el) => {
    const t = $(el).text().replace(/\s+/g, " ").trim();
    if (t.length >= 80 && t.length <= 600 && !/cookie|sign in|check-in/i.test(t)) {
      descCandidates.push(t.slice(0, 120));
    }
  });
  const features = [];
  $("p").filter((_, el) => /^Hotel Features$/i.test($(el).text().trim())).each((_, el) => {
    $(el).next("ul").find("li").each((_, li) => features.push($(li).text().trim()));
  });
  const general = [];
  $("p").filter((_, el) => /^General$/i.test($(el).text().trim())).each((_, el) => {
    $(el).next("ul").find("li").each((_, li) => general.push($(li).text().replace(/\s+/g, " ").trim()));
  });
  return { rooms, descCandidates: descCandidates.slice(0, 3), features: features.slice(0, 8), general: general.slice(0, 8) };
}

for (const u of urls) {
  const t = await (await fetch(u, { headers: { "User-Agent": "Mozilla/5.0" } })).text();
  console.log("\n===", u.split("/").slice(-2, -1)[0]);
  console.log(JSON.stringify(parse(t), null, 2));
}
