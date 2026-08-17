const html = await (
  await fetch("https://www.dealality.com/old-home?cb=" + Date.now(), {
    headers: { "cache-control": "no-cache" },
  })
).text();

const has = html.includes(
  "See the opportunity before selecting the relationship"
);
const start = html.indexOf('<section id="features"');
console.log({ has, start, platform: html.indexOf('id="platform-features"') });

const navFeats = [...html.matchAll(/href=(["'])(.*?)\1/g)]
  .map((m) => m[2])
  .filter((h) => h.includes("#features"));
console.log("nav", navFeats);

if (start >= 0) {
  let depth = 0;
  let i = start;
  for (; i < html.length; i++) {
    if (html.startsWith("<section", i)) depth++;
    if (html.startsWith("</section>", i)) {
      depth--;
      if (depth === 0) {
        i += 10;
        break;
      }
    }
  }
  const sec = html.slice(start, i);
  console.log("sectionLen", sec.length);
  console.log(sec.slice(0, 500));
  console.log("---end---");
  console.log(sec.slice(-300));
}
