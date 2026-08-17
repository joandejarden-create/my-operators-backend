const html = await (
  await fetch("https://www.dealality.com/old-home?cb=" + Date.now())
).text();
console.log({
  hasList: html.includes("hero-globe-list"),
  hasHgItem: html.includes("hg-item-1"),
  hasRotator: html.includes('id="rotator"'),
  hasGlobeJs307: html.includes("hero-globe-bg.v202607307"),
  hasGlobeJs308: html.includes("hero-globe-bg.v202607308"),
});
const i = html.indexOf("hero-globe");
console.log(html.slice(i, i + 700));
