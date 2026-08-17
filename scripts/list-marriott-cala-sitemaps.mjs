const t = await fetch(
  "https://www.marriott.com/content/dam/marriott-seo/en/marriott-tng/sitemap-hotel-sitemaps.xml"
).then((r) => r.text());
const slugs = [...t.matchAll(/hotel-sitemap\/([a-z0-9-]+)-hotel-sitemap/gi)].map((m) => m[1]);
const want = new Set([
  "mexico",
  "dominican-republic",
  "jamaica",
  "panama",
  "costa-rica",
  "colombia",
  "peru",
  "brazil",
  "chile",
  "ecuador",
  "venezuela",
  "guatemala",
  "honduras",
  "el-salvador",
  "bahamas",
  "barbados",
  "trinidad-and-tobago",
  "puerto-rico",
  "argentina",
]);
for (const s of slugs) {
  if (
    want.has(s) ||
    /aruba|turks|cayman|curacao|saint|virgin|grenada|bermuda|belize|guyana|suriname|paraguay|uruguay|bolivia|haiti|dominica|antigua|lucia|kitts|vincent|maarten|bonaire/i.test(
      s
    )
  ) {
    console.log(s);
  }
}
