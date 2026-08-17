const urls = [
  "https://www.hilton.com/en/locations/curio-collection/",
  "https://www.hilton.com/en/hotels/sjogqq-gran-hotel-costa-rica/",
  "https://www.hilton.com/en/hotels/sjogqq-gran-hotel-costa-rica/hotel-info/",
];

const headers = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
};

function scan(html, label) {
  console.log(`\n=== ${label} (${html.length} bytes) ===`);
  const checks = [
    "__NEXT_DATA__",
    "application/ld+json",
    "graphql",
    "hotelSummary",
    "amenit",
    "checkIn",
    "checkOut",
    "telephone",
    "og:image",
    "propCode",
    "ctyhocn",
    "Hotel",
    "address",
  ];
  for (const p of checks) {
    const n = (html.match(new RegExp(p, "gi")) || []).length;
    if (n) console.log(`  ${p}: ${n}`);
  }
  const ldMatches = [...html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)];
  console.log(`  JSON-LD blocks: ${ldMatches.length}`);
  for (const m of ldMatches.slice(0, 2)) {
    try {
      const j = JSON.parse(m[1]);
      console.log("  JSON-LD @type:", j["@type"] || j["@graph"]?.map((x) => x["@type"]).join(", "));
      console.log("  JSON-LD keys:", Object.keys(j).slice(0, 15).join(", "));
    } catch {
      console.log("  JSON-LD parse failed, snippet:", m[1].slice(0, 200));
    }
  }
  const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  if (next) {
    console.log(`  __NEXT_DATA__ bytes: ${next[1].length}`);
    try {
      const j = JSON.parse(next[1]);
      console.log("  __NEXT_DATA__ top keys:", Object.keys(j).join(", "));
      const props = j.props?.pageProps;
      if (props) console.log("  pageProps keys:", Object.keys(props).slice(0, 20).join(", "));
    } catch (e) {
      console.log("  __NEXT_DATA__ parse error:", e.message);
    }
  }
  const og = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i);
  if (og) console.log("  og:image:", og[1].slice(0, 100));
}

for (const url of urls) {
  const res = await fetch(url, { headers, redirect: "follow" });
  const html = await res.text();
  scan(html, `${url} [${res.status}]`);
}
