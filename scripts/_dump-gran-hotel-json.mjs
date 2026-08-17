import fs from "fs";

const headers = { "User-Agent": "Mozilla/5.0 Chrome/124", Accept: "text/html" };
const res = await fetch("https://www.hilton.com/en/locations/costa-rica/curio-collection/", { headers });
const data = JSON.parse((await res.text()).match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);
const gran = data.props.pageProps.pageData.hotelSummaryOptions.hotels.find((h) =>
  h.name.includes("Gran Hotel Costa Rica")
);
fs.writeFileSync("reports/_hilton-gran-hotel-costa-rica-sample.json", JSON.stringify(gran, null, 2));
console.log(JSON.stringify(gran, null, 2));
