import fs from "fs";

const headers = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html",
};
const res = await fetch("https://www.hilton.com/en/locations/curio-collection/", { headers });
const html = await res.text();
const data = JSON.parse(html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);
const pageData = data.props.pageProps.pageData;

console.log("ctyhocnList length:", pageData.ctyhocnList?.length);
console.log("hotels in page:", pageData.hotelSummaryOptions?.hotels?.length);
console.log("location:", JSON.stringify(pageData.location, null, 2).slice(0, 500));

const hotel = pageData.hotelSummaryOptions.hotels[0];
console.log("\nFirst hotel full keys:", Object.keys(hotel).join(", "));
console.log("\nFirst hotel sample (truncated):");
const sample = { ...hotel };
if (sample.images) sample.images = `[${sample.images.length} images]`;
if (sample.facilityOverview) sample.facilityOverview = sample.facilityOverview?.slice?.(0, 200) || sample.facilityOverview;
console.log(JSON.stringify(sample, null, 2).slice(0, 3500));

// dehydratedState may hold react-query cache with more hotels
const ds = data.props.pageProps.dehydratedState;
if (ds?.queries) {
  console.log("\ndehydratedState queries:", ds.queries.length);
  for (const q of ds.queries) {
    const key = JSON.stringify(q.queryKey).slice(0, 120);
    const d = q.state?.data;
    let info = typeof d;
    if (Array.isArray(d)) info += ` len=${d.length}`;
    else if (d && typeof d === "object") info += ` keys=${Object.keys(d).slice(0, 8).join(",")}`;
    console.log(" ", key, "->", info);
  }
}

fs.writeFileSync("reports/_hilton-curio-locations-pagedata-keys.json", JSON.stringify({
  pageDataKeys: Object.keys(pageData),
  ctyhocnListLen: pageData.ctyhocnList?.length,
  hotelsLen: pageData.hotelSummaryOptions?.hotels?.length,
  firstHotelKeys: Object.keys(hotel),
  contactInfoKeys: hotel.contactInfo ? Object.keys(hotel.contactInfo) : [],
  addressKeys: hotel.address ? Object.keys(hotel.address) : [],
  localizationKeys: hotel.localization ? Object.keys(hotel.localization) : [],
}, null, 2));
