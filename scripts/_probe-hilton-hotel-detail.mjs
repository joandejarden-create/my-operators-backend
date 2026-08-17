const HOTEL_URL = "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/";
const INFO_URL = HOTEL_URL + "hotel-info/";

const attempts = [
  { label: "bare fetch", headers: {} },
  {
    label: "browser UA",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
  },
  {
    label: "browser UA + referer",
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      Accept: "text/html",
      Referer: "https://www.hilton.com/en/locations/costa-rica/curio-collection/",
    },
  },
];

function scan(label, status, html) {
  console.log(`\n=== ${label} [${status}] ${html.length} bytes ===`);
  if (status !== 200) {
    console.log(html.slice(0, 200).replace(/\s+/g, " "));
    return;
  }
  const checks = [
    "checkIn",
    "checkOut",
    "check-in",
    "parking",
    "Pets allowed",
    "TripAdvisor",
    "facilityOverview",
    "nearby",
    "airport",
    "description",
    "accessible",
    "__NEXT_DATA__",
    "application/ld+json",
    "graphql",
  ];
  for (const p of checks) {
    const n = (html.match(new RegExp(p, "gi")) || []).length;
    if (n) console.log(`  ${p}: ${n}`);
  }
  const next = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  if (!next) return;
  console.log(`  __NEXT_DATA__: ${next[1].length} bytes`);
  const data = JSON.parse(next[1]);
  const pp = data.props?.pageProps || {};
  console.log("  pageProps keys:", Object.keys(pp).slice(0, 15).join(", "));
  const pageData = pp.pageData || pp.hotelData || pp.data;
  if (pageData && typeof pageData === "object") {
    console.log("  pageData keys:", Object.keys(pageData).slice(0, 25).join(", "));
  }
  // search for policy-ish strings in JSON
  const blob = next[1];
  for (const needle of ["checkInTime", "checkOutTime", "parking", "pet", "nearby", "tripAdvisor", "overview"]) {
    if (blob.toLowerCase().includes(needle.toLowerCase())) console.log(`  JSON contains: ${needle}`);
  }
}

for (const url of [HOTEL_URL, INFO_URL]) {
  console.log("\n########", url);
  for (const a of attempts) {
    const res = await fetch(url, { headers: a.headers, redirect: "follow" });
    const html = await res.text();
    scan(a.label, res.status, html);
    if (res.status === 200) break;
  }
}

// Compare: country locations JSON already has for same hotel
const locRes = await fetch("https://www.hilton.com/en/locations/costa-rica/curio-collection/", {
  headers: attempts[1].headers,
});
const locData = JSON.parse((await locRes.text()).match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);
const gran = locData.props.pageProps.pageData.hotelSummaryOptions.hotels.find((h) =>
  h.name.includes("Gran Hotel Costa Rica")
);
console.log("\n=== Locations-page JSON fields (same hotel) ===");
console.log(Object.keys(gran).join(", "));
console.log("Has checkIn/checkOut/description/nearby in JSON?", {
  checkIn: JSON.stringify(gran).includes("checkIn"),
  description: JSON.stringify(gran).includes("description"),
  nearby: JSON.stringify(gran).includes("nearby"),
  tripAdvisor: JSON.stringify(gran).includes("tripAdvisor"),
});
