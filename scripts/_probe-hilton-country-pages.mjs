const headers = { "User-Agent": "Mozilla/5.0 Chrome/124", Accept: "text/html" };
const urls = [
  "https://www.hilton.com/en/locations/costa-rica/curio-collection/",
  "https://www.hilton.com/en/locations/colombia/curio-collection/",
  "https://www.hilton.com/en/locations/argentina/curio-collection/",
];
for (const url of urls) {
  const res = await fetch(url, { headers });
  const html = await res.text();
  const data = JSON.parse(html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1]);
  const hotels = data.props?.pageProps?.pageData?.hotelSummaryOptions?.hotels || [];
  console.log("\n", url);
  console.log(" hotels:", hotels.length);
  for (const h of hotels) {
    console.log(" -", h.name, "| open:", h.display?.open, "| phone:", h.contactInfo?.phoneNumber);
  }
}
