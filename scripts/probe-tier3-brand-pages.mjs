#!/usr/bin/env node
const urls = [
  "https://www.wyndhamhotels.com/wyndham/puerto-rico/san-juan/wyndham-rio-mar/overview",
  "https://www.ihg.com/holidayinn/hotels/us/en/santo-domingo/sdqhi/hoteldetail",
  "https://www.hyatt.com/en-US/hotel/puerto-rico/hyatt-regency-grand-reserve/hyatt-regency-grand-reserve-puerto-rico",
  "https://all.accor.com/en/hotel/1234",
];

for (const url of urls.slice(0, 3)) {
  console.log("\n---", url);
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124" },
    });
    const html = await res.text();
    console.log("status", res.status, "len", html.length, "blocked", /access denied|captcha/i.test(html));
    console.log("ld+json", (html.match(/application\/ld\+json/gi) || []).length);
    console.log("amenit", (html.match(/amenit/gi) || []).length);
  } catch (e) {
    console.log("err", e.message);
  }
}
