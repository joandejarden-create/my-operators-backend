#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS } from "../lib/accor-brand-directory-extract.js";
import { accorPropertyIdFromBookingUrl } from "../lib/accor-booking-url.js";

const urls = [
  "https://all.accor.com/booking/en/ibis/hotels/bogota-bogota-colombia?compositions=1",
  "https://all.accor.com/a/en/destination/hotels-bogota-c57.html",
  "https://all.accor.com/a/en/destination/city/hotels-bogota-colombia-c57.html",
  "https://all.accor.com/booking/en/accor/hotels/bogota-bogota-colombia?compositions=1",
];

const bookingUrls = [
  "https://all.accor.com/booking/en/ibis/hotel/B544?destination=lavras-state-of-minas-gerais-brazil",
  "https://all.accor.com/booking/en/ibis/hotel/B429?destination=bogota-bogota-colombia",
  "https://all.accor.com/booking/en/ibis/hotel/7318?destination=bogota-bogota-colombia",
];

function scanHtml(label, html) {
  const hotelCodes = [
    ...new Set([...html.matchAll(/\/hotel\/([0-9A-Za-z]{3,6})/gi)].map((m) => m[1].toUpperCase())),
  ];
  const bookingCodes = [
    ...new Set(
      [...html.matchAll(/\/booking\/[^/]+\/(?:ibis\/)?hotel\/([0-9A-Za-z]{3,6})/gi)].map((m) =>
        m[1].toUpperCase()
      )
    ),
  ];
  const ridCodes = [
    ...new Set([...html.matchAll(/"rid"\s*:\s*"([A-Z0-9]{3,6})"/gi)].map((m) => m[1])),
  ];
  const itemList = /"@type"\s*:\s*"ItemList"/i.test(html);
  const apiHints = [
    ...new Set(
      [...html.matchAll(/https?:\/\/[^"'\s]+(?:api|graphql|search|availability)[^"'\s]*/gi)].map(
        (m) => m[0].slice(0, 120)
      )
    ),
  ].slice(0, 8);
  console.log(label, {
    len: html.length,
    hotelCodes: hotelCodes.length,
    bookingCodes: bookingCodes.length,
    sample: [...new Set([...hotelCodes, ...bookingCodes])].slice(0, 15),
    itemList,
    ridCodes: ridCodes.slice(0, 10),
    apiHints,
  });
}

console.log("=== City / destination probes ===\n");
for (const url of urls) {
  const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  const html = await res.text();
  scanHtml(`${res.status} ${url.replace("https://all.accor.com/", "")}`, html);
}

console.log("\n=== Booking URL code extraction ===\n");
for (const url of bookingUrls) {
  console.log(accorPropertyIdFromBookingUrl(url), url.split("?")[0]);
}
