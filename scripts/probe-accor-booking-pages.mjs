#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS } from "../lib/accor-brand-directory-extract.js";

const urls = [
  "https://all.accor.com/booking/en/ibis/hotel/5625?destination=belo-horizonte-state-of-minas-gerais-brazil",
  "https://all.accor.com/booking/en/accor/hotels/rio-de-janeiro-state-of-rio-de-janeiro-brazil?compositions=1",
  "https://all.accor.com/booking/en/accor/hotels/belo-horizonte-state-of-minas-gerais-brazil?compositions=1",
  "https://all.accor.com/booking/en/ibis/hotels/belo-horizonte-state-of-minas-gerais-brazil?compositions=1",
];

for (const url of urls) {
  const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  const html = await res.text();
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
  console.log(url.split("?")[0].replace("https://all.accor.com/", ""), {
    status: res.status,
    len: html.length,
    hotelCodes,
    bookingCodes,
    ridCodes: ridCodes.slice(0, 10),
    hasNextData: html.includes("__NEXT_DATA__"),
  });
}
