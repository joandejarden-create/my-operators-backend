#!/usr/bin/env node
import { ACCOR_FETCH_HEADERS } from "../lib/accor-brand-directory-extract.js";

const pages = [
  "https://ibis.accor.com/en/ibis-styles.html",
  "https://ibis.accor.com/en/destinations/south-america.html",
  "https://ibis.accor.com/en/destinations.html",
];

for (const url of pages) {
  const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  const html = await res.text();
  const codes = [
    ...new Set([...html.matchAll(/\/hotel\/([0-9A-Za-z]{3,6})/gi)].map((m) => m[1].toUpperCase())),
  ];
  const itemList = /"@type"\s*:\s*"ItemList"/i.test(html);
  const cala = ["Brazil", "Mexico", "Chile", "Argentina", "Colombia", "Peru"].filter((c) =>
    html.includes(c)
  );
  console.log(url.replace("https://ibis.accor.com/", ""), {
    status: res.status,
    len: html.length,
    hotelCodes: codes.length,
    sample: codes.slice(0, 8),
    itemList,
    cala,
  });
}
