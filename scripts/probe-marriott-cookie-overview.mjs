#!/usr/bin/env node
const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const expUrl = `https://www.marriott.com/en-us/hotels/${SLUG}/experiences/`;
const ovUrl = `https://www.marriott.com/en-us/hotels/${SLUG}/overview/`;

const baseHeaders = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
};

let cookie = "";
async function fetchWithCookies(url, extra = {}) {
  const res = await fetch(url, {
    headers: {
      ...baseHeaders,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      ...(cookie ? { Cookie: cookie } : {}),
      ...extra,
    },
    redirect: "follow",
  });
  const set = res.headers.getSetCookie?.() || [];
  if (set.length) {
    const parts = set.map((c) => c.split(";")[0]);
    cookie = cookie ? `${cookie}; ${parts.join("; ")}` : parts.join("; ");
  }
  return { res, text: await res.text() };
}

console.log("1) Warm up experiences");
const e1 = await fetchWithCookies(expUrl);
console.log(" status", e1.res.status, "cookies", cookie.length);

console.log("\n2) Overview same-origin referer");
const o1 = await fetchWithCookies(ovUrl, {
  Referer: expUrl,
  "Sec-Fetch-Site": "same-origin",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-User": "?1",
  "Upgrade-Insecure-Requests": "1",
});
console.log(" status", o1.res.status, "len", o1.text.length, "denied", /access denied/i.test(o1.text));
console.log(" overview text", /15-minute drive/i.test(o1.text));
console.log(" amenities", /Free high-speed internet/i.test(o1.text));

if (o1.res.status === 200) {
  const next = o1.text.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  console.log("__NEXT_DATA__", !!next, next?.[1]?.length);
}

console.log("\n3) Session endpoint then overview");
cookie = "";
const s = await fetch("https://www.marriott.com/mi/phoenix-gateway/session", {
  headers: {
    ...baseHeaders,
    Accept: "application/json",
    Origin: "https://www.marriott.com",
    Referer: "https://www.marriott.com/default.mi",
  },
});
const set = s.headers.getSetCookie?.() || [];
cookie = set.map((c) => c.split(";")[0]).join("; ");
console.log(" session", s.status, "cookies", cookie.length);
const o2 = await fetchWithCookies(ovUrl, { Referer: expUrl });
console.log(" overview2", o2.res.status, /15-minute drive/i.test(o2.text));
