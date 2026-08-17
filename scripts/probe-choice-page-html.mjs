#!/usr/bin/env node
import { writeFileSync } from "fs";
const url = process.argv[2] || "https://www.choicehotels.com/puerto-rico/levittown/comfort-inn-hotels/pr006";
const res = await fetch(url, {
  headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0" },
});
const html = await res.text();
writeFileSync("reports/choice-page-probe.html", html);
console.log("status", res.status, "len", html.length);
console.log("has __NEXT", /__NEXT_DATA__/.test(html));
console.log("has hoteldam", /hoteldam/i.test(html));
console.log("has amenity", /amenit/i.test(html));
const m = html.match(/__NEXT_DATA__[^>]*>([\s\S]*?)<\/script/i);
if (m) {
  try {
    const j = JSON.parse(m[1]);
    console.log("next keys", Object.keys(j));
    const s = JSON.stringify(j).slice(0, 2000);
    console.log(s);
  } catch (e) {
    console.log("next parse fail");
  }
}
