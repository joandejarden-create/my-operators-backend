import fs from "fs";
import crypto from "crypto";
import https from "https";

const url = `https://www.dealality.com/old-home?cb=${Date.now()}`;
https
  .get(url, { headers: { "user-agent": "dealality-verify/1.0" } }, (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      const scripts = [...d.matchAll(/src="([^"]+)"/g)]
        .map((m) => m[1])
        .filter((u) => /old-home|freeform|quote|testimonial|problem|storyboard/i.test(u));
      console.log(scripts.join("\n"));
      console.log("---");
      console.log("hasQuote", /quote-tiles/i.test(d));
      console.log("hasProblemV2", /problem-v2/i.test(d));
      console.log("hasStoryboard", /problem-storyboard/i.test(d));
    });
  })
  .on("error", (e) => {
    console.error(e);
    process.exit(1);
  });
