import https from "https";

const url = "https://www.dealality.com/old-home?cb=" + Date.now();
https
  .get(url, (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      const links = [...d.matchAll(/href="([^"]*freeform-head[^"]*)"/g)].map((m) => m[1]);
      const scripts = [
        ...d.matchAll(/src="([^"]*(?:problem-v2|freeform-head|footer-oh)[^"]*)"/g),
      ].map((m) => m[1]);
      console.log("links", links);
      console.log("scripts", scripts);
      console.log("has w16", d.includes("w16.css"));
      console.log("has w20", d.includes("w20.css"));
      console.log("has problem-v2c", d.includes("problem-v2.v20260729c.js"));
    });
  })
  .on("error", (e) => {
    console.error(e);
    process.exit(1);
  });
