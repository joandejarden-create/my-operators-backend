import https from "https";

https
  .get("https://www.dealality.com/old-home", { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      console.log(JSON.stringify({ status: res.statusCode, len: d.length }, null, 0));
      for (const k of [
        "080F25",
        "080f25",
        "old-home-dark",
        "FILE:tmp",
        "Inter+Tight",
        "dc-premium",
        "background:#fff",
        "6C72FF",
        "6c72ff",
      ]) {
        console.log(k, d.includes(k));
      }
      const headEnd = d.indexOf("</head>");
      console.log("--- head snippet ---");
      console.log(d.slice(Math.max(0, headEnd - 1200), headEnd + 7));
    });
  })
  .on("error", (e) => {
    console.error(e);
    process.exit(1);
  });
