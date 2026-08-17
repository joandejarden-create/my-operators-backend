import https from "https";
import fs from "fs";

https
  .get("https://varko-template.webflow.io/", (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      fs.writeFileSync("tmp-varko-home.html", d);
      const lower = d.toLowerCase();
      const idxs = [];
      let i = 0;
      while ((i = lower.indexOf("faq", i)) !== -1 && idxs.length < 20) {
        idxs.push(i);
        i += 3;
      }
      console.log({ len: d.length, faqHits: idxs.length, first: idxs[0] });
      // Extract FAQ-ish chunk
      const start = Math.max(0, (lower.indexOf("questions & answers") >= 0 ? lower.indexOf("questions & answers") : lower.indexOf("faqs")) - 500);
      const chunk = d.slice(start, start + 8000);
      fs.writeFileSync("tmp-varko-faq-chunk.html", chunk);
      console.log("--- chunk head ---");
      console.log(chunk.slice(0, 1500));
    });
  })
  .on("error", (e) => {
    console.error(e);
    process.exit(1);
  });
