import fs from "fs";
const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/4f8e581d-7d58-4414-b1d2-dab7fe1bbfca.txt",
  "utf8"
);
const j = JSON.parse(raw);
const pages = j.result.pages;
const hits = pages.filter((x) =>
  /signup|access|request|pricing|for-brand|for-operator|member/i.test(
    `${x.slug} ${x.title} ${x.publishedPath || ""}`
  )
);
console.log(
  hits.map((x) => `${x.publishedPath || "/" + x.slug} | ${x.title}`).join("\n")
);
console.log("total", pages.length);
