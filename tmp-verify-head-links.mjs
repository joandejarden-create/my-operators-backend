import fs from "fs";

const html = fs.readFileSync("tmp-old-home-verify.html", "utf8");
const links = [...html.matchAll(/href="([^"]*freeform[^"]*)"/g)].map((m) => m[1]);
const scripts = [...html.matchAll(/ohmodulestabfixw16[^"']*/g)].map((m) => m[0]);
console.log("freeform links", links);
console.log("fix scripts", scripts);

// Find head end
const headEnd = html.indexOf("</head>");
console.log("head snippet freeform mentions:");
const head = html.slice(0, headEnd);
console.log(
  [...head.matchAll(/freeform-head[^"' ]+/g)].map((m) => m[0])
);
console.log(
  [...head.matchAll(/ohmodulestabfixw16[^"' ]+/g)].map((m) => m[0])
);

// Simulate activate conflict: does body still have broken activate?
console.log(
  "broken activate pattern",
  /panel2\.removeAttribute\("hidden"\);panel1\.setAttribute\("hidden"/g.test(
    html
  )
);
console.log(
  "has aria-hidden setPanel in inline body",
  /setAttribute\("aria-hidden"/g.test(html) &&
    html.includes('var panel2=document.getElementById("modules-panel-platform")')
);
