import fs from "fs";

const html = fs.readFileSync("tmp-old-home-live.html", "utf8");
const start = html.indexOf('<link rel="preconnect" href="https://fonts.googleapis.com">');
const end = html.indexOf("</style>", html.indexOf("oh-fsw-grad")) + "</style>".length;
if (start < 0 || end < start) throw new Error(`bad bounds ${start} ${end}`);

let head = html.slice(start, end);
const oldCss = "6a6873a23015b8ca0ab40e1c_dealality-old-home-dark.v20260728n.css";
const newCss = "6a689b2648471629f7f717af_dealality-old-home-dark.v20260728o.css";
if (!head.includes(oldCss)) throw new Error("old css not found");
head = head.split(oldCss).join(newCss);

fs.writeFileSync("tmp-old-home-freeform-head-patched.txt", head);
fs.writeFileSync(
  "tmp-old-home-freeform-head-patched.json",
  JSON.stringify({ content: head })
);
console.log(JSON.stringify({ start, end, length: head.length, hasNew: head.includes(newCss) }));
