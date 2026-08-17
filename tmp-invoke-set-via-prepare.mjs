import fs from "fs";
const a = JSON.parse(fs.readFileSync("tmp-old-home-head-set-args.json", "utf8"));
const c = a.actions[0].set_page_freeform_code.content;
fs.writeFileSync("tmp-callmcp-ready.json", JSON.stringify({
  server: "user-webflow",
  toolName: "data_scripts_tool",
  description: "Set Old Home HEAD with Benefits CSS",
  arguments: a,
}));
console.log(JSON.stringify({ ready: true, contentLen: c.length, argsBytes: Buffer.byteLength(JSON.stringify(a)) }));
