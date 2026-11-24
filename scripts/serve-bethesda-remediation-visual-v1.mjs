#!/usr/bin/env node
import http from "http";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const visualRoot = path.join(
  ROOT,
  "reports/gdi-bethesda-fqo-remediation-v1/visual",
);
const port = Number(process.env.PORT || 8093);

const server = http.createServer((req, res) => {
  let name = decodeURIComponent((req.url || "/").split("?")[0]);
  if (name === "/" || name === "") name = "/dual-pane-gold-vs-candidate.html";
  const f = path.normalize(path.join(visualRoot, name.replace(/^[/\\]+/, "")));
  if (!f.startsWith(visualRoot) || !fs.existsSync(f)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end(`nf ${f}`);
    return;
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  fs.createReadStream(f).pipe(res);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`serving ${visualRoot} on http://127.0.0.1:${port}/`);
});
