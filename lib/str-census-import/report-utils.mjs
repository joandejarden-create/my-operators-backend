import { mkdirSync, writeFileSync } from "fs";
import { dirname } from "path";
import { csvEscape } from "./normalize.mjs";

export function ensureDirFor(filePath) {
  mkdirSync(dirname(filePath), { recursive: true });
}

export function writeCsv(filePath, rows, headers) {
  ensureDirFor(filePath);
  const cols = headers || (rows[0] ? Object.keys(rows[0]) : []);
  const lines = [
    cols.join(","),
    ...rows.map((r) => cols.map((h) => csvEscape(r[h])).join(",")),
  ];
  writeFileSync(filePath, lines.join("\n") + "\n", "utf8");
}

export function writeJson(filePath, data) {
  ensureDirFor(filePath);
  writeFileSync(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}
