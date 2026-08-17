/** Tiny shared JSON reader for v37C-R2 modules. */
import fs from "fs";

export function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}
