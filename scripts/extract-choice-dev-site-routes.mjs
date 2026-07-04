/**
 * Parse route list from downloaded choicehotelsdevelopment.com homepage HTML.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const html = fs.readFileSync(
  path.join(ROOT, "fixtures", "choice-dev-site-home.html"),
  "utf8"
);
const m = html.match(/LWR\.define\('@app\/routes',\s*\[\],\s*function\(\)\s*\{\s*return\s*(\[[\s\S]*?\]);\s*\}\)/);
if (!m) {
  console.error("routes not found");
  process.exit(1);
}
const routes = JSON.parse(m[1]);
const publicRoutes = routes
  .filter((r) => r.isPublic && r.path && !r.path.includes(":"))
  .map((r) => ({ label: r.label, path: r.path, devName: r.devName }))
  .sort((a, b) => a.path.localeCompare(b.path));
fs.writeFileSync(
  path.join(ROOT, "fixtures", "choice-dev-site-routes.json"),
  JSON.stringify(publicRoutes, null, 2),
  "utf8"
);
console.log(`Wrote ${publicRoutes.length} public static routes`);
