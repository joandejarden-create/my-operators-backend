/**
 * Map production URL paths to on-disk public/ files.
 * Mirrors server.js:
 *   app.use("/app", express.static(path.join(__dirname, "public", "app")))
 *   app.use(express.static(path.join(__dirname, "public")))
 */
import fs from "node:fs";
import path from "node:path";

export function resolvePublicPathForRoute(routePath, repoRoot) {
  const raw = String(routePath || "").trim();
  if (!raw.startsWith("/")) return null;
  if (raw === "/app" || raw.startsWith("/app/")) {
    const rel = raw === "/app" ? "" : raw.slice("/app/".length);
    return path.join(repoRoot, "public", "app", rel);
  }
  return path.join(repoRoot, "public", raw.replace(/^\//, ""));
}

export function routeHasPublicFile(routePath, repoRoot) {
  const abs = resolvePublicPathForRoute(routePath, repoRoot);
  return Boolean(abs && fs.existsSync(abs) && fs.statSync(abs).isFile());
}
