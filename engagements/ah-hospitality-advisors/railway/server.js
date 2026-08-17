/**
 * Minimal static host for AH Commercial Performance Hub mockup.
 * Deploy as its own Railway service — separate from Dealality backend.
 */
import express from "express";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const port = Number(process.env.PORT) || 8080;
const staticDir = path.join(__dirname, "static");

app.disable("x-powered-by");
app.use((req, res, next) => {
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  next();
});

app.use(express.static(staticDir, { index: "index.html" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "ah-commercial-performance-hub" });
});

app.get("*", (_req, res) => {
  res.sendFile(path.join(staticDir, "index.html"));
});

app.listen(port, () => {
  console.log(`AH Commercial Performance Hub mockup listening on ${port}`);
});
