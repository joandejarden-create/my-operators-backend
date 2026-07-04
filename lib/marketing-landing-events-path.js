import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_LOG_FILE = path.join(
  __dirname,
  "..",
  "data",
  "marketing-landing-events.jsonl"
);

/**
 * Resolve append-only landing analytics log path.
 * Railway: mount a volume at /data and set LANDING_ANALYTICS_LOG_FILE=/data/marketing-landing-events.jsonl
 */
export function getLandingEventsLogFile() {
  const fromFile = (process.env.LANDING_ANALYTICS_LOG_FILE || "").trim();
  if (fromFile) return path.resolve(fromFile);

  const fromDir = (process.env.LANDING_ANALYTICS_LOG_DIR || "").trim();
  if (fromDir) {
    return path.join(path.resolve(fromDir), "marketing-landing-events.jsonl");
  }

  return DEFAULT_LOG_FILE;
}

export function ensureLandingEventsLogDir(logFile) {
  const dir = path.dirname(logFile);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

export function getLandingEventsStorageMeta() {
  const logFile = getLandingEventsLogFile();
  const persistent = Boolean(
    process.env.LANDING_ANALYTICS_LOG_FILE ||
      process.env.LANDING_ANALYTICS_LOG_DIR ||
      logFile.replace(/\\/g, "/").startsWith("/data/")
  );

  let lineCount = 0;
  let bytes = 0;
  let exists = false;

  if (fs.existsSync(logFile)) {
    exists = true;
    const raw = fs.readFileSync(logFile, "utf8");
    bytes = Buffer.byteLength(raw, "utf8");
    lineCount = raw.split(/\r?\n/).filter(Boolean).length;
  }

  return {
    logFile: logFile.replace(/\\/g, "/"),
    exists,
    lineCount,
    bytes,
    persistent,
    retentionNote: persistent
      ? "Events are stored on persistent disk and survive redeploys."
      : "Events use ephemeral disk — history is lost on Railway redeploy unless you mount a volume (see docs).",
  };
}
