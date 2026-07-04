/**
 * Load source text from URL, local file, or PDF (server-side only).
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { resolveReferenceRoot } from "./airtable-source.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PY = path.resolve(__dirname, "..", "..", "scripts", "lib", "extract-pdf-text.py");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

export async function fetchPublicUrlText(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "DealalityPartnerIntelligence/1.0 (+https://dealality.com)",
      Accept: "text/html,application/xhtml+xml",
    },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`URL fetch failed (${res.status}): ${url}`);
  const html = await res.text();
  const $ = cheerio.load(html);
  $("script, style, nav, footer, noscript").remove();
  const title = nz($("title").first().text());
  const metaDesc = nz($('meta[name="description"]').attr("content"));
  const bodyText = nz($("body").text()).replace(/\s+/g, " ").trim();
  return {
    kind: "html",
    url,
    title,
    metaDescription: metaDesc,
    text: [title, metaDesc, bodyText].filter(Boolean).join("\n\n"),
  };
}

export function readLocalSourceText(localFilePath) {
  const root = resolveReferenceRoot();
  const relative = nz(localFilePath).replace(/^[/\\]+/, "");
  const abs = path.resolve(root, relative);
  if (!abs.startsWith(path.resolve(root))) {
    throw new Error("Local file path escapes reference root.");
  }
  if (!fs.existsSync(abs)) throw new Error(`Local file not found: ${relative}`);

  const ext = path.extname(abs).toLowerCase();
  if (ext === ".pdf") {
    const r = spawnSync("python", [PY, abs], {
      encoding: "utf8",
      maxBuffer: 40 * 1024 * 1024,
      env: { ...process.env, PYTHONIOENCODING: "utf-8" },
    });
    if (r.error) throw r.error;
    if (r.status !== 0) throw new Error(r.stderr || `PDF extract failed: ${relative}`);
    return { kind: "pdf", path: relative, text: r.stdout };
  }

  const text = fs.readFileSync(abs, "utf8");
  return { kind: ext.slice(1) || "file", path: relative, text };
}

/**
 * @param {{ sourceUrl?: string, localFilePath?: string, sourceTitle?: string }} source
 */
export async function loadSourceDocumentText(source) {
  const localPath = nz(source.localFilePath);
  const url = nz(source.sourceUrl);

  if (localPath) {
    try {
      const doc = readLocalSourceText(localPath);
      return { ...doc, metaDescription: "" };
    } catch (localErr) {
      if (/^https?:\/\//i.test(url)) {
        return fetchPublicUrlText(url);
      }
      throw localErr;
    }
  }
  if (/^https?:\/\//i.test(url)) {
    return fetchPublicUrlText(url);
  }
  throw new Error("Source has no readable URL or local file path.");
}

export function excerptAround(text, index, radius = 120) {
  const start = Math.max(0, index - radius);
  const end = Math.min(text.length, index + radius);
  let snippet = text.slice(start, end).replace(/\s+/g, " ").trim();
  if (start > 0) snippet = "…" + snippet;
  if (end < text.length) snippet = snippet + "…";
  return snippet;
}
