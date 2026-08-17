/**
 * Load source text from URL, local file, or PDF (server-side only).
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { resolveLocalSourceAbsolutePath } from "./reference-material-paths.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PY = path.resolve(__dirname, "..", "..", "scripts", "lib", "extract-pdf-text.py");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

const HTML_NOISE_SELECTORS = "script, style, nav, footer, noscript, svg";

/**
 * Parse HTML into plain text for extraction (URL fetch + local .html/.htm).
 * @param {string} html
 * @returns {{ kind: "html", title: string, metaDescription: string, text: string }}
 */
export function parseHtmlDocument(html) {
  const $ = cheerio.load(html);
  $(HTML_NOISE_SELECTORS).remove();
  const title = nz($("title").first().text());
  const metaDesc = nz($('meta[name="description"]').attr("content"));
  let bodyText = nz($("body").text());
  bodyText = bodyText.replace(/\s+/g, " ").trim();
  // Drop stray tag-like fragments if any leaked through text nodes
  bodyText = bodyText.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
  const text = [title, metaDesc, bodyText].filter(Boolean).join("\n\n");
  return {
    kind: "html",
    title,
    metaDescription: metaDesc,
    text,
  };
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
  const parsed = parseHtmlDocument(html);
  return {
    ...parsed,
    url,
  };
}

export function readLocalSourceText(localFilePath, opts = {}) {
  const { absolutePath, relative, resolvedRoot, resolvedRootKind } = resolveLocalSourceAbsolutePath(
    localFilePath,
    opts
  );

  const ext = path.extname(absolutePath).toLowerCase();
  if (ext === ".pdf") {
    const r = spawnSync("python", [PY, absolutePath], {
      encoding: "utf8",
      maxBuffer: 40 * 1024 * 1024,
      env: { ...process.env, PYTHONIOENCODING: "utf-8" },
    });
    if (r.error) throw r.error;
    if (r.status !== 0) throw new Error(r.stderr || `PDF extract failed: ${relative}`);
    return { kind: "pdf", path: relative, text: r.stdout, resolvedRoot, resolvedRootKind };
  }

  const raw = fs.readFileSync(absolutePath, "utf8");
  if (ext === ".html" || ext === ".htm") {
    const parsed = parseHtmlDocument(raw);
    return {
      kind: parsed.kind,
      path: relative,
      title: parsed.title,
      metaDescription: parsed.metaDescription,
      text: parsed.text,
      resolvedRoot,
      resolvedRootKind,
    };
  }

  return {
    kind: ext.slice(1) || "file",
    path: relative,
    text: raw,
    resolvedRoot,
    resolvedRootKind,
  };
}

/**
 * @param {{ sourceUrl?: string, localFilePath?: string, sourceTitle?: string }} source
 */
export async function loadSourceDocumentText(source) {
  const localPath = nz(source.localFilePath);
  const url = nz(source.sourceUrl);

  if (localPath) {
    try {
      return readLocalSourceText(localPath);
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
