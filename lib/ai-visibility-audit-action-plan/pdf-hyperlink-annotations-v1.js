/**
 * Extract clickable PDF hyperlink annotations (/URI) from PDF bytes.
 * Do not rely on visible text alone.
 */
import fs from "node:fs";

export const PDF_HYPERLINK_ANNOTATIONS_CONTRACT =
  "AVA_PDF_HYPERLINK_ANNOTATIONS_V1";

function decodePdfLiteral(raw) {
  return String(raw || "")
    .replace(/\\([nrtbf()\\])/g, (_, ch) => {
      const map = {
        n: "\n",
        r: "\r",
        t: "\t",
        b: "\b",
        f: "\f",
        "(": "(",
        ")": ")",
        "\\": "\\",
      };
      return map[ch] || ch;
    })
    .replace(/\\([0-7]{1,3})/g, (_, oct) =>
      String.fromCharCode(parseInt(oct, 8))
    );
}

function decodePdfHex(hex) {
  const clean = String(hex || "").replace(/\s+/g, "");
  const bytes = [];
  for (let i = 0; i < clean.length; i += 2) {
    bytes.push(parseInt(clean.slice(i, i + 2), 16));
  }
  return Buffer.from(bytes).toString("utf8");
}

export function extractPdfUriAnnotationsFromBuffer(buf) {
  const text = Buffer.isBuffer(buf) ? buf.toString("latin1") : String(buf || "");
  const uris = [];
  const lit = /\/URI\s*\((?:\\.|[^\\)])*\)/g;
  let m;
  while ((m = lit.exec(text))) {
    const inner = m[0].replace(/^\/URI\s*\(/, "").replace(/\)$/, "");
    uris.push(decodePdfLiteral(inner));
  }
  const hex = /\/URI\s*<([0-9A-Fa-f\s]+)>/g;
  while ((m = hex.exec(text))) {
    uris.push(decodePdfHex(m[1]));
  }
  const httpOnly = uris
    .map((u) => String(u || "").trim())
    .filter((u) => /^https?:\/\//i.test(u));
  const unique = [...new Set(httpOnly)];
  return {
    contract: PDF_HYPERLINK_ANNOTATIONS_CONTRACT,
    totalAnnotations: httpOnly.length,
    uniqueUrls: unique,
    urls: httpOnly,
  };
}

export function extractPdfUriAnnotationsFromFile(absPath) {
  if (!absPath || !fs.existsSync(absPath)) {
    return {
      contract: PDF_HYPERLINK_ANNOTATIONS_CONTRACT,
      totalAnnotations: 0,
      uniqueUrls: [],
      urls: [],
      missingFile: true,
      absPath: absPath || null,
    };
  }
  const buf = fs.readFileSync(absPath);
  return {
    ...extractPdfUriAnnotationsFromBuffer(buf),
    missingFile: false,
    absPath,
    size: buf.length,
  };
}

export function pdfContainsUrl(bufOrPath, needle) {
  const n = String(needle || "");
  if (!n) return false;
  let buf;
  if (Buffer.isBuffer(bufOrPath)) buf = bufOrPath;
  else if (typeof bufOrPath === "string" && fs.existsSync(bufOrPath)) {
    buf = fs.readFileSync(bufOrPath);
  } else return false;
  const extracted = extractPdfUriAnnotationsFromBuffer(buf);
  if (extracted.uniqueUrls.some((u) => u.includes(n) || n.includes(u))) {
    return true;
  }
  return buf.toString("latin1").includes(n);
}
