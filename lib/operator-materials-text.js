/**
 * Display formatting for Operator Materials file titles.
 */

const FILE_TITLE_ACRONYMS = {
  cala: "CALA",
  fdd: "FDD",
  faq: "FAQ",
  pdf: "PDF",
  zip: "ZIP",
  doc: "DOC",
};

/**
 * @param {unknown} raw
 * @returns {string}
 */
export function toProperCaseFileTitle(raw) {
  const s = raw == null ? "" : String(raw).trim();
  if (!s) return s;
  let base = s;
  let ext = "";
  const m = s.match(/^(.+?)(\.[a-z0-9]{2,8})$/i);
  if (m) {
    base = m[1];
    ext = m[2].toLowerCase();
  }
  const single = base.replace(/[-_+.]+/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
  if (single === "dummy" || single === "sample") {
    return "Sample Operator Document" + ext;
  }
  base = base.replace(/[-_+.]+/g, " ").replace(/\s+/g, " ").trim();
  if (!base) return s;
  const out = base.split(" ").map((w) => {
    if (!w) return w;
    if (w === "&") return "&";
    const low = w.toLowerCase();
    if (FILE_TITLE_ACRONYMS[low]) return FILE_TITLE_ACRONYMS[low];
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  });
  const joined = out.join(" ");
  return ext ? joined + ext : joined;
}
