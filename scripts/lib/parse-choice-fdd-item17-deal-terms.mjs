/**
 * Parse Choice FDD plain text for Deal Terms (Item 17 franchise agreement summary).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Skip TOC: require newline after ITEM 17. */
export function sliceItem17FranchiseBlock(t) {
  const idx = t.search(/ITEM\s*17\s*\n[\s\S]*?RENEWAL,?\s+TERMINATION/i);
  if (idx < 0) return "";
  const from = idx + 100;
  const rel = t.slice(from).search(/\n\s*ITEM\s*18\s*\n/i);
  const end = rel >= 0 ? from + rel : Math.min(t.length, idx + 50000);
  return t.slice(idx, end);
}

export function parseInitialTermYears(chunk) {
  let m = chunk.match(/Term\s+is\s+(\d{1,2})\s+years/i);
  if (!m) m = chunk.match(/Term\s+is\s+(\d{1,2})\s+years\s+from/i);
  return m ? parseInt(m[1], 10) : null;
}

export function parseNoContractualRenewal(chunk) {
  return (
    /No\s+provision\s+for\s+renewal\s+after/i.test(chunk) ||
    /no\s+right\s+or\s+option\s+to\s+renew/i.test(chunk) ||
    /Renewal\s+or\s+extension\s+of\s*[\s\S]{0,200}?the\s+term[\s\S]{0,600}?Not\s+Applicable/i.test(chunk)
  );
}

export function readFddText(filename) {
  const dir = path.join(__dirname, "..", "..", "fixtures", "choice-fdd-text");
  const p = path.join(dir, filename);
  if (!fs.existsSync(p)) return null;
  return fs.readFileSync(p, "utf8");
}

export function parseChoiceFddDealTerms(text) {
  const ch = sliceItem17FranchiseBlock(text);
  if (!ch) return { initialYears: null, noRenewal: false };
  return {
    initialYears: parseInitialTermYears(ch),
    noRenewal: parseNoContractualRenewal(ch),
  };
}
