/**
 * Extract headline Item 6 fees from Choice FDD plain text (fixtures/choice-fdd-text/*.txt).
 * Skips TOC by requiring a newline after "ITEM 6".
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export function sliceItem6Body(t) {
  const idx = t.search(
    /ITEM\s*6\s*\n[\s\n]*OTHER\s+FEES[\s\S]*?(?:TYPE\s+OF\s+FEE|Type\s+of\s+Fee)/i
  );
  if (idx < 0) return "";
  const from = idx + 200;
  const rel = t.slice(from).search(/\bITEM\s*7\s*\n/i);
  const i7 = rel >= 0 ? from + rel : -1;
  return t.slice(idx, i7 > idx ? i7 : idx + 25000);
}

function firstPct(label, chunk) {
  const re = new RegExp(label.replace(/\s+/g, "\\s+") + "[\\s\\S]*?([\\d.]+)\\s*%", "i");
  const m = chunk.match(re);
  return m ? parseFloat(m[1]) : null;
}

function loyaltyRange(chunk) {
  const m = chunk.match(
    /Rewards Programs?\s+Fee[\s\S]{0,900}?(\d+\.?\d*)\s*%\s*-\s*(\d+\.?\d*)\s*%/i
  );
  if (m) return [parseFloat(m[1]), parseFloat(m[2])];
  return [null, null];
}

function techPerRoom(chunk) {
  const m = chunk.match(
    /Property\s+Technology\s*&\s*Service\s+Fee[\s\S]*?\$\s*([\d.]+)\s*per\s+room\s+monthly/i
  );
  return m ? parseFloat(m[1]) : null;
}

export function parseChoiceFddItem6Fees(text) {
  const ch = sliceItem6Body(text);
  if (!ch) {
    return {
      royaltyMin: null,
      royaltyMax: null,
      marketingReservationPct: null,
      loyaltyMin: null,
      loyaltyMax: null,
      techPerRoomMonthly: null,
    };
  }
  const roy =
    firstPct("Royalty Fee", ch) ??
    firstPct("Membership Fee", ch) ??
    firstPct("Continuing License Fee", ch);
  const mr = firstPct("Marketing and Reservation", ch);
  const [loyMin, loyMax] = loyaltyRange(ch);
  const tech = techPerRoom(ch);
  return {
    royaltyMin: roy,
    royaltyMax: roy,
    marketingReservationPct: mr,
    loyaltyMin: loyMin,
    loyaltyMax: loyMax,
    techPerRoomMonthly: tech,
  };
}

export function readFddText(filename) {
  const dir = path.join(__dirname, "..", "..", "fixtures", "choice-fdd-text");
  const p = path.join(dir, filename);
  if (!fs.existsSync(p)) return null;
  return fs.readFileSync(p, "utf8");
}
