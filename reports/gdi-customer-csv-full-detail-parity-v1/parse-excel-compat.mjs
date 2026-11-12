import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const csvPath = path.join(
  __dirname,
  "Bethesda Marriott - GDI Opportunities.csv"
);

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let i = 0;
  let inQuotes = false;
  const s = text.replace(/^\uFEFF/, "");
  while (i < s.length) {
    const c = s[i];
    if (inQuotes) {
      if (c === '"') {
        if (s[i + 1] === '"') {
          cell += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      cell += c;
      i += 1;
      continue;
    }
    if (c === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (c === ",") {
      row.push(cell);
      cell = "";
      i += 1;
      continue;
    }
    if (c === "\r") {
      i += 1;
      continue;
    }
    if (c === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      i += 1;
      continue;
    }
    cell += c;
    i += 1;
  }
  if (cell.length || row.length) {
    row.push(cell);
    rows.push(row);
  }
  return rows.filter((r) => r.some((x) => String(x || "").trim()));
}

const csv = fs.readFileSync(csvPath, "utf8");
const rows = parseCsv(csv);
const header = rows[0];
const data = rows.slice(1);
const ids = data.map((r) => r[0]);
const out = {
  bom: csv.charCodeAt(0) === 0xfeff,
  headerCols: header.length,
  dataRows: data.length,
  uniqueIds: new Set(ids).size,
  allRowsMatchHeader: data.every((r) => r.length === header.length),
  multilineCellRows: data.filter((r) =>
    r.some((c) => String(c).includes("\n"))
  ).length,
  commaContainingRows: data.filter((r) =>
    r.some((c) => String(c).includes(","))
  ).length,
  quoteContainingRows: data.filter((r) =>
    r.some((c) => String(c).includes('"'))
  ).length,
  utf8Sample: data.some((r) => /[^\x00-\x7F]/.test(r.join(""))),
  maxCellLen: Math.max(...data.flatMap((r) => r.map((c) => String(c).length))),
  formulaInjectionSafe: !data.some((r) =>
    r.some((c) => /^[=+\-@]/.test(String(c)))
  ),
  sampleId: ids[0],
};
fs.writeFileSync(
  path.join(__dirname, "excel-compat-result.json"),
  JSON.stringify(out, null, 2)
);
console.log(JSON.stringify(out, null, 2));
