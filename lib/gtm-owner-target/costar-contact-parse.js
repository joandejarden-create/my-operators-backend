import { readFileSync, existsSync } from "fs";
import { basename } from "path";
import XLSX from "xlsx";
import { rowToContactFields, contactDedupeKey } from "./contact-to-airtable.js";
import { MAP_GTM_CONTACT } from "./contact-field-map.js";

function matrixFromFile(filePath) {
  const ext = filePath.toLowerCase();
  if (ext.endsWith(".csv")) {
    const text = readFileSync(filePath, "utf8");
    const wb = XLSX.read(text, { type: "string" });
    const sheetName = wb.SheetNames.includes("ContactDataExport")
      ? "ContactDataExport"
      : wb.SheetNames[0];
    return { sheetName, matrix: XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1, defval: "" }) };
  }
  const wb = XLSX.readFile(filePath);
  const sheetName = wb.SheetNames.includes("ContactDataExport")
    ? "ContactDataExport"
    : wb.SheetNames[0];
  return { sheetName, matrix: XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1, defval: "" }) };
}

function findHeaderRowIndex(matrix) {
  for (let i = 0; i < Math.min(8, matrix.length); i++) {
    const row = matrix[i] || [];
    if (row.some((c) => String(c ?? "").trim() === "Name") && row.some((c) => String(c ?? "").trim() === "Email")) {
      return i;
    }
  }
  return 0;
}

/**
 * @param {string} filePath
 */
export function parseCostarContactFile(filePath) {
  if (!existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  const { sheetName, matrix } = matrixFromFile(filePath);
  if (!matrix.length) {
    return { fileName: basename(filePath), sheetName, headers: [], rows: [], warnings: ["Empty sheet"] };
  }

  const headerRowIndex = findHeaderRowIndex(matrix);
  const headers = (matrix[headerRowIndex] || []).map((h) => String(h ?? "").trim());
  const warnings = [];
  if (!headers.includes("Email") && !headers.includes("Name")) {
    warnings.push("Missing Name/Email columns — not a ContactDataExport file?");
  }

  const rows = [];
  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const line = matrix[r];
    if (!line || line.every((c) => String(c ?? "").trim() === "")) continue;

    const fields = rowToContactFields(headers, line);
    if (!fields.Name && !fields.Email && !fields.Company) continue;

    const key = contactDedupeKey(fields);
    fields[MAP_GTM_CONTACT.contactDedupeKey] = key;
    fields[MAP_GTM_CONTACT.sourceFile] = basename(filePath);

    rows.push({
      key,
      fields,
      sourceFile: basename(filePath),
      sourceRow: r + 1,
    });
  }

  return { fileName: basename(filePath), sheetName, headers, rows, warnings };
}

/**
 * @param {string[]} filePaths
 */
export function parseCostarContactFiles(filePaths) {
  const fileReports = [];
  const allRows = [];
  for (const filePath of filePaths) {
    const parsed = parseCostarContactFile(filePath);
    fileReports.push({
      filePath,
      fileName: parsed.fileName,
      sheetName: parsed.sheetName,
      rowCount: parsed.rows.length,
      warnings: parsed.warnings,
    });
    allRows.push(...parsed.rows);
  }
  return { fileReports, allRows };
}
