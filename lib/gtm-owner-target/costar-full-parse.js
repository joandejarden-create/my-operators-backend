import { readFileSync, existsSync } from "fs";
import { basename } from "path";
import XLSX from "xlsx";
import { rowToAirtableFields, propertyDedupeKey } from "./costar-to-airtable.js";

function matrixFromFile(filePath) {
  const ext = filePath.toLowerCase();
  if (ext.endsWith(".csv")) {
    const text = readFileSync(filePath, "utf8");
    const wb = XLSX.read(text, { type: "string" });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    return { sheetName: wb.SheetNames[0], matrix: XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" }) };
  }
  const wb = XLSX.readFile(filePath);
  const sheetName = wb.SheetNames.includes("Properties") ? "Properties" : wb.SheetNames[0];
  return { sheetName, matrix: XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1, defval: "" }) };
}

function findHeaderRowIndex(matrix) {
  for (let i = 0; i < Math.min(12, matrix.length); i++) {
    const row = matrix[i] || [];
    if (row.some((c) => String(c ?? "").trim() === "Building Name")) return i;
  }
  return 0;
}

/**
 * @param {string} filePath
 */
export function parseCostarPropertiesFile(filePath) {
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

  if (!headers.includes("Building Name")) {
    warnings.push("Missing Building Name column");
  }
  if (!headers.includes("Property ID")) {
    warnings.push("Missing Property ID column — fallback dedupe uses name+city+owner");
  }

  const rows = [];
  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const line = matrix[r];
    if (!line || line.every((c) => String(c ?? "").trim() === "")) continue;

    const fields = rowToAirtableFields(headers, line);
    if (!fields["Building Name"] && !fields["Property ID"]) continue;

    const key = propertyDedupeKey(fields);
    rows.push({
      key,
      fields,
      sourceFile: basename(filePath),
      sourceRow: r + 1,
    });
  }

  return {
    fileName: basename(filePath),
    sheetName,
    headers,
    rows,
    warnings,
  };
}

/**
 * @param {string[]} filePaths
 */
export function parseCostarPropertiesFiles(filePaths) {
  const fileReports = [];
  const allRows = [];

  for (const filePath of filePaths) {
    const parsed = parseCostarPropertiesFile(filePath);
    fileReports.push({
      filePath,
      fileName: parsed.fileName,
      rowCount: parsed.rows.length,
      warnings: parsed.warnings,
    });
    allRows.push(...parsed.rows);
  }

  return { fileReports, allRows };
}
