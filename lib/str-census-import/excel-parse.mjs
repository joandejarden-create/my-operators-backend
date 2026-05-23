import { join } from "path";
import XLSX from "xlsx";
import {
  normalizeStrId,
  mapExcelHeaderToCanonical,
  EXPECTED_EXCEL_CANONICAL,
} from "./normalize.mjs";

/**
 * STR exports use a grouped header row then column names (row 0 = groups, row 1 = STR ID…).
 */
export function findDataHeaderRowIndex(matrix) {
  for (let i = 0; i < Math.min(10, matrix.length); i++) {
    const row = matrix[i] || [];
    let matchCount = 0;
    for (const cell of row) {
      if (mapExcelHeaderToCanonical(String(cell ?? "").trim())) matchCount++;
    }
    if (matchCount >= 3) return i;
  }
  return 0;
}

/**
 * @param {object} sheet XLSX sheet
 * @param {string} fileName
 * @param {string} sheetName
 */
export function parseStrExcelSheet(sheet, fileName, sheetName) {
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  if (!matrix.length) {
    return { headers: [], rows: [], headerMap: {}, headerRowIndex: 0 };
  }

  const headerRowIndex = findDataHeaderRowIndex(matrix);
  const headerRow = (matrix[headerRowIndex] || []).map((h) => String(h ?? "").trim());
  const headerMap = {};
  headerRow.forEach((h, i) => {
    const canon = mapExcelHeaderToCanonical(h);
    if (canon) headerMap[canon] = i;
  });

  const rows = [];
  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const line = matrix[r];
    if (!line || line.every((c) => String(c ?? "").trim() === "")) continue;

    const row = {
      sourceFile: fileName,
      sheetName,
      rowNumber: r + 1,
    };
    for (const canon of EXPECTED_EXCEL_CANONICAL) {
      const idx = headerMap[canon];
      row[canon] = idx != null ? String(line[idx] ?? "").trim() : "";
    }
    if (row.strId) row.strId = normalizeStrId(row.strId);
    rows.push(row);
  }

  return { headers: headerRow, rows, headerMap, headerRowIndex };
}

/**
 * @param {string} dir
 * @param {(dir: string) => string[]} listExcelFiles
 */
export function readStrExcelDirectory(dir, listExcelFiles) {
  const files = listExcelFiles(dir);
  const inventoryRows = [];
  const allRows = [];

  for (const file of files) {
    const path = join(dir, file);
    const wb = XLSX.readFile(path, { cellDates: false });
    for (const sheetName of wb.SheetNames) {
      const parsed = parseStrExcelSheet(wb.Sheets[sheetName], file, sheetName);
      const missingCols = EXPECTED_EXCEL_CANONICAL.filter((c) => parsed.headerMap[c] == null);
      inventoryRows.push({
        fileName: file,
        sheetName,
        headerRowIndex: parsed.headerRowIndex + 1,
        rowCount: parsed.rows.length,
        columnsFound: parsed.headers.join(" | "),
        missingExpectedColumns: missingCols.join("; ") || "",
        hasStrId: parsed.headerMap.strId != null ? "yes" : "no",
        hasCity: parsed.headerMap.city != null ? "yes" : "no",
        hasHotelName: parsed.headerMap.hotelName != null ? "yes" : "no",
        hasCountry: parsed.headerMap.country != null ? "yes" : "no",
        hasStrMarket: parsed.headerMap.strMarket != null ? "yes" : "no",
        hasStrSubmarket: parsed.headerMap.strSubmarket != null ? "yes" : "no",
      });
      allRows.push(...parsed.rows);
    }
  }

  return { files, inventoryRows, allRows };
}
