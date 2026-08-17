import { readFileSync, existsSync, readdirSync } from "fs";
import { join, basename } from "path";
import XLSX from "xlsx";
import {
  mapCostarHeaderToCanonical,
  parseNumber,
  parseYear,
  buildSourceRowKey,
  normalizeOwnerKey,
} from "./normalize.js";

function findHeaderRowIndex(matrix) {
  for (let i = 0; i < Math.min(12, matrix.length); i++) {
    const row = matrix[i] || [];
    let hits = 0;
    for (const cell of row) {
      if (mapCostarHeaderToCanonical(String(cell ?? "").trim())) hits++;
    }
    if (hits >= 2) return i;
  }
  return 0;
}

function matrixFromFile(filePath) {
  const ext = filePath.toLowerCase();
  if (ext.endsWith(".csv")) {
    const text = readFileSync(filePath, "utf8");
    const wb = XLSX.read(text, { type: "string" });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  }
  const wb = XLSX.readFile(filePath);
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
}

function cellValue(line, index) {
  if (index == null || index < 0) return "";
  return String(line[index] ?? "").trim();
}

/**
 * @param {string} filePath
 * @returns {{ fileName: string, headers: string[], headerMap: Record<string, number>, rows: object[], warnings: string[] }}
 */
export function parseCostarExportFile(filePath) {
  if (!existsSync(filePath)) {
    throw new Error(`CoStar export file not found: ${filePath}`);
  }

  const matrix = matrixFromFile(filePath);
  if (!matrix.length) {
    throw new Error(`CoStar export file is empty: ${filePath}`);
  }

  const headerRowIndex = findHeaderRowIndex(matrix);
  const headerRow = (matrix[headerRowIndex] || []).map((h) => String(h ?? "").trim());
  const headerMap = {};
  headerRow.forEach((header, index) => {
    const canon = mapCostarHeaderToCanonical(header);
    if (canon && headerMap[canon] == null) headerMap[canon] = index;
  });

  const warnings = [];
  if (headerMap.trueOwner == null) {
    warnings.push('Missing "True Owner" column — cannot build owner rollups.');
  }
  if (headerMap.buildingName == null) {
    warnings.push('Missing "Building Name" column — property records may be incomplete.');
  }

  const rows = [];
  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const line = matrix[r];
    if (!line || line.every((c) => String(c ?? "").trim() === "")) continue;

    const trueOwner = cellValue(line, headerMap.trueOwner);
    const buildingName = cellValue(line, headerMap.buildingName);
    if (!trueOwner && !buildingName) continue;

    const row = {
      trueOwner,
      buildingName,
      costarPropertyId: cellValue(line, headerMap.costarPropertyId),
      submarket: cellValue(line, headerMap.submarket),
      market: cellValue(line, headerMap.market),
      country: cellValue(line, headerMap.country),
      city: cellValue(line, headerMap.city),
      zipCode: cellValue(line, headerMap.zipCode),
      starRating: parseNumber(cellValue(line, headerMap.starRating)),
      rbaGlaSf: parseNumber(cellValue(line, headerMap.rbaGlaSf)),
      yearBuilt: parseYear(cellValue(line, headerMap.yearBuilt)),
      yearRenovated: parseYear(cellValue(line, headerMap.yearRenovated)),
      builtRenovText: cellValue(line, headerMap.builtRenovText),
      propertyType: cellValue(line, headerMap.propertyType) || "Hospitality",
      brandAffiliation: cellValue(line, headerMap.brandAffiliation),
      sourceFile: basename(filePath),
      sourceRowNumber: r + 1,
    };
    row.sourceRowKey = buildSourceRowKey(row);
    rows.push(row);
  }

  return {
    fileName: basename(filePath),
    headers: headerRow,
    headerMap,
    rows,
    warnings,
  };
}

/**
 * @param {string} dir
 * @returns {object[]}
 */
export function parseCostarExportDirectory(dir) {
  if (!existsSync(dir)) return [];
  const files = readdirSync(dir).filter((f) => /\.(csv|xlsx|xls)$/i.test(f));
  const allRows = [];
  const fileReports = [];

  for (const file of files) {
    const report = parseCostarExportFile(join(dir, file));
    fileReports.push({
      fileName: report.fileName,
      rowCount: report.rows.length,
      warnings: report.warnings,
      detectedColumns: Object.keys(report.headerMap),
    });
    allRows.push(...report.rows);
  }

  return { rows: allRows, fileReports };
}

/**
 * @param {object[]} rows
 */
export function groupRowsByOwner(rows) {
  const groups = new Map();

  for (const row of rows) {
    const ownerName = String(row.trueOwner || "").trim();
    if (!ownerName) continue;
    const key = normalizeOwnerKey(ownerName);
    if (!groups.has(key)) {
      groups.set(key, { ownerName, ownerKey: key, properties: [] });
    }
    groups.get(key).properties.push(row);
  }

  return [...groups.values()];
}
