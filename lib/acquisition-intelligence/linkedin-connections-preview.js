/**
 * Import preview + validation summary for LinkedIn Connections CSV.
 */

/**
 * @param {{ ok: boolean, rows?: object[], invalidRows?: object[], warnings?: string[], fileName?: string, metadataRowCount?: number, error?: string, message?: string }} parsed
 */
export function buildLinkedInConnectionsPreview(parsed) {
  if (!parsed?.ok) {
    return {
      ok: false,
      validation: {
        pass: false,
        failedChecks: [parsed?.error || "parse_failed"],
      },
      error: parsed?.error || "parse_failed",
      message: parsed?.message || "Could not parse LinkedIn Connections CSV.",
      fileName: parsed?.fileName || "",
      stats: null,
    };
  }

  const rows = parsed.rows || [];
  const uniqueRows = rows.filter((r) => !r.duplicateOfRow);
  const duplicateRows = rows.filter((r) => r.duplicateOfRow);
  const connectedDates = uniqueRows
    .map((r) => r.connectedOn)
    .filter(Boolean)
    .sort();

  const failedChecks = [];
  if (!uniqueRows.length) failedChecks.push("no_valid_connections");

  const stats = {
    fileName: parsed.fileName,
    metadataRowsBeforeHeader: parsed.metadataRowCount || 0,
    connectionsDetected: uniqueRows.length,
    rawDataRows: rows.length,
    recordsWithCompany: uniqueRows.filter((r) => r.company).length,
    recordsWithPosition: uniqueRows.filter((r) => r.position).length,
    recordsWithLinkedInUrl: uniqueRows.filter((r) => r.linkedInUrl).length,
    recordsWithEmail: uniqueRows.filter((r) => r.email).length,
    earliestConnection: connectedDates[0] || null,
    latestConnection: connectedDates[connectedDates.length - 1] || null,
    potentialDuplicates: duplicateRows.length,
    invalidRows: (parsed.invalidRows || []).length,
    malformedConnectedOn: uniqueRows.filter((r) => r.connectedOnInvalid).length,
    nameCollisionsWithoutLinkedIn: countNameCollisions(uniqueRows),
  };

  return {
    ok: true,
    validation: {
      pass: failedChecks.length === 0,
      failedChecks,
    },
    fileName: parsed.fileName,
    headers: parsed.headers,
    warnings: parsed.warnings || [],
    stats,
    sampleRows: uniqueRows.slice(0, 8).map(summarizeRow),
    invalidRowSamples: (parsed.invalidRows || []).slice(0, 10),
    duplicateSamples: duplicateRows.slice(0, 10).map((r) => ({
      rowNumber: r.rowNumber,
      duplicateOfRow: r.duplicateOfRow,
      displayName: r.displayName,
      identityKey: r.identityKey,
    })),
  };
}

function summarizeRow(r) {
  return {
    rowNumber: r.rowNumber,
    displayName: r.displayName,
    company: r.company || "",
    position: r.position || "",
    linkedInUrl: r.linkedInUrl || "",
    hasEmail: Boolean(r.email),
    connectedOn: r.connectedOn || null,
  };
}

function countNameCollisions(rows) {
  const byName = new Map();
  for (const r of rows) {
    if (r.linkedInUrl) continue;
    const key = String(r.displayName || "")
      .trim()
      .toLowerCase();
    if (!key) continue;
    byName.set(key, (byName.get(key) || 0) + 1);
  }
  let collisions = 0;
  for (const count of byName.values()) {
    if (count > 1) collisions += count;
  }
  return collisions;
}

/**
 * Reject preview that failed validation before import.
 * @param {ReturnType<typeof buildLinkedInConnectionsPreview>} preview
 */
export function assertPreviewImportable(preview) {
  if (!preview?.ok || !preview.validation?.pass) {
    return {
      ok: false,
      error: "preview_not_importable",
      message: "Import blocked — preview validation failed.",
      failedChecks: preview?.validation?.failedChecks || ["unknown"],
    };
  }
  return { ok: true };
}
