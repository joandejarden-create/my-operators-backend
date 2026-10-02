/**
 * GDI PDF body-only print styles.
 * Cover geometry is owned by adp-monthly-review-report-v1.css (.adp-mr-pdf-host)
 * — same as ADP monthly review. Do not add GDI-specific cover rules here.
 */
export const GDI_PDF_EXTRA_CSS = `
.gdi-pdf-report-body {
  padding: 0;
  margin: 0;
  background: #fff;
}

.gdi-pdf-section { break-before: auto; }
.gdi-pdf-lede { color: #4a5568; margin: 0 0 12pt; }
.gdi-pdf-disclaimer {
  font-size: 8.5pt;
  color: #4a5568;
  margin: 8pt 0 4pt;
  font-style: italic;
}
.gdi-pdf-note { font-size: 8.5pt; color: #4a5568; margin: 0 0 10pt; }
.gdi-pdf-rev-block { margin-top: 12pt; }
.gdi-pdf-card {
  border: 1px solid #d9e1fa;
  border-radius: 8px;
  padding: 10pt 12pt;
  margin: 0 0 10pt;
  background: #fff;
}
.gdi-pdf-card--compact { padding: 8pt 10pt; }
.gdi-pdf-card--watch { background: #f4f6fc; }
.gdi-pdf-avoid-break {
  break-inside: avoid;
  page-break-inside: avoid;
}
.gdi-pdf-card__head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 4pt;
}
.gdi-pdf-card__num {
  font-size: 9pt;
  color: #6c72ff;
  font-weight: 600;
}
.gdi-pdf-card__title {
  font-size: 11.5pt;
  margin: 0 0 4pt;
  color: #0f172a;
  font-weight: 650;
}
.gdi-pdf-card__org {
  font-size: 9pt;
  color: #4a5568;
  margin: 0 0 8pt;
}
.gdi-pdf-dl {
  display: grid;
  gap: 6pt;
  margin: 0;
}
.gdi-pdf-dl dt {
  font-size: 8pt;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: #6b7280;
  margin: 0;
}
.gdi-pdf-dl dd {
  margin: 1pt 0 0;
  font-size: 9.5pt;
  color: #1f2937;
}
.gdi-pdf-pri {
  display: inline-block;
  font-size: 8pt;
  font-weight: 700;
  letter-spacing: 0.06em;
  padding: 2pt 6pt;
  border-radius: 999px;
}
.gdi-pdf-pri--high { background: #fee2e2; color: #991b1b; }
.gdi-pdf-pri--med { background: #ffedd5; color: #9a3412; }
.gdi-pdf-pri--watch { background: #e0e7ff; color: #3730a3; }
.gdi-pdf-muted { font-size: 8.5pt; color: #6b7280; }
.gdi-pdf-list { margin: 0 0 10pt; padding-left: 16pt; }
.gdi-pdf-list li { margin: 0 0 4pt; }
`;
