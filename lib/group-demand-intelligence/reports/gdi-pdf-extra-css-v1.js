/**
 * GDI PDF-specific print styles (extends dealality-report-system-v1).
 */
export const GDI_PDF_EXTRA_CSS = `
.gdi-pdf-report { padding: 0; }
.gdi-pdf-cover.bas-cover-page {
  min-height: 240mm;
  break-after: page;
  page-break-after: always;
  position: relative;
  color: #fff;
  background: #080f25;
  display: flex;
  align-items: flex-end;
  padding: 28mm 16mm 24mm;
}
.gdi-pdf-cover .bas-cover-kicker {
  letter-spacing: 0.14em;
  font-size: 9pt;
  text-transform: uppercase;
  opacity: 0.85;
  margin: 0 0 10pt;
}
.gdi-pdf-cover .bas-cover-title {
  font-size: 28pt;
  line-height: 1.15;
  margin: 0 0 8pt;
  font-weight: 700;
}
.gdi-pdf-cover .bas-cover-sub {
  font-size: 12pt;
  opacity: 0.9;
  margin: 0 0 18pt;
}
.gdi-pdf-cover .bas-cover-meta {
  font-size: 9.5pt;
  opacity: 0.75;
  margin: 0;
}
.gdi-pdf-cover .bas-cover-logo-img {
  height: 28px;
  width: auto;
  margin-bottom: 28pt;
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
  line-height: 1.25;
}
.gdi-pdf-card__org {
  font-size: 9pt;
  color: #4a5568;
  margin: 0 0 8pt;
}
.gdi-pdf-dl { margin: 0; }
.gdi-pdf-dl > div { margin: 0 0 6pt; }
.gdi-pdf-dl dt {
  font-size: 8pt;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #6c72ff;
  margin: 0;
}
.gdi-pdf-dl dd {
  margin: 1pt 0 0;
  font-size: 9.5pt;
}
.gdi-pdf-pri {
  display: inline-block;
  font-size: 8pt;
  font-weight: 700;
  letter-spacing: 0.04em;
  padding: 2pt 6pt;
  border-radius: 999px;
  border: 1px solid #c5cee8;
  background: #f4f6fc;
}
.gdi-pdf-pri--high { background: #eef0ff; border-color: #6c72ff; color: #2a2f7a; }
.gdi-pdf-pri--med { background: #fff7e8; border-color: #fdb52a; color: #7a5a00; }
.gdi-pdf-pri--watch { background: #f4f6fc; color: #4a5568; }
.gdi-pdf-muted { color: #4a5568; font-size: 8.5pt; }
.gdi-pdf-list { margin: 0 0 10pt; padding-left: 16pt; }
.gdi-pdf-list li { margin: 0 0 4pt; }
.gdi-pdf-table { font-size: 8.5pt; width: 100%; border-collapse: collapse; margin: 0 0 14pt; }
.gdi-pdf-table th, .gdi-pdf-table td {
  border: 1px solid #d9e1fa;
  padding: 5pt 6pt;
  vertical-align: top;
  text-align: left;
}
.gdi-pdf-table th { background: #f4f6fc; }
.drs-kpi-band--3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
`;
