/**
 * GDI PDF body-only print styles (pages 2+).
 * Cover + document chrome owned by adp-monthly-review-report-v1.css
 * (.adp-mr-pdf-host) — same shell as ADP. Do not add @page / cover geometry here.
 */
export const GDI_PDF_EXTRA_CSS = `
/* Body family — align to ADP monthly review visual system */
.gdi-pdf-report-body.adp-mr-report-body,
.gdi-pdf-report-body {
  padding: 0;
  margin: 0;
  background: #fff;
  color: #101935;
}

.gdi-pdf-section {
  break-before: auto;
}

.gdi-pdf-section .drs-h2,
.gdi-pdf-section h2.drs-h2 {
  margin: 0 0 10pt;
  padding: 0 0 6pt;
  font-size: 13pt;
  font-weight: 700;
  letter-spacing: 0.01em;
  color: #101935;
  border-bottom: 2px solid #1a2a5c;
}

.gdi-pdf-section .drs-h3 {
  margin: 12pt 0 6pt;
  font-size: 10pt;
  font-weight: 700;
  color: #1a2a5c;
  text-transform: none;
  letter-spacing: 0;
}

.gdi-pdf-lede {
  color: #4a5568;
  margin: 0 0 12pt;
  font-size: 9pt;
  line-height: 1.4;
}

.gdi-pdf-disclaimer {
  font-size: 8.5pt;
  color: #4a5568;
  margin: 8pt 0 4pt;
  font-style: italic;
}

.gdi-pdf-note {
  font-size: 8.5pt;
  color: #4a5568;
  margin: 0 0 10pt;
}

.gdi-pdf-rev-block {
  margin-top: 12pt;
}

/* ADP-like callout / card treatment */
.gdi-pdf-card {
  border: none;
  border-radius: 0;
  border-top: 2px solid #1a2a5c;
  padding: 8pt 9pt;
  margin: 0 0 10pt;
  background: #f5f7ff;
  break-inside: avoid;
  page-break-inside: avoid;
}

.gdi-pdf-card--compact {
  padding: 7pt 9pt;
}

.gdi-pdf-card--watch {
  background: #f7f8fb;
  border-top-color: #4a5568;
}

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
  font-size: 8pt;
  color: #4a5568;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.gdi-pdf-card__title {
  font-size: 10pt;
  margin: 0 0 4pt;
  color: #101935;
  font-weight: 700;
  line-height: 1.25;
}

.gdi-pdf-card__org {
  font-size: 8.5pt;
  color: #4a5568;
  margin: 0 0 8pt;
}

.gdi-pdf-dl {
  display: grid;
  gap: 6pt;
  margin: 0;
}

.gdi-pdf-dl dt {
  font-size: 7.5pt;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #4a5568;
  margin: 0;
  font-weight: 700;
}

.gdi-pdf-dl dd {
  margin: 1pt 0 0;
  font-size: 9pt;
  line-height: 1.35;
  color: #1a202c;
}

/* Priority chips — ADP pill language, not rounded candy */
.gdi-pdf-pri {
  display: inline-block;
  font-size: 7pt;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 2pt 6pt;
  border-radius: 2px;
}

.gdi-pdf-pri--high {
  background: #fee2e2;
  color: #991b1b;
}

.gdi-pdf-pri--med {
  background: #fff8eb;
  color: #9a6b00;
}

.gdi-pdf-pri--watch {
  background: #eef1f8;
  color: #3730a3;
}

.gdi-pdf-muted {
  font-size: 8pt;
  color: #6b7280;
}

.gdi-pdf-list {
  margin: 0 0 10pt;
  padding-left: 16pt;
  font-size: 9pt;
  line-height: 1.4;
}

.gdi-pdf-list li {
  margin: 0 0 4pt;
}

/* Tables — same pale system as ADP .drs-table */
.gdi-pdf-table.drs-table,
.gdi-pdf-report-body .drs-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 8.5pt;
  margin: 0 0 12pt;
}

.gdi-pdf-report-body .drs-table th {
  background: #eef1f8;
  color: #1a2a5c;
  font-weight: 700;
  text-align: left;
  padding: 5pt 6pt;
  border-bottom: 1px solid #d9e1fa;
}

.gdi-pdf-report-body .drs-table td {
  padding: 5pt 6pt;
  border-bottom: 1px solid #e8ecf6;
  color: #1a202c;
  vertical-align: top;
}
`;
