# BROWSER QA

Runtime: localhost:8080 after fix (admin auth stubbed for automation).

| Step | Result |
|------|--------|
| AI Demand Admin → AI Visibility Audit → Radisson Blu Toronto Downtown | Row visible |
| Primary **Download PDF** (`data-act=download-pdf`) | Clicked |
| Network | `/pdf?download=1&channel=current` |
| Alert | **None** |
| Blob download | 279308 bytes |
| Filename | `Dealality - AI Visibility Audit & Action Plan - Radisson Blu Toronto Downtown - NOVEMBER2026.pdf` |
| Content | Hotel + November 2026 + 33.2 / 38.1 / 19.0 + Choice Privileges #1 of 6 |

TORONTO DOWNLOAD PDF WORKS: **YES**  
ALERT REMOVED: **YES**  
PDF OPENS/DOWNLOADS: **YES**  
CORRECT TORONTO AVA PDF: **YES**
