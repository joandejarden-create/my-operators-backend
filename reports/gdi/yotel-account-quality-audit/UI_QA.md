# YOTEL account quality — UI QA

| Check | Result |
|---|---|
| API facing count (after restart) | **3** |
| Palexpo READY | **0** |
| CHILD ACCOUNT text | Scrubbed in UI + segment source |
| QUALIFY on Ready survivors | **CONTACT_NOW** |
| Homepage in buyer footer | Hidden for root/home URLs |
| Duplicate machine summary | Rebuilt sales description |
| SETAC Ready | YES |
| AidEx Ready | YES |
| CHI Ready | YES |

## Browser

1. Hard-refresh YOTEL GDI after server restart.
2. Expect **3** Ready cards only.
3. No Palexpo venue Ready tiles.
4. No CHILD ACCOUNT in meta line.
5. Action pill shows CONTACT (not QUALIFY) on survivors.
