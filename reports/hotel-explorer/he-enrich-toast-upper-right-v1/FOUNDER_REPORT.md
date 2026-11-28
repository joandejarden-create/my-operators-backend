# Permanent Upper-Right Loading Toast Fix

**Classification:** `HE_ENRICH_TOAST_UPPER_RIGHT_CERTIFIED`  
**Verified from:** clean worktree `wt-he-toast-upper-right-v1` serving on port **8097** (not a CSS overlay on dirty 8080).

## Gap

Canonical main still positioned `.hci-enrich-loading-toast` at bottom-center (`left:50%; bottom:28px`). Working Surfe overlay used upper-right (`top:20px; right:20px`) matching shared `.bdd-toast`.

## Fix

- CSS: upper-right fixed toast, slide-in/out aligned with `.bdd-toast`, `z-index:14050` above HE modal (`10000`), `pointer-events:none`
- JS: re-parent toast to `document.body` if ever found inside a table/row
- Gate: `npm run test:he-enrich-toast-upper-right-v1 -- --browser`

## Unchanged

Airtable, Surfe/Findymail workflows, contact records, table layout.
