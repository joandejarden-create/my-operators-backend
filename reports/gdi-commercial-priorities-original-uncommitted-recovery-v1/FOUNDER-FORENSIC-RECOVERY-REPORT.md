# Forensic Recovery — Original Uncommitted Hotel Commercial Priorities Tab

**Date:** 2026-11-12  
**Hotel validation unchanged:** Bethesda Marriott `recLuxvwwxID7U2B8` (19 approved opportunities untouched)  
**Current main tip preserved:** `e02a8965b` (includes PR #216) — no destructive git ops

---

## 1. Recovery sources accessible

| Source | Accessible? |
|---|---|
| Local git worktrees (`git worktree list`) | Yes — 60+ |
| Git reflog / stash (read-only) | Yes |
| Cursor User History (`%APPDATA%\Cursor\User\History`) | Yes — 745 entry dirs |
| VS Code User History | **No** — `%APPDATA%\Code\User\History` missing |
| Cursor agent transcripts | Yes |
| Cursor AgentStores | Yes (scanned; no HCP app payload) |
| Authorized backups under `C:\Dev\dealality\authorized-backups` | Yes |
| Railway lean stage folders | Yes |
| Windows Previous Versions / File History UI | Not used (no automated API); filesystem copies found instead |

## 2. Sources actually searched

Searched all of the above except VS Code History (absent) and OS File History UI automation.

Prior PR #216 report incorrectly concluded the app “never existed” because it only searched **committed** Git objects and the primary checkout. The original lived as **staged-but-uncommitted** files in a secondary worktree, plus a CSS twin in Railway stage folders.

## 3. Likely original development branch / session

| Item | Value |
|---|---|
| Worktree | `C:\Dev\dealality\my-operators-backend-gdi-universal-need-date` |
| Branch | `feat/gdi-universal-need-date-intelligence-v1` @ `fb7086b8b` |
| Git state of HCP app | **Staged (`A`) in index, not in HEAD** — never committed / never pushed |
| Index blob (app) | `820f82fa5f36713ae7463f604641fe7081882149` |
| Index blob (html) | `c514a1622dfc1395e2ec85ac62c7a3ba5432fa37` |
| File timestamps | App/HTML **2026-11-02 10:30**; CSS **2026-11-02 10:27** |
| CSS twin | `C:\Dev\dealality\_railway-lean-stage-gdi-spice\public\css\hotel-commercial-priorities.css` (byte-identical to recovery-v1 stage) |
| JS twin | Same SHA256 as need-date worktree app |

Conversation corroboration: earlier GDI Full E2E chat noted “Unrelated untracked commercial-priorities portal files left out of this PR.”

## 4. Was the original uncommitted source recovered?

**Yes — complete original UI source recovered (JS + HTML + CSS + intake shell).**

This is **actual recovered code**, not another contract reconstruction.

## 5. Exact recovery source and timestamp

| Artifact | Path | Timestamp | SHA256 |
|---|---|---|---|
| App JS | need-date WT `public/js/group-demand-intelligence/hotel-commercial-priorities-app.js` | 2026-11-02T10:30:20+01:00 | `3BE01213885C…` |
| HTML shell | need-date WT `public/hotel-commercial-priorities.html` | 2026-11-02T10:30:21+01:00 | `1FDC02CAA652…` |
| CSS | Railway lean stage `public/css/hotel-commercial-priorities.css` | 2026-11-02T10:27:24+01:00 | `D679A5EE69D5…` |
| Intake shell CSS | Railway lean stage `dealality-intake-form-shell.css` | 2026-10-30 | (copied) |

Immutable evidence copy:  
`reports/gdi-commercial-priorities-original-uncommitted-recovery-v1/evidence/canonical-original-v3/`

Manifest: `00-recovery-manifest.json`

## 6. Screenshots / layout evidence

- Original HTML declares: **Hotel Commercial Priorities V3 — single scrollable page (My Deals intake shell)**; “Former 8-step wizard sections remain fully accessible on one page.”
- Seven continuous sections: Revenue Priorities · Need Dates · Preferred Business · Target Demand Segments · Local Market Intelligence · Hotel Fit & Constraints · Hotel Team.
- Selection-first chip UI (`hcp-chip`), sticky save, month calendar tiles + drawer with weekday/weekend/any + peak-rooms override.
- Live browser screenshot not captured in this pass (no forced browser session); layout is fully determined by recovered CSS + section markup. Prior PR #213 screenshot remains at `reports/gdi-restore-hotel-need-times-priorities-tab-v1/tab2-hotel-need-times-priorities.png` (calendar-era Tab 2, not this HCP V3).

## 7. Comparison vs PR #216 reconstruction

| Capability | Original uncommitted V3 | PR #216 reconstruction | Restored now |
|---|---|---|---|
| 7-section scrollable intake layout | Yes | No (fieldset form) | **Yes (original)** |
| Chip-based selection UI | Yes | Partial (checkboxes) | **Yes (original)** |
| Need Dates month tiles + drawer | Yes (integrated) | Calendar from need-dates app | **Yes (original)** |
| Revenue priorities | Yes | Yes | Yes |
| Peak rooms / booking / meeting / stay | Yes | Yes | Yes |
| Day preference weekdays/weekends/both | Yes | Yes | Yes |
| Specific Mon–Sun days | No | Yes | **Yes (kept from #216)** |
| 14 demand segments + mode | Yes | Yes | Yes |
| Excluded / lower-priority segments | No | Yes | **Yes (kept from #216 as additive chips)** |
| Local demand generators + known accounts | Yes | Yes (audiences) | **Yes (original)** |
| Business to avoid / hotel considerations | Yes | Yes | Yes |
| Hotel Team role chips | Yes | No | **Yes (original)** |
| Restriction free-text notes | Limited | Yes | Original profile path |
| Mount / keep-alive Tab 2 API (#215) | N/A (standalone page) | Yes | **Preserved** |
| Supervisor soft boosts (#215/#216) | Server-side | Server-side | **Unchanged** |

## 8. Missing functionality identified (before this restore)

PR #216 approximated the contract but **missed the original product surface**:

- Continuous 7-section My Deals intake composition
- Native `hcp-*` chip language and sticky save
- Integrated commercial Need Dates drawer (month-level revenue need / priority / days / peak override)
- Hotel Team role selectors
- Local Market “known accounts” suggestion chips
- Intake-shell visual system (`dealality-intake-form-shell.css`)

PR #216 additions worth keeping: specific Mon–Sun days; keep-alive mount; supervisor ranking (backend).

## 9. Changes implemented

1. **Preserved** current PR #216 Tab 2 JS/CSS under `preserved-current/` before overwrite.
2. **Copied** canonical original JS/HTML/CSS into evidence.
3. **Adapted** original `hotel-commercial-priorities-app.js` into mountable `DealalityGdiNeedTimesPrioritiesTab` (PR #215 keep-alive API).
4. **Installed** original `hotel-commercial-priorities.css` + `dealality-intake-form-shell.css`.
5. **Wired** `group-demand-intelligence.html` stylesheets.
6. **Retained** PR #216 specific-days chips as an additive Preferred Business row.
7. **Did not** publish, run paid research, or touch Bethesda’s 19 approved opportunities / Tab 1 Opportunities.

## 10. Regression and performance results

| Check | Result |
|---|---|
| `node --check` Tab 2 JS | Pass |
| `npm run test:gdi-hotel-need-times-priorities-tab-v1` | Pass |
| Tab 1 Opportunities code path | Untouched |
| Paid research / customer publication | Not triggered |
| Destructive git (`clean` / `reset --hard` / GC) | Not used |
| Fast tab keep-alive mount API | Preserved |

---

## Honesty classification

| Component | Classification |
|---|---|
| HCP V3 app JS + HTML | **Recovered original** (staged uncommitted) |
| HCP CSS + intake shell | **Recovered original** (Railway stage twin; same Nov 2 lineage) |
| Tab mount / keep-alive wrapper | **Adapter** (necessary for GDI Tab 2; does not invent commercial fields) |
| Specific Mon–Sun chips | **Retained PR #216 improvement** |
| PR #216 reconstructed fieldset UI | **Superseded**; preserved under `preserved-current/` |

**Primary objective met: the original uncommitted interface was found and restored into GDI Tab 2.**
