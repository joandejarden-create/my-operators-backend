# Shell route Admin repair — 2026-10-17

## 1. Shell versus standalone

| Surface | URL | What loads | Auth |
| --- | --- | --- | --- |
| Application shell (actual user route) | `http://localhost:8080/app#/admin/ai-demand?tab=gdi-reports` | `public/app.js` embeds `/app/admin/ai-demand-admin.html?embed=1&appShell=1&tab=gdi-reports` in a same-origin iframe; JWT broadcast via `broadcastJwtToActiveFrames` | Shell gate first (`Sign in to continue`). Embed Admin then uses the same Memberstack JWT. |
| Standalone Admin | `http://localhost:8080/app/admin/ai-demand-admin.html?tab=gdi-reports` | Same HTML/JS/CSS assets (cache-bust `ada-shell-layout-v2` / `ada-client-inventory-v2`) | `SupportAdminGate` only — Access restricted without JWT |

**Origin:** `localhost` and `127.0.0.1` both serve 200 for shell and standalone. Memberstack cookies are origin-scoped; a session on one hostname does not apply to the other. No auth bypass was added.

**Automation browser (separate session):** On the exact shell URL, no JWT (`hasJwt: false`), zero iframes, shell shows Sign in. Standalone shows Access restricted over the Admin DOM. This does **not** mean the user’s authenticated shell is stale.

**Embed handoff (code):** `routeToEmbedUrl` forwards hash `tab` into the iframe query; Admin `readTabFromUrl` also reads parent hash. Tab switches call `syncParentHash`.

## 2. Missing-link resolution

Probes from primary checkout (no production signing secret locally):

| Hotel | GDI Open/Copy | ADP Open/Copy |
| --- | --- | --- |
| Renaissance `recG66DQJKP2c0UNh` | Available — sealed local `gdisht_b73f3e698d39fc66f00455f2` → production host | Available — contract token `sht_06ba4cd8b5a5cdb865d5109a` |
| Westin `recdmo0CnhIZAwHGC` | **Unavailable** — active `gdisht_30441d3811f87cb4d4f8e16e`, sealed URL absent (delivery files REDACTED) | Unavailable — registry active, no sealed bearer / no local secret |
| Wyndham `recnPB3Q02NO8Ma1o` | **Unavailable** — active `gdisht_c09c26841b52009d39698223`, same | Unavailable — same |

Ingest path ready (gitignored catalog only; bearers never printed or committed):

```bash
# Preferred: env vars (avoid shell history of full URLs if possible)
$env:GDI_WESTIN_SHARE_URL = '<full production GDI share URL>'
$env:GDI_WYNDHAM_SHARE_URL = '<full production GDI share URL>'
node scripts/ingest-production-share-sealed-local.mjs
```

Or drop file `config/client-share/production-share-sealed.drop.json` (gitignored):
`{ "urls": ["https://…/group-demand-intelligence-share.html?share=…", "https://…"] }`

Westin empty inventory stays `0` + compact **No published opportunities**; when the GDI link is sealed in, Open/Copy titles label it as an **empty client preview**.

## 3. Presentation / layout (shipped locally, not committed unless requested)

- Count cell: numeric + one compact zero label; link reasons removed from the count cell.
- Actions: Open/Copy always visible; disabled when sealed URL missing, with full reason on `title`.
- Tabs: `.ada-page .ada-tabs.bdd-section-nav { position: static; width: 100%; }` — stops sticky float over table in shell iframe. Shared Admin CSS only.

Screenshots:

- Before (sticky float demo): `reports/admin-client-inventory-action-parity/screenshots/admin-gdi-layout-before-sticky-overlap.png`
- After embed static: `reports/admin-client-inventory-action-parity/screenshots/admin-gdi-layout-after-embed-static.png`
- After standalone: `reports/admin-client-inventory-action-parity/screenshots/admin-gdi-layout-after-standalone.png`

Computed after fix (embed): `position: static`, `z-index: auto`, width full (~1564px).

## 4. Six-row action results

**Not executed.** Automation browser has no Memberstack session on the shell route. Catalog API returns login-required. Screenshot ≠ click pass.

| Row | Open | Copy | View PDF | Download PDF |
| --- | --- | --- | --- | --- |
| Renaissance ADP | pending auth | pending auth | pending auth | pending auth |
| Renaissance GDI | pending auth (link resolvable server-side) | pending auth | pending auth | pending auth |
| Westin ADP | pending auth + sealed ADP URL | pending auth + sealed ADP URL | pending auth | pending auth |
| Westin GDI | pending auth + **sealed GDI URL** | pending auth + **sealed GDI URL** | pending auth | pending auth |
| Wyndham ADP | pending auth + sealed ADP URL | pending auth + sealed ADP URL | pending auth | pending auth |
| Wyndham GDI | pending auth + **sealed GDI URL** | pending auth + **sealed GDI URL** | pending auth | pending auth |

## 5. Remaining protected inputs (required)

1. **Memberstack** on the exact shell URL:  
   `http://localhost:8080/app#/admin/ai-demand?tab=gdi-reports`  
   (same origin the user already uses — automation is a separate session).

2. **Two GDI production client URLs** (secure input — env or gitignored drop file, not chat paste if avoidable):
   - Westin Tampa Waterside — token id `gdisht_30441d3811f87cb4d4f8e16e` (`recdmo0CnhIZAwHGC`)
   - Wyndham San Diego Bayside — token id `gdisht_c09c26841b52009d39698223` (`recnPB3Q02NO8Ma1o`)

Memberstack alone will not populate the sealed catalog. No token rotation, no local production signing secret, no commit of bearers.

Optional for full six-row ADP Open/Copy: sealed ADP URLs for `sht_916fc65700ea6aa430417bcf` (Westin) and `sht_8ea2781c49c7e6b2a76bbbc5` (Wyndham).

## 6. Non-goals observed

No research, paid calls, eligibility, or publication changes. Nobu / Westin data untouched.
