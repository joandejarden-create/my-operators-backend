# Bethesda 59→54 regression — cause, recovery, approval gate

## Exact five missing IDs

| ID | Organization |
|----|--------------|
| `gdi_sc_b36b66547864e8ce` | Matthews |
| `gdi_sc_a5768a99d4e89875` | IMC Construction |
| `gdi_sc_e7dd46732dd5bb7a` | BioHub Maryland |
| `gdi_sc_5c5d41eb670e6c7b` | Miles & Stockbridge |
| `gdi_sc_9d14aee7ff250b11` | REGENXBIO |

## Full original bodies recovered?

**Yes.** Exact verified 59-row store located:

- SHA: `fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad`
- Primary source: `C:\Dev\dealality\authorized-backups\primary-working-state-e2ae6c7d3-20261111\files\data\group-demand-intelligence\hotels\recLuxvwwxID7U2B8\opportunities.json`
- Duplicate verified copies: `primary-operational-data-20261111`, `primary-cutover-backup-20261111`

Full JSON bodies extracted to local (gitignored) `01-recovered-bodies/`.

## Proven cause of 59→54 regression

**Proven:** `data/group-demand-intelligence/hotels/*/opportunities.json` is **tracked in Git** (17 hotel stores). A `git checkout --force` onto the Exa feature branch replaced the richer local Bethesda store (59 / `fd438549…`) with the leaner committed blob (54 / working-tree SHA `54378697…`).

Evidence:
- All git history blobs for Bethesda `opportunities.json` contain **0** `gdi_sc_*` IDs and count **54**.
- The five Scaling Companies records existed only in local/authorized-backup operational state, never in committed main.
- Exa batch code did **not** write `opportunities.json`.

## Share registry comparison

| | Earlier reported | Current | Cutover backup 20261111 |
|--|------------------|---------|-------------------------|
| active SHA | `f1c533…` (not found on disk) | `9058f797…` | `babf0ec8…` |
| revoked SHA | `39ae2d…` (not found on disk) | `9a45475c…` | `d4d46d67…` |
| active count | — | 54 | 75 |

Current active tokens are a **subset** of the cutover backup (no current-only tokens). Same class of risk as hotel stores: tracked config can be replaced by leaner committed copies. **Do not overwrite registries merely to recreate old checksums.**

## Dry-run restoration (approval required)

**Recommended:** merge-add the five recovered bodies into the **current** 54-row store → 59 canonical → union **88**.

**Not recommended unless explicitly chosen:** byte-replace with `fd438549…` (would discard post-backup presentation mutations on ~16 shared IDs).

**STOP:** no authoritative `opportunities.json` write until founder approves this manifest.

## Paid research

Exa three-call batch remains exhausted (3 / $0.30). PDA / Chamber / Potomac Exa evidence preserved in research-stage. No additional paid calls.
