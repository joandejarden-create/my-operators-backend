# SEC EDGAR Cache Compression and Storage Controls

**Status:** Implementation certified for merge; **bulk migration not authorized** until a separate founder/IC go-ahead.  
**Store version:** `sec-edgar-cache-store-v1`  
**Primary cache:** `data/sec-edgar/cache` (official SEC EDGAR HTTP responses)

## Problem

SEC EDGAR response caches used by HIF, Hotel Explorer, and related research workflows grow into multi‑GiB JSON trees. Sample gzip savings on production-shaped payloads are typically **~95%**, lossless relative to the exact UTF‑8 JSON bytes.

## Confirmed baseline (pre-migration)

| Metric | Value |
|--------|-------|
| Cache JSON files (3 workspaces) | ~2,731 |
| Approximate total size | ~16.14 GiB |
| Sample compression savings | 95.1%–95.9% |
| Record fields | `url`, `cachedAt`, `status`, `contentType`, `body`, `text` |

**Hard rule:** Matching basenames (`.json` vs `.json.gz`) can contain **different** content. Never assume filename equality proves content equality. Always compare SHA-256 of decompressed/canonical JSON bytes.

## Architecture

| Path | Role |
|------|------|
| `lib/hotel-intelligence/sec-edgar/sec-edgar-cache-store-v1.js` | Transparent read, atomic `.json.gz` write, migrate, monitor helpers |
| `lib/hotel-intelligence/sec-edgar/edgar-client-v1.js` | Official SEC client — reads legacy `.json` + `.json.gz`; **writes `.json.gz` only** |
| `lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js` | Mines `full_text_*.json` and `full_text_*.json.gz` without paid re-calls |
| `scripts/sec-edgar-cache-migrate-gzip-v1.mjs` | Migration utility (**dry-run default**) |
| `scripts/sec-edgar-cache-monitor-v1.mjs` | Read-only size / savings monitor |
| `scripts/test-sec-edgar-cache-gzip-v1.mjs` | Certification suite (temp dirs only) |

### Read order

1. If `key.json.gz` exists and gunzips + parses → use it.  
2. Else if `key.json` exists and parses → use it.  
3. Corrupt gzip increments monitor counters and falls back to `.json` when present.

### Write path

1. Serialize record with `JSON.stringify` (exact field preservation).  
2. gzip (best compression).  
3. Write `*.json.gz.tmp.<pid>.<rand>` then rename into `*.json.gz` (Windows-safe replace).  
4. **Do not** delete sibling `.json` on ordinary writes — deletion is migration-only and authorization-gated.

## Commands

```bash
# Certification (no network paid calls; temp dirs only)
npm run test:sec-edgar-cache-gzip-v1

# Read-only monitor across known SEC cache directories
npm run sec-edgar-cache:monitor

# Migration dry-run (default)
npm run sec-edgar-cache:migrate-gzip
npm run sec-edgar-cache:migrate-gzip -- --cache-dir data/sec-edgar/cache --limit 50

# Apply gzip writes but KEEP originals (still requires conscious --apply)
npm run sec-edgar-cache:migrate-gzip -- --apply --limit 1

# Delete originals ONLY after certify + separate authorization
npm run sec-edgar-cache:migrate-gzip -- --apply --delete-original \
  --authorize-delete AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE
```

## Migration plan (authorized later)

1. **Certify implementation** — `npm run test:sec-edgar-cache-gzip-v1` all PASS; PR merged.  
2. **Inventory** — run `sec-edgar-cache:monitor` on each workspace; archive reports under `reports/sec-edgar-cache-gzip/`.  
3. **Dry-run full dirs** — review mismatches (`CONTENT_MISMATCH_SAME_BASENAME`), busy files, projected savings.  
4. **Dual-read window** — `--apply` **without** `--delete-original` so readers can use either encoding. Validate HIF / HE cache hits on real workloads (**no paid research** for validation when cache-complete).  
5. **Founder/IC authorization** — explicit go-ahead for bulk delete of uncompressed originals.  
6. **Apply + delete** — `--apply --delete-original --authorize-delete AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE` in controlled batches (`--limit`).  
7. **Post-check** — re-monitor sizes; spot SHA-256 verify sample; confirm no open research process was interrupted (`FILE_BUSY` skips).

### Explicit non-goals until authorized

- No bulk `--apply` on production caches from this PR alone.  
- No deletion of originals without the authorize token **and** separate operational approval.  
- No paid SEC-API / Exa / research calls for testing.

## Certification checklist

| Gate | Expected |
|------|----------|
| Existing uncompressed cache reads | PASS |
| New compressed cache writes and reads | PASS |
| Corrupt compressed file handling | PASS |
| Exact content restoration | PASS |
| Atomic write and interrupted migration recovery | PASS |
| Existing HIF/Hotel Explorer workflows (client cache path) | PASS |
| No lost evidence or source references | PASS |

## Audit summary (readers / writers)

| Component | Before | After |
|-----------|--------|-------|
| `edgar-client-v1` read/write | `.json` only | read `.json`\|`.json.gz`; write `.json.gz` atomic |
| `ex10-contract-harvest-v1` mine | `.json` only | `.json` + `.json.gz` via store reader |
| `sec-api-io-client-v1` / Exa caches | separate schemas | unchanged writers; monitor includes their dirs; harvest can read gzip |
| Checkpoints / ledgers / inventories | not response caches | **out of scope** (not migrated) |

## Safety

- Preserve all HIF research, verified hotel intelligence, and SEC source provenance.  
- Skip busy/open files during migration.  
- Abort delete when basename pair hashes differ.  
- Roll back gzip if post-write SHA-256 diverges.  
- Temp leftovers from interrupted writes are ignored by the resolver (only final `.json` / `.json.gz` are candidates).
