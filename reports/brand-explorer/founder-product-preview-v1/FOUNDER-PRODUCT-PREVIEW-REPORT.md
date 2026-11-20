# Founder Product Preview — Evidence

**Contract:** `BE_FOUNDER_PRODUCT_PREVIEW_V1`  
**Result:** PASS (wiring + browser hash parity; no Airtable writes)

## URLs (feature branch on :8082)

| View | URL |
|------|-----|
| Curio CURRENT SAVED (app shell; login required) | `http://localhost:8082/app#/brand-explorer-combined?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=saved` |
| Curio PROPOSED UPDATE (direct product) | `http://localhost:8082/brand-explorer-combined.html?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=proposed` |
| Curio Comparison Studio | `http://localhost:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg` |
| Ascend PROPOSED UPDATE | `http://localhost:8082/brand-explorer-combined.html?id=reclkgOzvAcBheUSo&beInternalPreview=1&beProfileMode=proposed` |

After IC merge + post-merge sync, use the same paths on `http://localhost:8080` (canonical port).

## Hash parity

| Brand | Product PROPOSED hash | Compare Studio RIGHT hash | Match |
|-------|----------------------|---------------------------|-------|
| Curio | `55a13b6c5fd59cbec19ab4fceb36465e37e51f2bc38f70c7960bcfdc238caff3` | same | YES |
| Ascend | `8b8a73b7c8c492cd93cc1f2014f10d8102e5630787780cdd64e0a18cec56b876` | same | YES |

Both use `phase2h-component-parity` + cumulative assembler. Ten atelier tabs present. Founder approval: **NOT_APPROVED**. Airtable writes: **none**.

## Screenshots

See `screenshots/` in this folder.
