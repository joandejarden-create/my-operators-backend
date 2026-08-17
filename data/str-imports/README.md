# STR Excel imports (read-only workflow)

Place STR export files here before running inventory / dry-run scripts:

- `Existing - Caribbean, South and Central America.xls`
- `Existing - Mexico.xls`
- (any additional `.xlsx` / `.xls` files)

## Geography mapping

Excel **STR Market** → Hotel Census **`Market`**  
Excel **STR Submarket** → Hotel Census **`Submarket`**

There are no separate `STR Market` or `STR Submarket` Airtable columns. Scout and STR import both use `Market` / `Submarket` as the official STR geography fields.

## Commands (no Airtable writes)

```bash
node scripts/inventory-hotel-census-for-str-import.mjs
node scripts/inventory-str-excel-files.mjs
node scripts/import-str-census-dry-run.mjs
```

Optional custom folder:

```bash
node scripts/inventory-str-excel-files.mjs --dir="C:/Users/you/Desktop"
node scripts/import-str-census-dry-run.mjs --dir="C:/Users/you/Desktop"
```

Reports are written under `reports/`.
