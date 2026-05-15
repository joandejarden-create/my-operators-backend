Brand Setup ↔ Airtable mapping inventory (generated files in this folder)

Regenerate from repo root:
  npm run export-brand-setup-airtable-inventory

Optional output directory:
  OUT_DIR=./path/to/folder npm run export-brand-setup-airtable-inventory

Source of truth: api/brand-library.js → buildBrandSetupAirtableMappingInventory()

Use the CSV/JSON to:
  1) Diff against your live Airtable schema (columns in base but not in export = candidates to wire or retire).
  2) Add columns "brandJsonPath" and "uiConsumer" in a copy for the full three-way matrix (code does not know every UI slot).

Presentation layer (Brand Explorer combined copy by slot):
  See docs/brand-explorer-presentation-slots.md — table "Brand Setup - Brand Explorer Presentation", merged as brand.brandExplorer on GET.
