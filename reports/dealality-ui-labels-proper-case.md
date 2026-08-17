# Dealality UI labels — Proper Case pass

## Convention

| Use Proper Case | Use sentence case |
|-----------------|-------------------|
| Section labels (`Workspace Access`, `Company Capabilities`) | `.help-text` paragraphs |
| Dev workspace switcher (`Owner-Side`, `Demo Mode`) | Field explanations under Company Settings |
| Dropdown option text (Airtable-aligned: `Case-by-Case`, `Own-and-Operate Only`) | Badge `title` tooltips on operator cards |
| Badge prefixes (`Third-Party Management: Yes`) | Long checkbox captions (“We own or control hotel assets”) |

## Source of truth (browser)

`public/js/dealality-ui-labels.js` → `window.DEALALITY_UI_LABELS`

- `formatWorkspaceSideLabel` / `formatWorkspaceAccessDisplay`
- `formatDevWorkspaceSwitcherLabel`
- `formatThirdPartyManagementBadge`

Loaded on:

- `public/app.html` (shell dev workspace control + subtitle)
- `public/company-settings.html` (readonly Workspace Access display)

## Server badge alignment

`lib/company-workspace-access.js` `getCompanyDisplayBadges()` uses `Third-Party Management: {value}` to match the browser helper.

## Manual QA

1. **Company Settings** — Capabilities → Workspace Access shows `Owner-Side, Operator-Side` (not raw `Owner, Operator`). Helper under Workspace Access is sentence case.
2. **App shell (dev)** — Workspace dropdown: Owner-Side, Brand-Side, Operator-Side, Demo Mode, All Workspaces.
3. **Operator Explorer** (when using `public/js/operator-explorer.js` bundle) — Owner-operator cards show `Hotel Owner - Operator` and `Third-Party Management: …` badges.

## Operator Explorer

`public/operator-explorer.html` loads `dealality-ui-labels.js` + `operator-explorer.js` (inline list script removed). See `reports/operator-explorer-shared-label-formatters-qa.md`.
