# Partner Directory – Layout Reference

Exact code that controls main content position and width. Use this when changing layout or adding new pages so behavior stays consistent.

---

## 1. Structure (HTML)

```html
<div class="page-wrapper">
    <div class="sidebar-wrapper">...</div>
    <div class="dashboard-main-section">
        <div class="partner-directory-container">
            <!-- header, tabs, filters, results-grid, etc. -->
        </div>
    </div>
</div>
```

---

## 2. CSS That Controls the Treatment

**File:** `partner-directory.html` (in the `<style>` block).

| Selector | Purpose |
|----------|--------|
| `.page-wrapper` | `min-height: 100vh; display: flex;` – flex container for sidebar + main. |
| `.sidebar-wrapper` | `width: 300px; min-width: 300px; position: fixed;` – fixed left sidebar. |
| `.dashboard-main-section` | `flex: 1; margin-left: 300px; min-width: 0;` – main area starts after sidebar, fills rest. |
| `.partner-directory-container` | **Default:** full width, left-aligned. **Optional:** `?layout=centered` – 1400px centered. |
| `.results-grid` | `minmax(340px, 1fr)` – card grid; columns grow/shrink with width. |

**Default (full-width, left-aligned):**

```css
.partner-directory-container {
    max-width: none;
    width: 100%;
    margin: 0;
    padding: 30px;
    box-sizing: border-box;
}
.results-grid {
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
}
```

- Main content is **left-aligned** and uses the **full width** of the main area (no 1400px cap, no side gaps on large screens).
- Grid uses **340px** min column width so more columns on wide screens; cards scale with width.

---

## 3. Optional Centered (Narrow) Layout

**URL to restore the old centered, capped layout:**

```
/partner-directory.html?layout=centered
```

**How it works:**  
`partner-directory.js` reads `?layout=centered` and adds `partner-directory-layout-centered` to `<body>`. The HTML includes:

```css
body.partner-directory-layout-centered .partner-directory-container {
    max-width: 1400px;
    margin: 0 auto;
}
body.partner-directory-layout-centered .results-grid {
    grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
}
```

Effect: content is centered with a 1400px cap (empty space on left and right when viewport > 1400px).

---

## 4. Future Code

- **New pages that should match this layout:** use the same structure (`.page-wrapper` > `.sidebar-wrapper` + `.dashboard-main-section` > `.partner-directory-container`) and the same rules above.
- **Changing gap/width:** only change `.partner-directory-container` (and, if desired, `.results-grid`). Do not change `.dashboard-main-section` margin unless the sidebar width changes.
- **Sidebar width:** if the real sidebar is not 300px, update `.sidebar-wrapper` width and `.dashboard-main-section` `margin-left` to the same value.
