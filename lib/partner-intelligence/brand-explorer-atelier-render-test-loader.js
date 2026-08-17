/**
 * Load Brand Explorer Atelier renderer in Node for DOM/HTML assertion tests.
 */
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import vm from "vm";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

function createDocumentMock() {
  const store = new Map();
  const documentElement = {
    classList: { contains() { return false; } },
    getAttribute(name) {
      if (name === "data-brand-explorer-unified-tabs") return "1";
      return null;
    },
  };
  return {
    documentElement,
    body: { querySelectorAll() { return []; } },
    createElement(tag) {
      const el = {
        tagName: tag.toUpperCase(),
        style: {},
        textContent: "",
        innerHTML: "",
        setAttribute() {},
        appendChild() {},
        addEventListener() {},
        getAttribute() { return null; },
      };
      Object.defineProperty(el, "textContent", {
        set(v) {
          this._text = v == null ? "" : String(v);
          this.innerHTML = this._text
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
        },
        get() {
          return this._text || "";
        },
      });
      return el;
    },
    getElementById(id) {
      return store.get(id) || null;
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    addEventListener() {},
  };
}

function loadScript(relativePath, sandboxExtras = {}) {
  const locationSearch =
    sandboxExtras.locationSearch != null ? String(sandboxExtras.locationSearch) : "";
  const { locationSearch: _omit, ...restExtras } = sandboxExtras;
  const sandbox = {
    window: {},
    globalThis: {},
    document: createDocumentMock(),
    console,
    // Atelier uses `new URL(...)` for safe-link checks; vm sandbox must expose it.
    URL,
    URLSearchParams,
    ...restExtras,
  };
  sandbox.globalThis = sandbox.window;
  sandbox.window.URL = URL;
  sandbox.window.URLSearchParams = URLSearchParams;
  sandbox.window.location = {
    search: locationSearch,
    href: `http://localhost/${locationSearch}`,
  };
  sandbox.window.addEventListener = function () {};
  sandbox.window.setTimeout = function (fn) {
    if (typeof fn === "function") fn();
    return 0;
  };
  vm.runInNewContext(readFileSync(join(ROOT, relativePath), "utf8"), sandbox);
  return sandbox.window;
}

/**
 * @returns {{ BrandExplorerAtelierFromApi: object }}
 */
export function loadBrandExplorerAtelierRenderer() {
  loadScript("public/js/brand-explorer-census-metrics.js");
  const win = loadScript("public/js/brand-explorer-atelier-from-api.js");
  if (!win.BrandExplorerAtelierFromApi?.renderOverviewHtmlForTest) {
    throw new Error("BrandExplorerAtelierFromApi test hooks missing");
  }
  return win.BrandExplorerAtelierFromApi;
}

/**
 * @param {object} brand
 * @param {{ allPanels?: boolean, internalPreview?: boolean, factoryPreview?: boolean }} [options]
 *   internalPreview — sets `?beInternalPreview=1` so locked brands render full profile for QA.
 *   factoryPreview — sets `?beInternalPreview=1&factoryPreview=1` for Factory Preview Mode.
 */
export function renderBrandExplorerHtmlForTest(brand, options = {}) {
  let locationSearch = "";
  if (options.factoryPreview) {
    locationSearch = "?beInternalPreview=1&factoryPreview=1";
  } else if (options.internalPreview) {
    locationSearch = "?beInternalPreview=1";
  }
  // Reload atelier with the requested location.search (internal/factory preview is request-scoped).
  loadScript("public/js/brand-explorer-census-metrics.js", { locationSearch });
  const win = loadScript("public/js/brand-explorer-atelier-from-api.js", { locationSearch });
  const atelier = win.BrandExplorerAtelierFromApi;
  if (!atelier?.renderOverviewHtmlForTest) {
    throw new Error("BrandExplorerAtelierFromApi test hooks missing");
  }
  if (options.allPanels) {
    return atelier.buildPanelsHtmlForTest(brand).html;
  }
  return atelier.renderOverviewHtmlForTest(brand);
}
