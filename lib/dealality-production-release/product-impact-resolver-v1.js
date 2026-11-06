/**
 * Resolve which Dealality products a proposed change set affects.
 * Fail-closed: unknown shared backend paths always pull SHARED mandatory gates.
 */
import fs from "node:fs";
import path from "node:path";

export const PRODUCT_IMPACT_CONTRACT = "DEALALITY_PRODUCT_IMPACT_RESOLVER_V1";

export function loadRegressionMatrix(root = process.cwd()) {
  const p = path.join(root, "config", "dealality-release-regression-matrix-v1.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * @param {{ changedFiles?: string[], primaryProduct?: string, matrix?: object }} opts
 */
export function resolveProductImpact(opts = {}) {
  const matrix = opts.matrix || loadRegressionMatrix(opts.root || process.cwd());
  const hints = matrix.pathProductHints || {};
  const files = Array.isArray(opts.changedFiles) ? opts.changedFiles : [];
  const products = new Set();

  if (opts.primaryProduct) {
    products.add(String(opts.primaryProduct).trim().toUpperCase());
  }

  for (const file of files) {
    const norm = String(file || "").replace(/\\/g, "/");
    let matched = false;
    for (const [prefix, product] of Object.entries(hints)) {
      if (norm.includes(prefix) || norm.startsWith(prefix.replace(/^\//, ""))) {
        products.add(product);
        matched = true;
      }
    }
    if (!matched && norm) {
      products.add("SHARED");
    }
  }

  for (const always of matrix.crossProductAlways || ["SHARED"]) {
    products.add(always);
  }

  // Primary product release always includes that product even with empty diff.
  if (opts.primaryProduct) {
    products.add(String(opts.primaryProduct).trim().toUpperCase());
  }

  const ordered = ["ADP", "GDI", "AVA", "SHARED", "PLATFORM"].filter((p) =>
    products.has(p)
  );
  for (const p of products) {
    if (!ordered.includes(p)) ordered.push(p);
  }

  return {
    contract: PRODUCT_IMPACT_CONTRACT,
    products: ordered,
    filesConsidered: files.length,
    primaryProduct: opts.primaryProduct || null,
  };
}

/**
 * Resolve mandatory tests for impacted products.
 * Missing npm script / script file = MISSING (failed gate), never silent skip.
 *
 * @param {{ products: string[], matrix?: object, packageScripts?: object, root?: string, expedited?: boolean }} opts
 */
export function resolveMandatoryTests(opts = {}) {
  const matrix = opts.matrix || loadRegressionMatrix(opts.root || process.cwd());
  const root = opts.root || process.cwd();
  const packageScripts =
    opts.packageScripts ||
    JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).scripts ||
    {};
  const expedited = opts.expedited === true;
  const products = new Set(opts.products || []);

  const selected = [];
  const seen = new Set();

  function addAll(list) {
    for (const t of list || []) {
      if (seen.has(t.id)) continue;
      if (expedited && t.expedited === false) continue;
      seen.add(t.id);
      selected.push({ ...t });
    }
  }

  addAll(matrix.sharedMandatory);
  for (const product of ["ADP", "GDI", "AVA"]) {
    if (products.has(product)) {
      addAll(matrix.productMandatory?.[product]);
    }
  }

  const resolved = selected.map((t) => {
    const npmExists = Boolean(packageScripts[t.npm]);
    const scriptPath = t.script ? path.join(root, t.script) : null;
    const scriptExists = scriptPath ? fs.existsSync(scriptPath) : false;
    let status = "READY";
    let detail = null;
    if (!npmExists || !scriptExists) {
      status = "MISSING";
      detail = !npmExists
        ? `npm script missing: ${t.npm}`
        : `script file missing: ${t.script}`;
    }
    return {
      ...t,
      status,
      detail,
      npmExists,
      scriptExists,
    };
  });

  const missing = resolved.filter((t) => t.status === "MISSING");
  return {
    ok: missing.length === 0,
    expedited,
    tests: resolved,
    missing,
    failures: missing.map((t) => ({
      code: "MANDATORY_TEST_MISSING",
      testId: t.id,
      detail: t.detail,
    })),
  };
}
