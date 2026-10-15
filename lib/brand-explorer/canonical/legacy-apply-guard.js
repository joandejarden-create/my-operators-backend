/**
 * Break-glass only. Canonical build never writes Airtable.
 * Legacy npm apply paths call this and stop unless the operator sets the env var.
 */

export function legacyApplyRequested(argv = []) {
  const args = argv.slice(2);
  const stageIdx = args.indexOf("--stage");
  const stage = stageIdx >= 0 ? args[stageIdx + 1] : "";
  const alwaysWriteStages = new Set(["apply-draft", "status-promotion", "public-release"]);
  return args.includes("--apply") || alwaysWriteStages.has(stage);
}

export function refuseLegacyApply(argv = process.argv, source = "legacy-writer") {
  if (!legacyApplyRequested(argv)) return;
  if (process.env.BRAND_EXPLORER_ALLOW_LEGACY_APPLY === "1") {
    console.error(`[brand-explorer] legacy apply break-glass enabled for ${source}`);
    return;
  }
  console.error(`[brand-explorer] Legacy apply blocked (${source}).`);
  console.error("The canonical pipeline is the production path. It does not write during consolidation.");
  console.error("Break-glass for an audited historical writer: BRAND_EXPLORER_ALLOW_LEGACY_APPLY=1");
  process.exit(2);
}
