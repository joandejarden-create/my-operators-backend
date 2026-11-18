# Remaining issues

1. **San Diego sparse classification** — many LMC rows lack `revenueStreamClass` / sales keys; icons depend on evidence derivation. Not a CSS regression; richer classification is a separate research-quality task.
2. **Need-date calendars often gray** — honest `not-assessed` when no need-period match exists. Do not force gold calendars without match evidence.
3. **Castillo / Waterstone / Radisson Blu Toronto** — representative Research Preview verification should continue on localhost after server restart; some hotels use alternate local store IDs or thinner inventories.
4. **Browser screenshot corpus** — this deliverable includes attribute/state regression tests and CSS token checks. Full pixel screenshot suites remain optional and were not required to prove icon state restoration.
5. **Unrelated WIP** — `pursuits.json` and other local dirty/untracked research artifacts were preserved and not part of this PR.
