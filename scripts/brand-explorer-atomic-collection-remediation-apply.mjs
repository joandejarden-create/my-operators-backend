#!/usr/bin/env node
console.error("Atomic collection remediation apply is disabled. The canonical pipeline does not write Presentation rows.");
console.error(JSON.stringify({ writes: 0, applied: false }));
process.exit(2);
