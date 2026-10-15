/**
 * Knip entry points for GDI — unused findings are candidates for inspection,
 * not automatic deletion.
 */
export default {
  entry: [
    "api/group-demand-intelligence.js",
    "public/js/group-demand-intelligence/**/*.js",
    "lib/group-demand-intelligence/index.js",
    "lib/group-demand-intelligence/**/*.js",
    "scripts/gdi-*.mjs",
    "scripts/test-gdi-*.mjs",
    "scripts/gdi-quality-*.mjs",
    "scripts/deploy-production.mjs",
  ],
  project: [
    "api/group-demand-intelligence.js",
    "api/lib/**/*gdi*",
    "lib/group-demand-intelligence/**/*.{js,mjs}",
    "public/js/group-demand-intelligence/**/*.js",
    "scripts/gdi-*.mjs",
    "scripts/test-gdi-*.mjs",
    "scripts/gdi-quality-*.mjs",
  ],
  ignore: [
    "data/**",
    "reports/**",
    "fixtures/**",
    "**/node_modules/**",
  ],
  ignoreDependencies: [
    // Runtime deps used outside GDI entry graph
    "kill-port",
    "nodemon",
  ],
  // Report only; do not fail CI on unused by default (quality:full can promote)
  rules: {
    files: "off",
    dependencies: "off",
    unlisted: "off",
    binaries: "off",
    unresolved: "off",
    exports: "off",
    types: "off",
    nsExports: "off",
    classMembers: "off",
    enumMembers: "off",
    duplicates: "off",
  },
};
