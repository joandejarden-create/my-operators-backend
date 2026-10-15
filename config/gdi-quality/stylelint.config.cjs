/** Stylelint for GDI CSS — correctness-first; stylistic prefs off for legacy CSS. */
module.exports = {
  extends: ["stylelint-config-standard"],
  rules: {
    "selector-class-pattern": null,
    "custom-property-pattern": null,
    "keyframes-name-pattern": null,
    "no-descending-specificity": null,
    "declaration-block-no-redundant-longhand-properties": null,
    "color-function-notation": null,
    "color-function-alias-notation": null,
    "color-hex-length": null,
    "alpha-value-notation": null,
    "hue-degree-notation": null,
    "import-notation": null,
    "media-feature-range-notation": null,
    "declaration-property-value-keyword-no-deprecated": null,
    "declaration-block-no-shorthand-property-overrides": null,
    "declaration-block-single-line-max-declarations": null,
    "rule-empty-line-before": null,
    "no-duplicate-selectors": null, // advisory: responsive re-declarations intentional
    "declaration-property-value-no-unknown": true,
    "property-no-unknown": true,
    "selector-pseudo-element-no-unknown": true,
    "selector-pseudo-class-no-unknown": [
      true,
      { ignorePseudoClasses: ["global"] },
    ],
    "function-no-unknown": true,
    "unit-no-unknown": true,
    "value-keyword-case": null, // font stacks / currentColor casing left as authored
  },
  ignoreFiles: ["**/node_modules/**", "**/*.min.css"],
};
