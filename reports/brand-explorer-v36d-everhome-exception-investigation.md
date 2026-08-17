# Everhome Exception Investigation (v36D)

- Action: **investigate_exception**
- Previous readyForActiveProfile: **true**
- v36C calibrated score: 50 (founder_review_required)
- Regression verdict: investigate

## Recommendation

**remediation_apply** — Owner-visible gaps appear genuine — remediation apply warranted despite prior readyForActiveProfile

## Determination

```json
{
  "trueOwnerVisibleIssues": true,
  "contractPossiblyTooStrict": false,
  "legacyExceptionWarranted": false
}
```

## Blockers

```json
{
  "total": 37,
  "ownerVisible": 16,
  "codePatch": 0,
  "fallbackLike": 0,
  "trueOwnerGaps": 16,
  "samples": [
    {
      "issueId": "everhome-suites:governance_language:economics.legal",
      "issueType": "governance_language",
      "severity": "high",
      "rootCause": "governance_language"
    },
    {
      "issueId": "everhome-suites:property_example_render_not_ready:footprint.openings",
      "issueType": "property_example_render_not_ready",
      "severity": "high",
      "rootCause": "0/3 row-level image match"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:overview.scenario.3",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:overview.scenario.3"
    },
    {
      "issueId": "everhome-suites:factory:registry_not_approved:overview.scenario.2",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "registry_not_approved:overview.scenario.2"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:overview.scenario.2",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:overview.scenario.2"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:overview.scenario.1",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:overview.scenario.1"
    },
    {
      "issueId": "everhome-suites:factory:registry_not_approved:materials.gallery.1",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "registry_not_approved:materials.gallery.1"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:materials.gallery.1",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:materials.gallery.1"
    },
    {
      "issueId": "everhome-suites:factory:registry_not_approved:materials.gallery.2",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "registry_not_approved:materials.gallery.2"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:materials.gallery.2",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:materials.gallery.2"
    },
    {
      "issueId": "everhome-suites:factory:missing_durable_source_url:materials.gallery.3",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "missing_durable_source_url:materials.gallery.3"
    },
    {
      "issueId": "everhome-suites:factory:registry_not_approved:materials.gallery.4",
      "issueType": "factory_rule_blocker",
      "severity": "high",
      "rootCause": "registry_not_approved:materials.gallery.4"
    }
  ]
}
```

## Next

Run remediation_apply after founder confirms gaps are real