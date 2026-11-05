# Three-Hotel Final Customer PDF Review

| Hotel | Pages | SHA-256 | Checks | Published OK | Parity |
|---|---:|---|---|---|---|
| Waterstone Resort & Marina | 7 | `f87fab8c023aa3733427d22f4c8519a45e30bdbac40d393a4e082be6e8bd7083` | true | true | true |
| AC Hotel A Coruña | 8 | `0b83459ecdaa46f9553137624d66bf84908b80223f455d9ae03fa55f4031a0ac` | true | true | true |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | 10 | `e0210c2b509b437f09e8441de6955453ed724c29747336958eb5c08c3b1a87e4` | true | true | true |

## Section 1 defect resolution

### Waterstone Resort & Marina

```json
{
  "domainStepsConcrete": true,
  "noTruncation": true,
  "noObjectObject": true,
  "meetingNotTitleOnly": true,
  "noTitleOnlySteps": true,
  "noSerialization": true,
  "noNearEmptyFinalPage": true
}
```

### AC Hotel A Coruña

```json
{
  "noMeetingLoungeMismatch": true,
  "loungeSeparate": true,
  "acbConsolidated": true,
  "noTitleOnlySteps": true,
  "noSerialization": true,
  "noNearEmptyFinalPage": true
}
```

### Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca

```json
{
  "singleSpaWorkstream": true,
  "noDualSpaTitles": true,
  "historicalDestDisclosed": true,
  "noTitleOnlySteps": true,
  "noSerialization": true,
  "noNearEmptyFinalPage": true
}
```


Visual QA method: pdfjs_page_text_density_plus_defect_rules. Admin Preview spot-check still recommended.
