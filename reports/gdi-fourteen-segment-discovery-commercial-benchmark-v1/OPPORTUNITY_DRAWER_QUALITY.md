# Opportunity drawer quality

## Certified commercial content audit (FS bag)

```json
{
  "audited": 86,
  "withMissingCommercialContent": 39,
  "withTruncatedText": 5,
  "withGenericBoilerplate": 11,
  "withHotelFitRewrite": 7,
  "withInvalidOrMissingContactRelevance": 0,
  "requiringAdditionalResearch": 39,
  "requiringReadinessDemotion": 1,
  "fullyPassing": 42
}
```

## Findings

- **39** rows missing at least one core commercial section.
- **Local catering** repeatedly used identical “plausible local venue…” hotel-fit boilerplate — weak for DOS action.
- Presentation contract now **rewrites** that boilerplate into org-specific uncertainty language without inventing demand.
- Truncation defects remain on a small set of recommended-next-step strings.

## Representative samples

- `disp_recLuxvwwxID7U2B8_city_of_rockville` — hotel-fit rewritten; identity: City of Rockville
- `disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce` — hotel-fit rewritten; identity: Greater Bethesda Chamber of Commerce
- `disp_recLuxvwwxID7U2B8_montgomery_county_shrm` — hotel-fit rewritten; identity: Montgomery County SHRM
