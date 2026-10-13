# Multilingual Discovery Contract

## AC Hotel A Coruña

- **PRIMARY:** Spanish (`es`), Galician (`gl`)
- **SECONDARY:** English (`en`)
- Runtime profile: primary=`es` secondary=`gl,en`
- Hotel locale: {"primary":"es","secondary":["gl","en"],"notes":"PRIMARY Spanish + Galician local discovery; SECONDARY English control / intl. Do not treat English as canonical."}
- Scout languages: es, gl, en

## Radisson Santo Domingo

- **PRIMARY:** Spanish (`es`)
- **SECONDARY:** English (`en`)
- Runtime profile: primary=`es` secondary=`en`
- Hotel locale: {"primary":"es","secondary":["en"],"notes":"PRIMARY Spanish (Dominican institutional / MICE terminology); SECONDARY English control. Do not treat English as canonical."}
- Scout languages: es, en

## YOTEL Geneva Lake (control)

- **PRIMARY:** French (`fr`)
- **SECONDARY:** English (`en`); selective DE/IT
- Runtime profile: primary=`fr` secondary=`en` selective=`de,it`
- Hotel locale: {"primary":"fr","secondary":["en"],"selective":["de","it"],"notes":"PRIMARY French (Swiss/Geneva Lake local); SECONDARY English intl associations; DE/IT selective only when yield supports."}

## Rules

- English is a **control lane**, not the canonical demand language.
- Queries emit `queryLanguage`, `sourceLanguage`, `queryFamily`, `baseOfDemand`, `market`.
- No language-specific sidecar outside the canonical pipeline.
