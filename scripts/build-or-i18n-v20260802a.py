#!/usr/bin/env python3
"""Build dealality-opportunity-review.v20260802a.js with correct JS concatenation."""
from pathlib import Path
import subprocess

src = Path("/workspace/public/marketing/dealality-opportunity-review.v20260729b.js").read_text()

header = r'''(function () {
  "use strict";

  var pathNorm = (window.location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
  var isEs = pathNorm === "/es" || pathNorm.indexOf("/es/") === 0;
  function t(en, es) { return isEs ? es : en; }
  function homeHref() { return isEs ? "/es" : "/"; }
  function loginHref() { return isEs ? "/es/login" : "/login"; }
  function signupHref() { return isEs ? "/es/signup" : "/signup"; }
  function privacyHref() { return isEs ? "/es/privacy" : "/privacy"; }
  function insightsHref() { return isEs ? "/es/insights" : "/insights"; }

'''
if not src.startswith('(function () {\n  "use strict";\n\n'):
    raise SystemExit("unexpected header")
src = header + src[len('(function () {\n  "use strict";\n\n'):]

old_dec = """  var DECISION_TYPES = [
    ["Brand selection", "Brand Selection"],
    ["Operator selection", "Operator Selection"],
    ["Conversion or repositioning", "Conversion or Repositioning"],
    ["New hotel development", "New Hotel Development"],
    ["Mixed-use hospitality", "Mixed-Use Hospitality"],
    ["Branded residences", "Branded Residences"],
    ["Franchise versus management structure", "Franchise Versus Management"],
    ["Strategic partner outreach", "Capital or Strategic Partner"],
    ["Sale or exit strategy", "Sale or Exit Strategy"],
    ["Other", "Other"],
  ];
  var TIMINGS = [
    ["Active now", "Active Now"],
    ["Within 3 months", "Within 3 Months"],
    ["Within 6 months", "Within 6 Months"],
    ["Within 12 months", "Within 12 Months"],
    ["Early planning", "Early Planning"],
    ["Not yet determined", "Not Yet Determined"],
  ];"""
new_dec = """  var DECISION_TYPES = [
    ["Brand selection", t("Brand Selection", "Selección de Marca")],
    ["Operator selection", t("Operator Selection", "Selección de Operador")],
    ["Conversion or repositioning", t("Conversion or Repositioning", "Conversión o Reposicionamiento")],
    ["New hotel development", t("New Hotel Development", "Nuevo Desarrollo Hotelero")],
    ["Mixed-use hospitality", t("Mixed-Use Hospitality", "Hospitalidad de Uso Mixto")],
    ["Branded residences", t("Branded Residences", "Residencias de Marca")],
    ["Franchise versus management structure", t("Franchise Versus Management", "Franquicia Frente a Gestión")],
    ["Strategic partner outreach", t("Capital or Strategic Partner", "Capital o Socio Estratégico")],
    ["Sale or exit strategy", t("Sale or Exit Strategy", "Venta o Estrategia de Salida")],
    ["Other", t("Other", "Otro")],
  ];
  var TIMINGS = [
    ["Active now", t("Active Now", "Activo Ahora")],
    ["Within 3 months", t("Within 3 Months", "En 3 Meses")],
    ["Within 6 months", t("Within 6 Months", "En 6 Meses")],
    ["Within 12 months", t("Within 12 Months", "En 12 Meses")],
    ["Early planning", t("Early Planning", "Planificación Temprana")],
    ["Not yet determined", t("Not Yet Determined", "Aún No Determinado")],
  ];"""
if old_dec not in src:
    raise SystemExit("DECISION_TYPES missing")
src = src.replace(old_dec, new_dec, 1)

# Exact literal -> JS expression replacements. Values are inserted as-is into the JS file.
REPLACEMENTS = [
    (
        '<a class="or-nav-logo" href="/old-home" aria-label="Dealality home">',
        """'<a class="or-nav-logo" href="' + homeHref() + '" aria-label="Dealality home">'""",
    ),
    (
        '<a class="or-nav-back" href="/old-home">Back to Dealality</a>',
        """'<a class="or-nav-back" href="' + homeHref() + '">' + t("Back to Dealality", "Volver a Dealality") + '</a>'""",
    ),
    (
        '<a class="or-nav-signin" href="/login">Sign In</a>',
        """'<a class="or-nav-signin" href="' + loginHref() + '">' + t("Sign In", "Iniciar Sesión") + '</a>'""",
    ),
    (
        '<p class="or-eyebrow"><span class="or-eyebrow-pill">Opportunity Review</span><span class="or-eyebrow-text">Confidential. Owner-led.</span></p>',
        """'<p class="or-eyebrow"><span class="or-eyebrow-pill">' + t("Opportunity Review", "Revisión de Oportunidad") + '</span><span class="or-eyebrow-text">' + t("Confidential. Owner-led.", "Confidencial. Dirigida por el propietario.") + '</span></p>'""",
    ),
    (
        '<h1 class="or-h1">Tell us about your hotel opportunity.</h1>',
        """'<h1 class="or-h1">' + t("Tell us about your hotel opportunity.", "Cuéntanos Sobre Tu Oportunidad Hotelera.") + '</h1>'""",
    ),
    (
        '<p class="or-lead">Share a few details about the hotel decision you are evaluating.<br>We will review it confidentially and contact you to discuss next steps.</p>',
        """'<p class="or-lead">' + t("Share a few details about the hotel decision you are evaluating.<br>We will review it confidentially and contact you to discuss next steps.", "Comparte algunos detalles sobre la decisión hotelera que estás evaluando.<br>La revisaremos de forma confidencial y te contactaremos para hablar sobre los próximos pasos.") + '</p>'""",
    ),
    (
        '<p class="or-reassure">No account required. Nothing is shared with brands, operators, or other parties without your approval.</p>',
        """'<p class="or-reassure">' + t("No account required. Nothing is shared with brands, operators, or other parties without your approval.", "No se requiere cuenta. Nada se comparte con marcas, operadores u otras partes sin tu aprobación.") + '</p>'""",
    ),
    (
        '<p class="or-req-note">All fields are required unless marked optional.</p>',
        """'<p class="or-req-note">' + t("All fields are required unless marked optional.", "Todos los campos son obligatorios salvo que se indiquen como opcionales.") + '</p>'""",
    ),
    (
        '<p class="or-eyebrow"><span class="or-eyebrow-pill">Submitted</span><span class="or-eyebrow-text">Confidential review requested.</span></p>',
        """'<p class="or-eyebrow"><span class="or-eyebrow-pill">' + t("Submitted", "Enviado") + '</span><span class="or-eyebrow-text">' + t("Confidential review requested.", "Revisión confidencial solicitada.") + '</span></p>'""",
    ),
    (
        '<h2 class="or-h1">Your opportunity has been submitted.</h2>',
        """'<h2 class="or-h1">' + t("Your opportunity has been submitted.", "Tu oportunidad ha sido enviada.") + '</h2>'""",
    ),
    (
        '<p class="or-lead">Thank you. We will review the information confidentially and contact you directly regarding the next step.</p>',
        """'<p class="or-lead">' + t("Thank you. We will review the information confidentially and contact you directly regarding the next step.", "Gracias. Revisaremos la información de forma confidencial y te contactaremos directamente sobre el siguiente paso.") + '</p>'""",
    ),
    (
        '<a class="or-btn-ghost" href="/old-home">Return to Dealality</a>',
        """'<a class="or-btn-ghost" href="' + homeHref() + '">' + t("Return to Dealality", "Volver a Dealality") + '</a>'""",
    ),
    (
        '<a class="or-btn-ghost" href="/insights">Read Dealality Insights</a>',
        """'<a class="or-btn-ghost" href="' + insightsHref() + '">' + t("Read Dealality Insights", "Leer Insights de Dealality") + '</a>'""",
    ),
    (
        '<span>I have read the <a href="/privacy" target="_blank" rel="noopener">Privacy Notice</a> and agree that Dealality may contact me about this opportunity.</span>',
        """'<span>' + t('I have read the <a href="' + privacyHref() + '" target="_blank" rel="noopener">Privacy Notice</a> and agree that Dealality may contact me about this opportunity.', 'He leído el <a href="' + privacyHref() + '" target="_blank" rel="noopener">Aviso de Privacidad</a> y acepto que Dealality me contacte sobre esta oportunidad.') + '</span>'""",
    ),
]

# These appear inside larger '....' +  concatenations, so we only replace the inner HTML text,
# keeping surrounding quotes from the original source.
INNER = [
    (">1. About You</h2>", """>' + t("1. About You", "1. Sobre Ti") + '</h2>"""),
    (
        ">Who should we contact about the opportunity?</p>",
        """>' + t("Who should we contact about the opportunity?", "¿A quién debemos contactar sobre la oportunidad?") + '</p>""",
    ),
    (
        'fieldMarkup("fullName", "Full Name", "text", "John Smith", "name")',
        'fieldMarkup("fullName", t("Full Name", "Nombre Completo"), "text", "John Smith", "name")',
    ),
    (
        'fieldMarkup("businessEmail", "Business Email", "email", "you@company.com", "email")',
        'fieldMarkup("businessEmail", t("Business Email", "Email Corporativo"), "email", "you@company.com", "email")',
    ),
    (
        'fieldMarkup("company", "Company", "text", "Company Name", "organization")',
        'fieldMarkup("company", t("Company", "Empresa"), "text", t("Company Name", "Nombre de la Empresa"), "organization")',
    ),
    (
        'fieldMarkup("role", "Role", "text", "Owner, Developer, Investor, Advisor...", "organization-title", true)',
        'fieldMarkup("role", t("Role", "Rol"), "text", t("Owner, Developer, Investor, Advisor...", "Propietario, Desarrollador, Inversionista, Asesor..."), "organization-title", true)',
    ),
    (">2. About the Opportunity</h2>", """>' + t("2. About the Opportunity", "2. Sobre la Oportunidad") + '</h2>"""),
    (
        ">A few details will help us understand the asset and the decision.</p>",
        """>' + t("A few details will help us understand the asset and the decision.", "Algunos detalles nos ayudan a entender el activo y la decisión.") + '</p>""",
    ),
    (
        'fieldMarkup("projectName", "Hotel or Project Name", "text", "Hotel or Project Name", "", true)',
        'fieldMarkup("projectName", t("Hotel or Project Name", "Nombre del Hotel o Proyecto"), "text", t("Hotel or Project Name", "Nombre del Hotel o Proyecto"), "", true)',
    ),
    (
        'fieldMarkup("location", "Hotel or Project Location", "text", "City, Country", "")',
        'fieldMarkup("location", t("Hotel or Project Location", "Ubicación del Hotel o Proyecto"), "text", t("City, Country", "Ciudad, País"), "")',
    ),
    (
        ">3. What Are You Evaluating?</h2>",
        """>' + t("3. What Are You Evaluating?", "3. ¿Qué Estás Evaluando?") + '</h2>""",
    ),
    (
        ">Tell us enough to understand the question, the opportunity, and what you hope to achieve.</p>",
        """>' + t("Tell us enough to understand the question, the opportunity, and what you hope to achieve.", "Cuéntanos lo suficiente para entender la pregunta, la oportunidad y lo que esperas lograr.") + '</p>""",
    ),
    (">Decision Type</legend>", """>' + t("Decision Type", "Tipo de Decisión") + '</legend>"""),
    (
        ">Select all that apply.</span>",
        """>' + t("Select all that apply.", "Selecciona todas las que apliquen.") + '</span>""",
    ),
    (
        ">Select at least one Decision Type.</span>",
        """>' + t("Select at least one Decision Type.", "Selecciona al menos un Tipo de Decisión.") + '</span>""",
    ),
    (">Project Timing</label>", """>' + t("Project Timing", "Momento del Proyecto") + '</label>"""),
    (
        """'<option value="">Select Timing</option>'""",
        """'<option value="">' + t("Select Timing", "Seleccionar Momento") + '</option>'""",
    ),
    (
        ">Select the approximate Project Timing.</span>",
        """>' + t("Select the approximate Project Timing.", "Selecciona el Momento aproximado del Proyecto.") + '</span>""",
    ),
    (
        ">Briefly Describe the Opportunity and Decision</label>",
        """>' + t("Briefly Describe the Opportunity and Decision", "Describe Brevemente la Oportunidad y la Decisión") + '</label>""",
    ),
    (
        'placeholder="For example: We are evaluating whether to reposition an existing independent hotel, pursue a soft brand, or bring in a third-party operator..."',
        """placeholder="' + t("For example: We are evaluating whether to reposition an existing independent hotel, pursue a soft brand, or bring in a third-party operator...", "Por ejemplo: Estamos evaluando si reposicionar un hotel independiente existente, buscar una soft brand o incorporar un operador tercero...") + '" """,
    ),
    (
        ">You may include the current situation, the paths being considered, your main objectives, and any timing considerations. Recommended: 50–300 words.</span>",
        """>' + t("You may include the current situation, the paths being considered, your main objectives, and any timing considerations. Recommended: 50–300 words.", "Puedes incluir la situación actual, las opciones que estás considerando, tus objetivos principales y cualquier consideración de tiempo. Recomendado: 50–300 palabras.") + '</span>""",
    ),
    (
        ">Tell us briefly about the opportunity and decision.</span>",
        """>' + t("Tell us briefly about the opportunity and decision.", "Cuéntanos brevemente sobre la oportunidad y la decisión.") + '</span>""",
    ),
    (
        ">4. How Should We Follow Up?</h2>",
        """>' + t("4. How Should We Follow Up?", "4. ¿Cómo Debemos Dar Seguimiento?") + '</h2>""",
    ),
    (
        ">Choose how you would prefer us to contact you.</p>",
        """>' + t("Choose how you would prefer us to contact you.", "Elige cómo prefieres que te contactemos.") + '</p>""",
    ),
    (
        'Preferred Contact Method <span class="or-optional">(optional)</span>',
        """' + t("Preferred Contact Method", "Método de Contacto Preferido") + ' <span class="or-optional">(' + t("optional", "opcional") + ')</span>""",
    ),
    ('radioMarkup("Email")', 'radioMarkup("Email", t("Email", "Email"))'),
    ('radioMarkup("Phone")', 'radioMarkup("Phone", t("Phone", "Teléfono"))'),
    ('radioMarkup("Video Call")', 'radioMarkup("Video Call", t("Video Call", "Videollamada"))'),
    ('radioMarkup("No Preference")', 'radioMarkup("No Preference", t("No Preference", "Sin Preferencia"))'),
    (">Phone Number</label>", """>' + t("Phone Number", "Número de Teléfono") + '</label>"""),
    (
        ">Enter the phone number you would like us to use.</span>",
        """>' + t("Enter the phone number you would like us to use.", "Ingresa el número de teléfono que debemos usar.") + '</span>""",
    ),
    (
        ">Please confirm that you have read the Privacy Notice.</span>",
        """>' + t("Please confirm that you have read the Privacy Notice.", "Confirma que has leído el Aviso de Privacidad.") + '</span>""",
    ),
    (
        ">Submit for Confidential Review</button>",
        """>' + t("Submit for Confidential Review", "Enviar para Revisión Confidencial") + '</button>""",
    ),
    (
        ">We will review the information and contact you directly. Your submission will not be shared with outside parties without your approval.</p>",
        """>' + t("We will review the information and contact you directly. Your submission will not be shared with outside parties without your approval.", "Revisaremos la información y te contactaremos directamente. Tu envío no se compartirá con terceros sin tu aprobación.") + '</p>""",
    ),
    (
        'Prefer email? Contact <a href="mailto:hello@aohospitalityadvisors.com">hello@aohospitalityadvisors.com</a>',
        """' + t("Prefer email? Contact", "¿Prefieres email? Contacta") + ' <a href="mailto:hello@aohospitalityadvisors.com">hello@aohospitalityadvisors.com</a>""",
    ),
    (
        """      fullName: "Enter your name.",
      businessEmail: "Enter a valid business email address.",
      company: "Enter your company name.",
      location: "Enter the hotel or project location.",""",
        """      fullName: t("Enter your name.", "Ingresa tu nombre."),
      businessEmail: t("Enter a valid business email address.", "Ingresa un email corporativo válido."),
      company: t("Enter your company name.", "Ingresa el nombre de tu empresa."),
      location: t("Enter the hotel or project location.", "Ingresa la ubicación del hotel o proyecto."),""",
    ),
    (
        'submitButton.textContent = "Submitting...";',
        'submitButton.textContent = t("Submitting...", "Enviando...");',
    ),
    (
        'submitButton.textContent = "Submit for Confidential Review";',
        'submitButton.textContent = t("Submit for Confidential Review", "Enviar para Revisión Confidencial");',
    ),
    (
        '"We could not submit your opportunity. Please try again."',
        't("We could not submit your opportunity. Please try again.", "No pudimos enviar tu oportunidad. Inténtalo de nuevo.")',
    ),
    (
        '"We could not submit your opportunity. Please try again or email hello@aohospitalityadvisors.com.";',
        't("We could not submit your opportunity. Please try again or email hello@aohospitalityadvisors.com.", "No pudimos enviar tu oportunidad. Inténtalo de nuevo o escribe a hello@aohospitalityadvisors.com.");',
    ),
    (
        '(optional ? \' <span class="or-optional">(optional)</span>\' : "")',
        "(optional ? ' <span class=\"or-optional\">(' + t(\"optional\", \"opcional\") + ')</span>' : \"\")",
    ),
]

# Full-chunk replacements (standalone string literals in pageMarkup)
for old, new in REPLACEMENTS:
    # old appears inside: '....old....'  — replace old text with closed/reopened concat
    # For logo etc the old is the full content of a single-quoted string piece.
    # Strategy: find "'"+old+"'" in source? Actually source has:
    # '<a class="or-nav-logo" href="/old-home" aria-label="Dealality home">' +
    needle = "'" + old + "'"
    if needle not in src:
        raise SystemExit("missing chunk: " + old[:80])
    src = src.replace(needle, new, 1)

for old, new in INNER:
    if old not in src:
        raise SystemExit("missing inner: " + old[:90])
    # For INNER that start with >' we are replacing inside an existing quote:
    # '<h2 ...>1. About You</h2>'  ->  '<h2 ...>' + t(...) + '</h2>'
    # So if new starts with >' + t, we need to close the quote before +.
    # Pattern: if old starts with > then original is like ...title">1. About You</h2>
    # inside '...title">1. About You</h2>'
    # We want '...title">' + t(...) + '</h2>'
    if old.startswith(">") and new.startswith(">' + t("):
        # Replace inside the string by closing quote after >
        # Find occurrences carefully — only first
        src = src.replace(old, new, 1)
    else:
        src = src.replace(old, new, 1)

old_radio = """  function radioMarkup(value) {
    return (
      '<label class="or-radio"><input type="radio" name="preferredContact" value="' +
      escapeHtml(value) +
      '"><span>' +
      escapeHtml(value) +
      "</span></label>"
    );
  }"""
new_radio = """  function radioMarkup(value, label) {
    return (
      '<label class="or-radio"><input type="radio" name="preferredContact" value="' +
      escapeHtml(value) +
      '"><span>' +
      escapeHtml(label || value) +
      "</span></label>"
    );
  }"""
if old_radio not in src:
    raise SystemExit("radioMarkup mismatch")
src = src.replace(old_radio, new_radio, 1)

# After INNER replacements that used >' + t(, the surrounding quotes become broken:
# '<h2 id="x">' + t(...) + '</h2>'
# was originally: '<h2 id="x">1. About You</h2>'
# After replace of `>1. About You</h2>` with `>' + t(...) + '</h2>`:
# '<h2 id="x">' + t(...) + '</h2>'
# Wait: original string is `'<h2 ...>1. About You</h2>'`
# replacing `>1. About You</h2>` with `>' + t(...) + '</h2>` gives:
# `'<h2 ...' + t(...) + '</h2>'`  NO:
# `'<h2 ...id="x">' + t(...) + '</h2>'` 
# Actually: the characters are: quote, <h2...>, 1. About You, </h2>, quote
# Replace >1. About You</h2> which starts at the > of h2 opening tag's end...
# `<h2 class="or-section-h" id="or-about-title">1. About You</h2>`
# old = `>1. About You</h2>`
# new = `>' + t(...) + '</h2>`
# Result: `'<h2 class="or-section-h" id="or-about-title">' + t(...) + '</h2>'`
# That's CORRECT!

out = Path("/workspace/public/marketing/dealality-opportunity-review.v20260802a.js")
out.write_text(
    "/** Opportunity Review form v20260802a — Spanish UI on /es; EN API values preserved. */\n"
    + src
)
r = subprocess.run(["node", "--check", str(out)], capture_output=True, text=True)
print("wrote", out.stat().st_size, "syntax", r.returncode)
if r.stderr:
    print(r.stderr[:1200])
    # show around error line if any
    import re as _re
    m = _re.search(r":(\d+)\n", r.stderr)
    if m:
        n = int(m.group(1))
        lines = out.read_text().splitlines()
        for i in range(max(0, n - 3), min(len(lines), n + 2)):
            print(f"{i+1}: {lines[i][:200]}")
