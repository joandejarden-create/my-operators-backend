/** Opportunity Review form v20260802a — Spanish UI on /es; EN API values preserved. */
(function () {
  "use strict";

  var pathNorm = (window.location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
  var isEs = pathNorm === "/es" || pathNorm.indexOf("/es/") === 0;
  function t(en, es) { return isEs ? es : en; }
  function homeHref() { return isEs ? "/es" : "/"; }
  function loginHref() { return isEs ? "/es/login" : "/login"; }
  function signupHref() { return isEs ? "/es/signup" : "/signup"; }
  function privacyHref() { return isEs ? "/es/privacy" : "/privacy"; }
  function insightsHref() { return isEs ? "/es/insights" : "/insights"; }

  var API_BASE = window.DEALALITY_API_BASE || "";
  var SUBMIT_URL = API_BASE + "/api/marketing/opportunity-review";
  var EVENTS_URL = API_BASE + "/api/marketing/landing-events";
  var SESSION_KEY = "dl_landing_sid_v1";
  var DECISION_TYPES = [
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
  ];
  var SECTION_FIELDS = {
    about_you: ["fullName", "businessEmail", "company"],
    opportunity: ["location"],
    decision: ["decisionTypes", "projectTiming", "description"],
    follow_up: ["preferredContact", "privacyAck"],
  };
  var started = false;
  var submitting = false;
  var completedSections = {};
  var mounted = false;

  function isDevelopment() {
    return /^(localhost|127\.0\.0\.1)$/.test(window.location.hostname);
  }

  function devLog(level, message, detail) {
    if (!isDevelopment() || !window.console) return;
    var method = console[level] || console.log;
    method.call(console, "[opportunity-review] " + message, detail || "");
  }

  function deviceCategory() {
    var width = window.innerWidth || 0;
    if (width < 768) return "mobile";
    if (width < 1024) return "tablet";
    return "desktop";
  }

  function sessionId() {
    try {
      var existing = window.sessionStorage.getItem(SESSION_KEY);
      if (existing) return existing;
      var created =
        "dl_" +
        Date.now().toString(36) +
        "_" +
        Math.random().toString(36).slice(2, 11);
      window.sessionStorage.setItem(SESSION_KEY, created);
      return created;
    } catch (error) {
      devLog("warn", "session storage unavailable", error);
      return "dl_" + Date.now().toString(36) + "_fallback";
    }
  }

  function track(eventName, extra) {
    var payload = {
      event: eventName,
      sessionId: sessionId(),
      surface: "opportunity_review",
      device: deviceCategory(),
      referrer: document.referrer || "",
      path: window.location.pathname || "",
    };

    if (extra && typeof extra === "object") {
      Object.keys(extra).forEach(function (key) {
        payload[key] = extra[key];
      });
    }

    var body;
    try {
      body = JSON.stringify(payload);
      if (navigator.sendBeacon) {
        var queued = navigator.sendBeacon(
          EVENTS_URL,
          new Blob([body], { type: "application/json" })
        );
        if (queued) return;
      }
      fetch(EVENTS_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body,
        credentials: "omit",
        keepalive: true,
      }).catch(function (error) {
        devLog("warn", "analytics request failed", error);
      });
    } catch (error) {
      devLog("warn", "analytics event could not be sent", error);
    }
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function decisionMarkup() {
    return DECISION_TYPES.map(function (option) {
      return (
        '<label class="or-check">' +
        '<input type="checkbox" name="decisionTypes" value="' +
        escapeHtml(option[0]) +
        '" aria-describedby="decisionTypes-help decisionTypes-error">' +
        '<span class="or-check-box" aria-hidden="true"></span>' +
        "<span>" +
        escapeHtml(option[1]) +
        "</span>" +
        "</label>"
      );
    }).join("");
  }

  function timingMarkup() {
    return TIMINGS.map(function (timing) {
      var value = Array.isArray(timing) ? timing[0] : timing;
      var label = Array.isArray(timing) ? timing[1] : timing;
      return (
        '<option value="' +
        escapeHtml(value) +
        '">' +
        escapeHtml(label) +
        "</option>"
      );
    }).join("");
  }

  function pageMarkup() {
    return (
      '<nav class="or-nav" aria-label="Opportunity review navigation">' +
      '<a class="or-nav-logo" href="' + homeHref() + '" aria-label="Dealality home">' +
      '<img src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a67ed8bb2f335717dc3b820_dealality-wordmark-nav.png" alt="Dealality">' +
      "</a>" +
      '<div class="or-nav-right">' +
      '<a class="or-nav-back" href="' + homeHref() + '">' + t("Back to Dealality", "Volver a Dealality") + '</a>' +
      '<a class="or-nav-signin" href="' + loginHref() + '">' + t("Sign In", "Iniciar Sesión") + '</a>' +
      "</div>" +
      "</nav>" +
      '<main class="or-main">' +
      '<div class="or-intro" id="or-intro">' +
      '<p class="or-eyebrow"><span class="or-eyebrow-pill">' + t("Opportunity Review", "Revisión de Oportunidad") + '</span><span class="or-eyebrow-text">' + t("Confidential. Owner-led.", "Confidencial. Dirigida por el propietario.") + '</span></p>' +
      '<h1 class="or-h1">' + t("Tell us about your hotel opportunity.", "Cuéntanos Sobre Tu Oportunidad Hotelera.") + '</h1>' +
      '<p class="or-lead">' + t("Share a few details about the hotel decision you are evaluating.<br>We will review it confidentially and contact you to discuss next steps.", "Comparte algunos detalles sobre la decisión hotelera que estás evaluando.<br>La revisaremos de forma confidencial y te contactaremos para hablar sobre los próximos pasos.") + '</p>' +
      '<p class="or-reassure">' + t("No account required. Nothing is shared with brands, operators, or other parties without your approval.", "No se requiere cuenta. Nada se comparte con marcas, operadores u otras partes sin tu aprobación.") + '</p>' +
      '<p class="or-req-note">' + t("All fields are required unless marked optional.", "Todos los campos son obligatorios salvo que se indiquen como opcionales.") + '</p>' +
      "</div>" +
      '<div class="or-surface">' +
      '<section class="or-success" id="or-success" role="status" aria-live="polite" tabindex="-1">' +
      '<p class="or-eyebrow"><span class="or-eyebrow-pill">' + t("Submitted", "Enviado") + '</span><span class="or-eyebrow-text">' + t("Confidential review requested.", "Revisión confidencial solicitada.") + '</span></p>' +
      '<h2 class="or-h1">' + t("Your opportunity has been submitted.", "Tu oportunidad ha sido enviada.") + '</h2>' +
      '<p class="or-lead">' + t("Thank you. We will review the information confidentially and contact you directly regarding the next step.", "Gracias. Revisaremos la información de forma confidencial y te contactaremos directamente sobre el siguiente paso.") + '</p>' +
      '<div class="or-success-actions">' +
      '<a class="or-btn-ghost" href="' + homeHref() + '">' + t("Return to Dealality", "Volver a Dealality") + '</a>' +
      '<a class="or-btn-ghost" href="' + insightsHref() + '">' + t("Read Dealality Insights", "Leer Insights de Dealality") + '</a>' +
      "</div>" +
      "</section>" +
      '<form class="or-form" id="or-form" novalidate>' +
      '<div class="or-summary" id="or-summary" role="alert" aria-live="assertive" tabindex="-1"></div>' +
      '<section class="or-section" data-or-section="about_you" aria-labelledby="or-about-title">' +
      '<h2 class="or-section-h" id="or-about-title">' + t("1. About You", "1. Sobre Ti") + '</h2>' +
      '<p class="or-section-p">' + t("Who should we contact about the opportunity?", "¿A quién debemos contactar sobre la oportunidad?") + '</p>' +
      '<div class="or-grid">' +
      fieldMarkup("fullName", t("Full Name", "Nombre Completo"), "text", "John Smith", "name") +
      fieldMarkup("businessEmail", t("Business Email", "Email Corporativo"), "email", "you@company.com", "email") +
      fieldMarkup("company", t("Company", "Empresa"), "text", t("Company Name", "Nombre de la Empresa"), "organization") +
      fieldMarkup("role", t("Role", "Rol"), "text", t("Owner, Developer, Investor, Advisor...", "Propietario, Desarrollador, Inversionista, Asesor..."), "organization-title", true) +
      "</div></section>" +
      '<section class="or-section" data-or-section="opportunity" aria-labelledby="or-opportunity-title">' +
      '<h2 class="or-section-h" id="or-opportunity-title">' + t("2. About the Opportunity", "2. Sobre la Oportunidad") + '</h2>' +
      '<p class="or-section-p">' + t("A few details will help us understand the asset and the decision.", "Algunos detalles nos ayudan a entender el activo y la decisión.") + '</p>' +
      '<div class="or-grid">' +
      fieldMarkup("projectName", t("Hotel or Project Name", "Nombre del Hotel o Proyecto"), "text", t("Hotel or Project Name", "Nombre del Hotel o Proyecto"), "", true) +
      fieldMarkup("location", t("Hotel or Project Location", "Ubicación del Hotel o Proyecto"), "text", t("City, Country", "Ciudad, País"), "") +
      "</div></section>" +
      '<section class="or-section" data-or-section="decision" aria-labelledby="or-decision-title">' +
      '<h2 class="or-section-h" id="or-decision-title">' + t("3. What Are You Evaluating?", "3. ¿Qué Estás Evaluando?") + '</h2>' +
      '<p class="or-section-p">' + t("Tell us enough to understand the question, the opportunity, and what you hope to achieve.", "Cuéntanos lo suficiente para entender la pregunta, la oportunidad y lo que esperas lograr.") + '</p>' +
      '<fieldset style="border:0;margin:0;padding:0;min-width:0">' +
      '<legend class="or-label" id="decisionTypes-label">' + t("Decision Type", "Tipo de Decisión") + '</legend>' +
      '<span class="or-help" id="decisionTypes-help">' + t("Select all that apply.", "Selecciona todas las que apliquen.") + '</span>' +
      '<div class="or-field" id="decisionTypes-field">' +
      '<div class="or-checks" role="group" aria-labelledby="decisionTypes-label" aria-required="true">' +
      decisionMarkup() +
      "</div>" +
      '<span class="or-error" id="decisionTypes-error">' + t("Select at least one Decision Type.", "Selecciona al menos un Tipo de Decisión.") + '</span>' +
      "</div></fieldset>" +
      '<div class="or-grid" style="margin-top:18px">' +
      '<div class="or-field or-span-2" id="projectTiming-field">' +
      '<label class="or-label" for="projectTiming">' + t("Project Timing", "Momento del Proyecto") + '</label>' +
      '<select class="or-select" id="projectTiming" name="projectTiming" required aria-describedby="projectTiming-error">' +
      '<option value="">' + t("Select Timing", "Seleccionar Momento") + '</option>' +
      timingMarkup() +
      "</select>" +
      '<span class="or-error" id="projectTiming-error">' + t("Select the approximate Project Timing.", "Selecciona el Momento aproximado del Proyecto.") + '</span>' +
      "</div>" +
      '<div class="or-field or-span-2" id="description-field">' +
      '<label class="or-label" for="description">' + t("Briefly Describe the Opportunity and Decision", "Describe Brevemente la Oportunidad y la Decisión") + '</label>' +
      '<textarea class="or-textarea" id="description" name="description" required aria-describedby="description-help description-error" placeholder="' + t("For example: We are evaluating whether to reposition an existing independent hotel, pursue a soft brand, or bring in a third-party operator...", "Por ejemplo: Estamos evaluando si reposicionar un hotel independiente existente, buscar una soft brand o incorporar un operador tercero...") + '" ></textarea>' +
      '<span class="or-help" id="description-help">' + t("You may include the current situation, the paths being considered, your main objectives, and any timing considerations. Recommended: 50–300 words.", "Puedes incluir la situación actual, las opciones que estás considerando, tus objetivos principales y cualquier consideración de tiempo. Recomendado: 50–300 palabras.") + '</span>' +
      '<span class="or-error" id="description-error">' + t("Tell us briefly about the opportunity and decision.", "Cuéntanos brevemente sobre la oportunidad y la decisión.") + '</span>' +
      "</div></div></section>" +
      '<section class="or-section" data-or-section="follow_up" aria-labelledby="or-follow-title">' +
      '<h2 class="or-section-h" id="or-follow-title">' + t("4. How Should We Follow Up?", "4. ¿Cómo Debemos Dar Seguimiento?") + '</h2>' +
      '<p class="or-section-p">' + t("Choose how you would prefer us to contact you.", "Elige cómo prefieres que te contactemos.") + '</p>' +
      '<fieldset style="border:0;margin:0;padding:0;min-width:0">' +
      '<legend class="or-label">' + t("Preferred Contact Method", "Método de Contacto Preferido") + ' <span class="or-optional">(' + t("optional", "opcional") + ')</span></legend>' +
      '<div class="or-radios">' +
      radioMarkup("Email", t("Email", "Email")) +
      radioMarkup("Phone", t("Phone", "Teléfono")) +
      radioMarkup("Video Call", t("Video Call", "Videollamada")) +
      radioMarkup("No Preference", t("No Preference", "Sin Preferencia")) +
      "</div>" +
      '<div class="or-phone-wrap or-field" id="phone-field">' +
      '<label class="or-label" for="phone">' + t("Phone Number", "Número de Teléfono") + '</label>' +
      '<input class="or-input" id="phone" name="phone" type="tel" autocomplete="tel" aria-describedby="phone-error" placeholder="+1 555 123 4567">' +
      '<span class="or-error" id="phone-error">' + t("Enter the phone number you would like us to use.", "Ingresa el número de teléfono que debemos usar.") + '</span>' +
      "</div></fieldset>" +
      '<div class="or-field" id="privacyAck-field">' +
      '<label class="or-ack" for="privacyAck">' +
      '<input type="checkbox" id="privacyAck" name="privacyAck" required aria-describedby="privacyAck-error">' +
      '<span>' + t('I have read the <a href="' + privacyHref() + '" target="_blank" rel="noopener">Privacy Notice</a> and agree that Dealality may contact me about this opportunity.', 'He leído el <a href="' + privacyHref() + '" target="_blank" rel="noopener">Aviso de Privacidad</a> y acepto que Dealality me contacte sobre esta oportunidad.') + '</span>' +
      "</label>" +
      '<span class="or-error" id="privacyAck-error">' + t("Please confirm that you have read the Privacy Notice.", "Confirma que has leído el Aviso de Privacidad.") + '</span>' +
      "</div></section>" +
      '<div class="or-hp" aria-hidden="true">' +
      '<label for="website">Website</label>' +
      '<input id="website" name="website" type="text" tabindex="-1" autocomplete="off">' +
      "</div>" +
      '<div class="or-submit-block">' +
      '<button class="or-btn" id="or-submit" type="submit">' + t("Submit for Confidential Review", "Enviar para Revisión Confidencial") + '</button>' +
      '<p class="or-submit-note">' + t("We will review the information and contact you directly. Your submission will not be shared with outside parties without your approval.", "Revisaremos la información y te contactaremos directamente. Tu envío no se compartirá con terceros sin tu aprobación.") + '</p>' +
      '<p class="or-email-alt">' + t("Prefer email? Contact", "¿Prefieres email? Contacta") + ' <a href="mailto:hello@aohospitalityadvisors.com">hello@aohospitalityadvisors.com</a></p>' +
      '<div class="or-msg" id="or-message" role="alert" aria-live="assertive" tabindex="-1"></div>' +
      "</div></form></div></main>" +
      '<footer class="or-footer">© Dealality · <a href="/privacy">Privacy</a></footer>'
    );
  }

  function fieldMarkup(id, label, type, placeholder, autocomplete, optional) {
    return (
      '<div class="or-field" id="' +
      id +
      '-field">' +
      '<label class="or-label" for="' +
      id +
      '">' +
      label +
      (optional ? ' <span class="or-optional">(' + t("optional", "opcional") + ')</span>' : "") +
      "</label>" +
      '<input class="or-input" id="' +
      id +
      '" name="' +
      id +
      '" type="' +
      type +
      '" placeholder="' +
      escapeHtml(placeholder) +
      '"' +
      (autocomplete ? ' autocomplete="' + autocomplete + '"' : "") +
      (optional ? "" : " required") +
      ' aria-describedby="' +
      id +
      '-error">' +
      '<span class="or-error" id="' +
      id +
      '-error">' +
      fieldError(id) +
      "</span></div>"
    );
  }

  function radioMarkup(value, label) {
    return (
      '<label class="or-radio"><input type="radio" name="preferredContact" value="' +
      escapeHtml(value) +
      '"><span>' +
      escapeHtml(label || value) +
      "</span></label>"
    );
  }

  function fieldError(id) {
    var errors = {
      fullName: t("Enter your name.", "Ingresa tu nombre."),
      businessEmail: t("Enter a valid business email address.", "Ingresa un email corporativo válido."),
      company: t("Enter your company name.", "Ingresa el nombre de tu empresa."),
      location: t("Enter the hotel or project location.", "Ingresa la ubicación del hotel o proyecto."),
    };
    return errors[id] || "";
  }

  function markStarted() {
    if (started) return;
    started = true;
    track("opportunity_review_start");
  }

  function selectedDecisionTypes(form) {
    return Array.prototype.map.call(
      form.querySelectorAll('input[name="decisionTypes"]:checked'),
      function (input) {
        return input.value;
      }
    );
  }

  function selectedContact(form) {
    return form.querySelector('input[name="preferredContact"]:checked');
  }

  function preferredContactValue(form) {
    var selected = selectedContact(form);
    if (!selected) return "";
    if (selected.value === "Phone") {
      var phone = form.elements.phone.value.trim();
      return phone ? "Phone: " + phone : "Phone";
    }
    return selected.value;
  }

  function validateField(form, name) {
    var value;
    if (name === "decisionTypes") return selectedDecisionTypes(form).length > 0;
    if (name === "privacyAck") return form.elements.privacyAck.checked;
    if (name === "role" || name === "projectName") return true;
    if (name === "preferredContact") {
      var contact = selectedContact(form);
      return !contact || contact.value !== "Phone" || !!form.elements.phone.value.trim();
    }

    value = form.elements[name].value.trim();
    if (name === "businessEmail") return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
    if (name === "description") return value.length >= 20;
    return !!value;
  }

  function fieldNode(form, name) {
    if (name === "preferredContact") return form.querySelector("#phone-field");
    return form.querySelector("#" + name + "-field");
  }

  function fieldInputs(form, name) {
    if (name === "decisionTypes") {
      return form.querySelectorAll('input[name="decisionTypes"]');
    }
    if (name === "preferredContact") return [form.elements.phone];
    return [form.elements[name]];
  }

  function setFieldValidity(form, name, valid) {
    var node = fieldNode(form, name);
    if (node) node.classList.toggle("is-invalid", !valid);
    Array.prototype.forEach.call(fieldInputs(form, name), function (input) {
      if (input) input.setAttribute("aria-invalid", valid ? "false" : "true");
    });
  }

  function validateForm(form, showErrors) {
    var required = [
      "fullName",
      "businessEmail",
      "company",
      "location",
      "decisionTypes",
      "projectTiming",
      "description",
      "preferredContact",
      "privacyAck",
    ];
    var invalid = [];
    required.forEach(function (name) {
      var valid = validateField(form, name);
      if (showErrors) setFieldValidity(form, name, valid);
      if (!valid) invalid.push(name);
    });
    return invalid;
  }

  function sectionIsComplete(form, sectionName) {
    return SECTION_FIELDS[sectionName].every(function (name) {
      return validateField(form, name);
    });
  }

  function checkSectionCompletion(form) {
    Object.keys(SECTION_FIELDS).forEach(function (sectionName) {
      if (!completedSections[sectionName] && sectionIsComplete(form, sectionName)) {
        completedSections[sectionName] = true;
        track("opportunity_review_section_complete", { section: sectionName });
      }
    });
  }

  function showValidationSummary(form, invalid) {
    var summary = form.querySelector("#or-summary");
    var labels = {
      fullName: "Full Name",
      businessEmail: "Business Email",
      company: "Company",
      location: "Location",
      decisionTypes: "Decision Type",
      projectTiming: "Project Timing",
      description: "Brief Description",
      preferredContact: "Phone Number",
      privacyAck: "Privacy confirmation",
    };
    summary.innerHTML =
      "<strong>Please review the following fields:</strong><ul>" +
      invalid
        .map(function (name) {
          return "<li>" + escapeHtml(labels[name]) + "</li>";
        })
        .join("") +
      "</ul>";
    summary.classList.add("is-on");
    summary.focus();
  }

  function clearSubmitMessages(form) {
    var summary = form.querySelector("#or-summary");
    var message = form.querySelector("#or-message");
    summary.classList.remove("is-on");
    summary.textContent = "";
    message.className = "or-msg";
    message.textContent = "";
  }

  function payloadFor(form) {
    return {
      fullName: form.elements.fullName.value.trim(),
      businessEmail: form.elements.businessEmail.value.trim(),
      company: form.elements.company.value.trim(),
      role: form.elements.role.value.trim(),
      projectName: form.elements.projectName.value.trim(),
      location: form.elements.location.value.trim(),
      decisionTypes: selectedDecisionTypes(form),
      projectTiming: form.elements.projectTiming.value,
      description: form.elements.description.value.trim(),
      preferredContact: preferredContactValue(form),
      privacyAck: true,
      website: form.elements.website.value,
    };
  }

  function bindForm(root) {
    var form = root.querySelector("#or-form");
    var submitButton = root.querySelector("#or-submit");
    var phoneWrap = root.querySelector("#phone-field");

    form.addEventListener("focusin", markStarted);
    form.addEventListener("input", function (event) {
      markStarted();
      var name = event.target && event.target.name;
      if (name && fieldNode(form, name)) {
        setFieldValidity(form, name, validateField(form, name));
      }
      if (name === "phone") {
        setFieldValidity(
          form,
          "preferredContact",
          validateField(form, "preferredContact")
        );
      }
      checkSectionCompletion(form);
    });
    form.addEventListener("change", function (event) {
      markStarted();
      var target = event.target;
      if (target && target.name === "decisionTypes") {
        target.closest(".or-check").classList.toggle("is-on", target.checked);
        setFieldValidity(form, "decisionTypes", validateField(form, "decisionTypes"));
        if (target.checked) {
          track("opportunity_review_decision_type", {
            label: String(target.value || "").slice(0, 64),
          });
        }
      }
      if (target && target.name === "preferredContact") {
        var phoneSelected = target.value === "Phone" && target.checked;
        phoneWrap.classList.toggle("is-on", phoneSelected);
        form.elements.phone.required = phoneSelected;
        if (!phoneSelected) setFieldValidity(form, "preferredContact", true);
      }
      if (target && fieldNode(form, target.name)) {
        setFieldValidity(form, target.name, validateField(form, target.name));
      }
      checkSectionCompletion(form);
    });

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (submitting) return;

      markStarted();
      clearSubmitMessages(form);
      track("opportunity_review_submit_attempt");
      var invalid = validateForm(form, true);
      if (invalid.length) {
        track("opportunity_review_submit_failure", { outcome: "validation" });
        showValidationSummary(form, invalid);
        return;
      }

      checkSectionCompletion(form);
      submitting = true;
      submitButton.disabled = true;
      submitButton.textContent = t("Submitting...", "Enviando...");

      fetch(SUBMIT_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "omit",
        body: JSON.stringify(payloadFor(form)),
      })
        .then(function (response) {
          return response
            .json()
            .catch(function (error) {
              devLog("warn", "response body was not JSON", error);
              return {};
            })
            .then(function (data) {
              if (!response.ok) {
                var requestError = new Error(
                  data.error || t("We could not submit your opportunity. Please try again.", "No pudimos enviar tu oportunidad. Inténtalo de nuevo.")
                );
                requestError.status = response.status;
                throw requestError;
              }
              return data;
            });
        })
        .then(function () {
          track("opportunity_review_submit_success");
          form.classList.add("is-hidden");
          root.querySelector("#or-intro").classList.add("is-hidden");
          var success = root.querySelector("#or-success");
          success.classList.add("is-on");
          success.focus();
        })
        .catch(function (error) {
          devLog("error", "submission failed", {
            message: error && error.message,
            status: error && error.status,
          });
          track("opportunity_review_submit_failure", { outcome: "error" });
          var message = form.querySelector("#or-message");
          message.className = "or-msg is-err";
          message.textContent =
            (error && error.message) ||
            t("We could not submit your opportunity. Please try again or email hello@aohospitalityadvisors.com.", "No pudimos enviar tu oportunidad. Inténtalo de nuevo o escribe a hello@aohospitalityadvisors.com.");
          message.focus();
          submitting = false;
          submitButton.disabled = false;
          submitButton.textContent = t("Submit for Confidential Review", "Enviar para Revisión Confidencial");
        });
    });
  }

  function mount() {
    if (mounted) return;
    var root = document.getElementById("or-app");
    if (!root) {
      root = document.getElementById("dc-opportunity-review");
      if (root) {
        var existing = root.querySelector(".dc-opp-wrap");
        if (existing) existing.style.display = "none";
      }
    }
    if (!root) return;

    mounted = true;
    document.documentElement.classList.add("or-active", "or-page");
    document.body.classList.add("or-page");
    root.innerHTML = pageMarkup();
    bindForm(root);
    track("opportunity_review_view");
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
})();
