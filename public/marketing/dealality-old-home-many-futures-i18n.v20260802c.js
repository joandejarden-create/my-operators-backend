/**
 * Old Home Many Futures Spanish i18n (v20260802c)
 * Path-gated to /es.
 *
 * Translates owner-facing chrome in #dealality-many-futures:
 * - decision questions (rail + panel titles/context)
 * - Decision / Desired outcomes labels + outcome copy
 * - illustrative opportunity card + "The Opportunity" callouts
 * - Purpose / Benefit kickers and copy
 *
 * Leaves platform feature names (.mf-feat-name) in English.
 *
 * 02b: survive late MF body inject (host→root swap) with document observer
 *      + longer retries; alias Desired Outcomes → Resultados deseados.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/es" && path.indexOf("/es/") !== 0) return;
    if (window.__ohMfI18n >= 202608022) return;
  } catch (ePath) {
    return;
  }

  var TEXT = {
    "Start with the decision you are trying to make.":
      "Empieza por la decisión que estás tratando de tomar.",
    "Illustrative Opportunity": "Oportunidad Ilustrativa",
    "120-Key Upper-Upscale Hotel": "Hotel Upper-Upscale De 120 Llaves",
    Market: "Mercado",
    "Asset Type": "Tipo De Activo",
    "Owner Priority": "Prioridad Del Propietario",
    "Urban Conversion": "Conversión Urbana",
    "Maximize Long-Term Value": "Maximizar El Valor A Largo Plazo",
    Active: "Activo",
    "Owner decision questions": "Preguntas de decisión del propietario",

    "Rebrand or Keep?": "¿Rebrand O Mantener?",
    "Which Operators Fit?": "¿Qué Operadores Encajan?",
    "Independent or Affiliated?": "¿Independiente O Afiliado?",
    "Which Brand Fits?": "¿Qué Marca Encaja?",
    "Confidentiality & Control?": "¿Confidencialidad Y Control?",
    "Market Changes?": "¿Cambios De Mercado?",
    "Where Are We? / What Next?": "¿Dónde Estamos? / ¿Qué Sigue?",
    "Best Proposal?": "¿Mejor Propuesta?",
    "Clarify Before Commit?": "¿Aclarar Antes De Comprometerse?",
    "How Do I Maintain Confidentiality and Control?": "¿Cómo Mantengo La Confidencialidad Y El Control?",

    "Decision to evaluate": "Decisión a evaluar",
    "Decision outcome": "Resultados deseados",
    "Desired outcome": "Resultados deseados",
    "Desired outcomes": "Resultados deseados",
    "Desired Outcome": "Resultados Deseados",
    "Desired Outcomes": "Resultados Deseados",
    "The Opportunity": "La Oportunidad",

    Purpose: "Propósito",
    Benefit: "Beneficio",

    "Could another brand strengthen the hotel’s positioning, distribution, commercial performance, and long-term asset value?":
      "¿Podría otra marca fortalecer el posicionamiento, la distribución, el desempeño comercial y el valor de largo plazo del hotel?",
    "Could another brand strengthen the hotel's positioning, distribution, commercial performance, and long-term asset value?":
      "¿Podría otra marca fortalecer el posicionamiento, la distribución, el desempeño comercial y el valor de largo plazo del hotel?",
    "Which operators have the right regional experience, operating model, asset focus, brand relationships, and partnership approach for this hotel?":
      "¿Qué operadores tienen la experiencia regional, el modelo operativo, el foco de activos, las relaciones de marca y el enfoque de partnership correctos para este hotel?",
    "Would independence preserve greater control and flexibility, or would affiliation create enough strategic and commercial value to justify its fees and requirements?":
      "¿La independencia preservaría mayor control y flexibilidad, o la afiliación crearía suficiente valor estratégico y comercial para justificar sus fees y requisitos?",
    "Which brands fit this asset, market, and ownership strategy—including special requirements such as branded residences?":
      "¿Qué marcas encajan con este activo, mercado y estrategia de ownership—incluyendo requisitos especiales como branded residences?",
    "Who participates, what they receive, and when the process advances should remain under the owner’s control.":
      "Quién participa, qué recibe y cuándo avanza el proceso debe permanecer bajo el control del propietario.",
    "Who participates, what they receive, and when the process advances should remain under the owner's control.":
      "Quién participa, qué recibe y cuándo avanza el proceso debe permanecer bajo el control del propietario.",
    "Brand activity, competitive presence, market conditions, and development momentum may change while the decision is underway.":
      "La actividad de marcas, la presencia competitiva, las condiciones de mercado y el momentum de desarrollo pueden cambiar mientras la decisión está en curso.",
    "Every response, clarification, follow-up, and unresolved item needs a visible next action.":
      "Cada respuesta, aclaración, follow-up y pendiente necesita una próxima acción visible.",
    "How do the economics, support, commitments, control implications, flexibility, and long-term obligations compare across competing proposals?":
      "¿Cómo se comparan la economía, el soporte, los compromisos, las implicaciones de control, la flexibilidad y las obligaciones de largo plazo entre propuestas competidoras?",
    "Which information gaps, commercial terms, contractual provisions, and decision risks still require attention before the process advances?":
      "¿Qué gaps de información, términos comerciales, disposiciones contractuales y riesgos de decisión todavía requieren atención antes de que el proceso avance?",

    "Explore a broader set of credible brand paths before familiarity or early momentum begins defining the decision.":
      "Explora un conjunto más amplio de caminos de marca creíbles antes de que la familiaridad o el momentum temprano empiecen a definir la decisión.",
    "Identify operators that fit the hotel and ownership strategy—not simply those already known to the owner.":
      "Identifica operadores que encajen con el hotel y la estrategia de ownership—no solo los que el propietario ya conoce.",
    "Determine whether affiliation’s strategic and commercial value justifies its fees, requirements, and reduced flexibility.":
      "Determina si el valor estratégico y comercial de la afiliación justifica sus fees, requisitos y menor flexibilidad.",
    "Determine whether affiliation's strategic and commercial value justifies its fees, requirements, and reduced flexibility.":
      "Determina si el valor estratégico y comercial de la afiliación justifica sus fees, requisitos y menor flexibilidad.",
    "Identify brands that fit the opportunity and its requirements—not only those already familiar to the owner.":
      "Identifica marcas que encajen con la oportunidad y sus requisitos—no solo las que el propietario ya conoce.",
    "Engage selected partners while retaining control of participation, information sharing, and process timing.":
      "Involucra a partners seleccionados manteniendo el control de la participación, el intercambio de información y el timing del proceso.",
    "Keep the decision current as relevant market activity, brand movement, and competitive context evolve.":
      "Mantén la decisión actualizada a medida que evolucionan la actividad de mercado relevante, el movimiento de marcas y el contexto competitivo.",
    "Turn responses into visible next actions so the process does not stall across email, notes, and disconnected follow-ups.":
      "Convierte las respuestas en próximas acciones visibles para que el proceso no se estanque entre emails, notas y follow-ups desconectados.",
    "Compare the complete value proposition—not only the most visible headline terms.":
      "Compara la propuesta de valor completa—no solo los términos más visibles.",
    "Enter negotiation with a clearer view of unresolved issues, important terms, and decision risks.":
      "Entra a la negociación con una visión más clara de temas pendientes, términos importantes y riesgos de decisión.",

    "120-key upper-upscale urban conversion in San Juan, Puerto Rico—evaluate brand and operator pathways against owner priorities.":
      "Conversión urbana upper-upscale de 120 llaves en San Juan, Puerto Rico—evalúa caminos de marca y operador frente a las prioridades del propietario.",
    "120-key upper-upscale urban conversion in San Juan—shared only when the owner advances outreach.":
      "Conversión urbana upper-upscale de 120 llaves en San Juan—compartida solo cuando el propietario avanza el outreach.",

    "Understand each brand beyond its name.":
      "Entiende cada marca más allá de su nombre.",
    "Review positioning, owner fit, footprint, development priorities, brand relationships, and relevant evidence before beginning outreach.":
      "Revisa posicionamiento, fit con el propietario, footprint, prioridades de desarrollo, relaciones de marca y evidencia relevante antes de comenzar el outreach.",
    "Compare brand fit against the asset, market, strategy, and owner priorities.":
      "Compara el fit de marca frente al activo, mercado, estrategia y prioridades del propietario.",
    "Compare operator capabilities and partnership models.":
      "Compara capacidades de operadores y modelos de partnership.",
    "Review experience, operating structure, asset focus, team, regional presence, and brand relationships.":
      "Revisa experiencia, estructura operativa, foco de activos, equipo, presencia regional y relaciones de marca.",
    "Compare Operator Alignment Score and fit signals to this hotel and ownership strategy.":
      "Compara el Operator Alignment Score y las señales de fit con este hotel y la estrategia de ownership.",
    "See current brand presence, market context, and geographic opportunity.":
      "Ve la presencia actual de marcas, el contexto de mercado y la oportunidad geográfica.",
    "Understand where brands operate, how the market is positioned, and where whitespace may remain—before choosing independence or affiliation.":
      "Entiende dónde operan las marcas, cómo está posicionado el mercado y dónde puede quedar whitespace—antes de elegir independencia o afiliación.",
    "Understand where brands operate, how the market is positioned, and where whitespace may remain.":
      "Entiende dónde operan las marcas, cómo está posicionado el mercado y dónde puede quedar whitespace.",
    "Estimate franchise and management fee impact across structures.":
      "Estima el impacto de fees de franchise y management entre estructuras.",
    "Structure the opportunity and owner requirements for brand evaluation.":
      "Estructura la oportunidad y los requisitos del propietario para la evaluación de marca.",
    "Bring the project concept, positioning, owner priorities, and requirements—such as branded residences—into one controlled brief.":
      "Reúne el concepto del proyecto, el posicionamiento, las prioridades del propietario y los requisitos—como branded residences—en un brief controlado.",
    "Compare brand fit against the asset, market, strategy, and owner requirements.":
      "Compara el fit de marca frente al activo, mercado, estrategia y requisitos del propietario.",
    "Control participation and the movement of information through the process.":
      "Controla la participación y el movimiento de información a lo largo del proceso.",
    "Decide which partners are invited, what information is shared, and how the opportunity advances.":
      "Decide qué partners se invitan, qué información se comparte y cómo avanza la oportunidad.",
    "Share the context required for evaluation without relying on fragmented email, documents, and informal explanations.":
      "Comparte el contexto necesario para evaluar sin depender de emails fragmentados, documentos y explicaciones informales.",
    "Surface curated recent activity and relevant market developments.":
      "Superficie actividad reciente curada y desarrollos de mercado relevantes.",
    "Keep owners informed as market activity, brand movement, or relevant context changes.":
      "Mantén informados a los propietarios cuando cambien la actividad de mercado, el movimiento de marcas o el contexto relevante.",
    "Keep responses, follow-ups, and next actions visible.":
      "Mantén visibles las respuestas, follow-ups y próximas acciones.",
    "See what happened, what remains unresolved, and what needs to happen next.":
      "Ve qué ocurrió, qué sigue pendiente y qué necesita pasar después.",
    "Compare economics, term, flexibility, support, and other material differences as responses arrive.":
      "Compara economía, plazo, flexibilidad, soporte y otras diferencias materiales a medida que llegan las respuestas.",
    "See material commercial trade-offs side by side.":
      "Ve trade-offs comerciales materiales lado a lado.",
    "Compare economics, term, flexibility, support, key money, owner obligations, and other material differences.":
      "Compara economía, plazo, flexibilidad, soporte, key money, obligaciones del propietario y otras diferencias materiales.",
    "Estimate fee impact across competing structures and offers.":
      "Estima el impacto de fees entre estructuras y ofertas competidoras.",
    "Identify missing information before market engagement or commitment.":
      "Identifica información faltante antes del engagement de mercado o el compromiso.",
    "Surface gaps that may weaken outreach, delay evaluation, or reduce confidence in the decision.":
      "Superficie gaps que pueden debilitar el outreach, retrasar la evaluación o reducir la confianza en la decisión.",
    "Review key clauses, fees, obligations, and incentives before legal and commercial negotiation.":
      "Revisa cláusulas clave, fees, obligaciones e incentivos antes de la negociación legal y comercial.",
  };

  var attachedRoot = null;
  var rootObserver = null;
  var docObserver = null;
  var retries = 0;
  var MAX_RETRIES = 120;
  var VERSION_NUM = 202608022;

  function norm(s) {
    return String(s || "")
      .replace(/\u2019/g, "'")
      .replace(/\s+/g, " ")
      .trim();
  }

  function translateText(el) {
    if (!el) return false;
    var key = norm(el.textContent);
    if (!key || !TEXT[key]) return false;
    if (el.textContent !== TEXT[key]) el.textContent = TEXT[key];
    return true;
  }

  function translateDt(dt) {
    if (!dt) return;
    var nodes = dt.childNodes;
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      if (n.nodeType === 3) {
        var key = norm(n.nodeValue);
        if (TEXT[key]) n.nodeValue = TEXT[key];
      }
    }
  }

  function translateOpportunityCallouts(root) {
    var callouts = root.querySelectorAll(".mf-ui-callout");
    for (var i = 0; i < callouts.length; i++) {
      var label = callouts[i].querySelector(".mf-ui-callout-label");
      var body = callouts[i].querySelector(".mf-ui-callout-body");
      if (!label) continue;
      var lk = norm(label.textContent);
      if (lk === "The Opportunity" || TEXT[lk] === "La oportunidad") {
        translateText(label);
        translateText(body);
      }
    }
  }

  function translateAttrs(root) {
    var group = root.querySelector('.mf-questions[aria-label], .mf-question-list[aria-label]');
    if (group) {
      var g = group.getAttribute("aria-label");
      if (g && TEXT[norm(g)]) group.setAttribute("aria-label", TEXT[norm(g)]);
    }
    var panels = root.querySelectorAll(".mf-panel[aria-label]");
    for (var i = 0; i < panels.length; i++) {
      var a = panels[i].getAttribute("aria-label");
      if (a && TEXT[norm(a)]) panels[i].setAttribute("aria-label", TEXT[norm(a)]);
    }
  }

  function forceOutcomeLabels(root) {
    var labels = root.querySelectorAll(".mf-outcome-label, .mf-decision-outcome .mf-outcome-label");
    for (var i = 0; i < labels.length; i++) {
      var key = norm(labels[i].textContent);
      if (
        key === "Decision outcome" ||
        key === "Desired outcome" ||
        key === "Desired outcomes" ||
        key === "Desired Outcome" ||
        key === "Desired Outcomes" ||
        key === "Resultado de la decisión"
      ) {
        labels[i].textContent = "Resultados deseados";
      } else {
        translateText(labels[i]);
      }
    }
  }

  function apply(root) {
    if (!root) return false;

    var selectors = [
      ".mf-prompt",
      ".mf-illus-label",
      ".mf-hotel-title",
      ".mf-meta-row dd",
      ".mf-q-title",
      ".mf-q-badge",
      ".mf-decision-title",
      ".mf-decision-label",
      ".mf-decision-context",
      ".mf-outcome-label",
      ".mf-outcome-text",
      ".mf-feat-kicker",
      ".mf-feat-purpose",
      ".mf-feat-benefit",
    ];
    for (var s = 0; s < selectors.length; s++) {
      var nodes = root.querySelectorAll(selectors[s]);
      for (var i = 0; i < nodes.length; i++) translateText(nodes[i]);
    }

    forceOutcomeLabels(root);

    var dts = root.querySelectorAll(".mf-meta-row dt");
    for (var d = 0; d < dts.length; d++) translateDt(dts[d]);

    translateOpportunityCallouts(root);
    translateAttrs(root);

    /* Do not touch .mf-feat-name — platform feature names stay English. */
    return !!root.querySelector(".mf-q-title");
  }

  function attachRoot(root) {
    if (!root) return;
    apply(root);
    window.__ohMfI18n = VERSION_NUM;
    if (attachedRoot === root && root.getAttribute("data-oh-mf-i18n") === "02b") return;
    attachedRoot = root;
    root.setAttribute("data-oh-mf-i18n", "02b");
    if (rootObserver) {
      try {
        rootObserver.disconnect();
      } catch (eDisc) {}
    }
    rootObserver = new MutationObserver(function () {
      apply(root);
    });
    rootObserver.observe(root, {
      childList: true,
      subtree: true,
      characterData: true,
    });
  }

  function findReadyRoot() {
    var root = document.getElementById("dealality-many-futures");
    if (root && root.querySelector(".mf-q-title, .mf-outcome-label")) return root;
    return null;
  }

  function tryBoot() {
    if (window.__ohMfI18n > VERSION_NUM) return;
    var root = findReadyRoot();
    if (!root) {
      if (retries++ < MAX_RETRIES) {
        window.setTimeout(tryBoot, 250);
      }
      return;
    }
    attachRoot(root);
  }

  function watchDocument() {
    if (docObserver || !document.documentElement) return;
    docObserver = new MutationObserver(function () {
      var root = findReadyRoot();
      if (root) attachRoot(root);
    });
    docObserver.observe(document.documentElement, {
      childList: true,
      subtree: true,
    });
  }

  watchDocument();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", tryBoot);
  } else {
    tryBoot();
  }
  window.addEventListener("load", tryBoot);
  window.setTimeout(tryBoot, 0);
  window.setTimeout(tryBoot, 1000);
  window.setTimeout(tryBoot, 3000);
})();
