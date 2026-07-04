
  function renderTabGrid(data) {
    var labeled = data.tabScoresLabeled && data.tabScoresLabeled.length ? data.tabScoresLabeled : null;
    var sections = data.tabScores || data.sectionScores || {};
    var html = '<div class="drs-tab-grid">';
    if (labeled) {
      labeled.forEach(function (row) {
        var pctStr = row.score == null || row.score === "" ? "—" : esc(row.score) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr +
          '</div><div class="drs-tab-label">' +
          esc(row.label || row.id) +
          "</div></div>";
      });
    } else {
      Object.keys(sections).forEach(function (k) {
        var v = sections[k];
        var pctStr2 = v == null || v === "" ? "—" : esc(v) + "%";
        html +=
          '<div class="drs-tab-card drs-avoid-break"><div class="drs-tab-pct">' +
          pctStr2 +
          '</div><div class="drs-tab-label">' +
          esc(k) +
          "</div></div>";
      });
    }
    html += "</div>";
    return html;
  }

  function renderDetailSection(title, items, emptyMsg) {
    var html = '<section class="drs-section drs-avoid-break"><h2 class="drs-section-title">' + esc(title) + "</h2>";
    if (!items || !items.length) {
      html += '<p class="drs-muted">' + esc(emptyMsg) + "</p></section>";
      return html;
    }
    html += '<ul class="drs-detail-list">';
    items.forEach(function (item) {
      if (typeof item === "string") {
        html += "<li>" + esc(item) + "</li>";
        return;
      }
      var line = fieldLabel(item);
      var tab = item.relatedTab || item.section || "";
      if (tab) line += " (" + tab + ")";
      html += "<li>" + esc(line) + "</li>";
    });
    html += "</ul></section>";
    return html;
  }
