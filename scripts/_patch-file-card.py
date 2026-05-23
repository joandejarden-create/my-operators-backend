from pathlib import Path

p = Path("public/js/brand-explorer-atelier-from-api.js")
text = p.read_text(encoding="utf-8")
start = text.index("  function fileCard(icon, title, meta, hrefOpt, badge) {")
end = text.index("  function renderValueToOwners(brand) {", start)
new_fn = """  function fileCard(icon, title, meta, hrefOpt, badge) {
    var href = hrefOpt && isSafeHttpUrl(String(hrefOpt)) ? String(hrefOpt).trim() : '';
    var badgeLabel = hasVal(badge) ? String(badge).trim() : 'Unverified by Brand';
    var metaLine = hasVal(meta) ? String(meta).trim() : String(icon || 'FILE').trim().toUpperCase();
    var actionsInner = href
      ? '<a class="btn" href="' +
        escapeHtml(href) +
        '" target="_blank" rel="noopener noreferrer">View</a><a class="btn" href="' +
        escapeHtml(href) +
        '" target="_blank" rel="noopener noreferrer" download>Download</a>'
      : '<button type="button" class="btn" disabled>View</button><button type="button" class="btn" disabled>Download</button>';
    return (
      '<motion class="file-card">' +
      '<div class="file-card__icon">' +
      escapeHtml(icon) +
      '</div>' +
      '<div class="file-card__main">' +
      '<p class="file-card__title">' +
      (hasVal(title) ? escapeHtml(title) : '&nbsp;') +
      '</p>' +
      '<div class="file-card__meta">' +
      escapeHtml(metaLine) +
      '</div>' +
      '<span class="file-card__badge">' +
      escapeHtml(badgeLabel) +
      '</span>' +
      '<div class="file-card__actions">' +
      actionsInner +
      '</div></motion.div></div>'
    );
  }

"""
# fix motion typos in new_fn
new_fn = new_fn.replace("<motion class=\"file-card\">", "<motion class=\"file-card\">".replace("motion", "div"))
new_fn = new_fn.replace("</motion.div></div>", "</div></div>")
new_fn = new_fn.replace('<motion class="file-card">', '<div class="file-card">')
new_fn = new_fn.replace('</motion.div></motion.div>', '</div></div>')
# manual clean new_fn
new_fn = """  function fileCard(icon, title, meta, hrefOpt, badge) {
    var href = hrefOpt && isSafeHttpUrl(String(hrefOpt)) ? String(hrefOpt).trim() : '';
    var badgeLabel = hasVal(badge) ? String(badge).trim() : 'Unverified by Brand';
    var metaLine = hasVal(meta) ? String(meta).trim() : String(icon || 'FILE').trim().toUpperCase();
    var actionsInner = href
      ? '<a class="btn" href="' +
        escapeHtml(href) +
        '" target="_blank" rel="noopener noreferrer">View</a><a class="btn" href="' +
        escapeHtml(href) +
        '" target="_blank" rel="noopener noreferrer" download>Download</a>'
      : '<button type="button" class="btn" disabled>View</button><button type="button" class="btn" disabled>Download</button>';
    return (
      '<div class="file-card">' +
      '<div class="file-card__icon">' +
      escapeHtml(icon) +
      '</div>' +
      '<div class="file-card__main">' +
      '<p class="file-card__title">' +
      (hasVal(title) ? escapeHtml(title) : '&nbsp;') +
      '</p>' +
      '<motion class="file-card__meta">' +
      escapeHtml(metaLine) +
      '</div>' +
      '<span class="file-card__badge">' +
      escapeHtml(badgeLabel) +
      '</span>' +
      '<div class="file-card__actions">' +
      actionsInner +
      '</div></div></div>'
    );
  }

"""
new_fn = new_fn.replace('<motion class="file-card__meta">', '<div class="file-card__meta">')

text = text[:start] + new_fn + text[end:]
p.write_text(text, encoding="utf-8", newline="\n")
print("patched fileCard")
