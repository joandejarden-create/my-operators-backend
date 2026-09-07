#!/usr/bin/env python3
"""Repair broken string concatenation in OR i18n build output."""
from pathlib import Path
import re
import subprocess

p = Path("/workspace/public/marketing/dealality-opportunity-review.v20260802a.js")
src = p.read_text()

src = src.replace('href="" + homeHref() + ""', 'href="\' + homeHref() + \'"')
src = src.replace('href="" + loginHref() + ""', 'href="\' + loginHref() + \'"')
src = src.replace('href="" + insightsHref() + ""', 'href="\' + insightsHref() + \'"')

# Fix: ...title">" + t(  ->  ...title">' + t(
src = re.sub(r'">" \+ t\(', "'>\' + t(", src)

# Fix closers after t(): + "</tag>  -> + '</tag>
src = re.sub(r"(\+ t\([^;]+?\)) \+ \"</", r"\1 + '</", src)

# placeholder="" + t( -> placeholder="' + t(
src = src.replace('placeholder="" + t(', 'placeholder="\' + t(')
# after placeholder t() + ""  -> + '"
src = re.sub(
    r"(placeholder=\"' \+ t\([^)]+\) \+) \"\"",
    r"\1'\"",
    src,
)

# Preferred contact legend may be: '" + t(  inside a larger quote
# Check and fix: legend class="or-label">" + t(
src = src.replace(
    'legend class="or-label">" + t(',
    'legend class="or-label">\' + t(',
)

p.write_text(src)
r = subprocess.run(["node", "--check", str(p)], capture_output=True, text=True)
print("syntax", r.returncode)
if r.stderr:
    print(r.stderr[:1000])

for i, line in enumerate(src.splitlines(), 1):
    if any(
        k in line
        for k in (
            "homeHref()",
            "Sobre Ti",
            "Preferred Contact",
            "privacyHref()",
            "placeholder=",
        )
    ):
        if "t(" in line or "Href" in line:
            print(f"{i}: {line[:180]}")
