#!/usr/bin/env python3
"""Build Charlie Shi call package docx from Daniel Shamah template formatting."""

import shutil
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
W_NS = f"{{{W}}}"
XML_NS = "http://www.w3.org/XML/1998/namespace"

TEMPLATE = Path(r"g:\My Drive\Dealality™\Dealality - Daniel Shamah Call Package (Jul 9 2026) - English.docx")
OUTPUT = Path(r"g:\My Drive\Dealality™\Charlie Shi Call Package Jul 7 2026 - English.docx")


def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    return f"{{{W}}}{local}" if prefix == "w" else tag


def run_props(bold=False, color=None, sz="20", font="Poppins"):
    rpr = ET.Element(qn("w:rPr"))
    if bold:
        ET.SubElement(rpr, qn("w:b"))
    rfonts = ET.SubElement(rpr, qn("w:rFonts"))
    rfonts.set(qn("w:ascii"), font)
    rfonts.set(qn("w:hAnsi"), font)
    rfonts.set(qn("w:cs"), font)
    if color:
        c = ET.SubElement(rpr, qn("w:color"))
        c.set(qn("w:val"), color)
    sz_el = ET.SubElement(rpr, qn("w:sz"))
    sz_el.set(qn("w:val"), sz)
    sz_cs = ET.SubElement(rpr, qn("w:szCs"))
    sz_cs.set(qn("w:val"), sz)
    return rpr


def text_run(text, bold=False, color=None):
    r = ET.Element(qn("w:r"))
    r.append(run_props(bold=bold, color=color))
    t = ET.SubElement(r, qn("w:t"))
    if text.startswith(" ") or text.endswith(" "):
        t.set(f"{{{XML_NS}}}space", "preserve")
    t.text = text
    return r


def blank_para():
    return ET.Element(qn("w:p"))


def body_para(text):
    p = ET.Element(qn("w:p"))
    p.append(text_run(text))
    return p


def bullet_para(text):
    return body_para(f"   • {text}")


def title_para(text, color="C00000", leading_blank=False):
    p = ET.Element(qn("w:p"))
    if leading_blank:
        p.append(ET.Element(qn("w:r")))
    p.append(text_run(text, bold=True, color=color))
    return p


def subject_para(value):
    p = ET.Element(qn("w:p"))
    p.append(text_run("Subject: ", bold=True, color="C00000"))
    p.append(text_run(value, bold=True))
    return p


def section(number, title, color="C00000"):
    return title_para(f"{number}. {title.upper()}", color=color, leading_blank=True)


def add_block(paragraphs, lines, blank_between=True):
    for i, line in enumerate(lines):
        if line is None:
            paragraphs.append(blank_para())
        elif isinstance(line, tuple):
            paragraphs.append(subject_para(f"{line[0]} — {line[1]}"))
        elif line.startswith("BULLET:"):
            paragraphs.append(bullet_para(line[7:]))
        else:
            paragraphs.append(body_para(line))
        if blank_between and i < len(lines) - 1 and line is not None and lines[i + 1] is not None:
            paragraphs.append(blank_para())


def build_document_body():
    paragraphs = []

    # Title + overview
    paragraphs.append(title_para("CHARLIE SHI CALL PACKAGE — JUL 7 2026"))
    paragraphs.append(title_para("CALL OVERVIEW"))
    paragraphs.append(subject_para("Charlie Shi — Tue Jul 7, 2026 · Advisor feedback call"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Contact: Charlie Shi, AACI — Managing Director, HVS Toronto",
            "Time: Tue Jul 7, 10:00 AM Toronto / 4:00 PM Madrid",
            "Purpose: Advisor feedback, workflow refinement, and referral potential",
            "Goal: Refine where Dealality fits in the advisory ecosystem — not validate whether it should exist.",
            "Expected Outcome: Strongest use case, advisor-trust criteria, sample-output review, or specific referral.",
        ],
        blank_between=False,
    )

    paragraphs.append(title_para("BEFORE THE CALL — INTERNAL PREP", color="EE0000"))
    paragraphs.append(subject_para("One-line confidence check"))
    paragraphs.append(blank_para())
    paragraphs.append(
        body_para(
            "Be confident: Charlie is not deciding whether Dealality deserves to exist. "
            "He is helping clarify where it fits, how advisors would trust it, and who should see it next."
        )
    )

    # Section 1
    paragraphs.append(section(1, "Meeting Objective"))
    paragraphs.append(subject_para("Advisor perspective on fit, trust, and referrals"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "The goal of this call is not to ask Charlie whether Dealality should exist.",
            "The goal is to use Charlie's advisor perspective to refine where Dealality fits in the hospitality advisory ecosystem, which owner situations are strongest, what advisors would trust, and who else should see the platform next.",
            "Charlie's background with hotel owners, investors, brands, and operators makes him especially useful for pressure-testing how Dealality should work alongside advisors like HVS.",
            "The best outcome is one of the following:",
            "BULLET:Charlie identifies the strongest owner use case.",
            "BULLET:Charlie explains what would make Dealality credible to advisors.",
            "BULLET:Charlie agrees to review a sample output.",
            "BULLET:Charlie suggests a specific owner, investor, advisor, operator, or brand-development contact.",
            "BULLET:Charlie confirms whether HVS-like advisors would view Dealality as complementary.",
        ],
        blank_between=False,
    )

    # Section 2
    paragraphs.append(section(2, "Call Posture"))
    paragraphs.append(subject_para("Pilot pressure-test — not concept validation"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Do not frame this as: \"I am validating whether Dealality should exist.\"",
            "Frame it as: \"Dealality is moving from concept into pilot conversations. I'm pressure-testing the workflow with a small group of credible owners, advisors, and hospitality professionals before opening it more broadly.\"",
            "You are confident in the problem. You are using Charlie to refine:",
            "BULLET:where Dealality fits in the advisory ecosystem",
            "BULLET:which owner situations are strongest",
            "BULLET:what would make advisors trust the workflow",
            "BULLET:what would feel useful versus redundant",
            "BULLET:who else you should speak with next",
        ],
        blank_between=False,
    )

    # Section 3
    paragraphs.append(section(3, "Opening Script", color="EE0000"))
    paragraphs.append(subject_para("Say in English — opening framing"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Charlie, thank you again for making time. I really appreciate it.",
            "Dealality is moving from concept into pilot conversations. I'm speaking with a small group of owners, advisors, and hospitality professionals to pressure-test the workflow before opening it up more broadly.",
            "I'm not looking for a generic thumbs-up or thumbs-down as much as your practical perspective: where this would be useful, where it might create friction, and how it should fit alongside advisors rather than try to replace them.",
            "The core idea is that hotel owners often evaluate brand and operator options through scattered conversations, incomplete information, and relationship-driven outreach. Dealality helps organize that early decision process in one confidential, owner-controlled workflow.",
            "It helps structure the opportunity, identify missing information, compare potential brand/operator paths, and manage next steps so owners and advisors can have better-prepared conversations.",
        ],
        blank_between=True,
    )

    # Section 4
    paragraphs.append(section(4, "Simple Positioning"))
    paragraphs.append(subject_para("One-liner — process layer before or alongside advisory"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Dealality helps hotel owners and advisors structure early-stage hotel opportunities, compare brand/operator fit, identify missing information, and manage next steps in one confidential, owner-controlled workflow.",
            "I see it as a process layer before, or alongside, formal advisory conversations — not a replacement for advisory judgment.",
        ],
        blank_between=True,
    )

    # Section 5
    paragraphs.append(section(5, "What to Emphasize With Charlie"))
    paragraphs.append(subject_para("Advisor-friendly workflow"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Charlie is an advisor, so emphasize that Dealality is advisor-friendly.",
            "Key points:",
            "BULLET:Dealality does not replace advisors, HVS, lawyers, brands, or operators.",
            "BULLET:It helps owners arrive better prepared.",
            "BULLET:It organizes information before conversations become fragmented.",
            "BULLET:It can surface missing information earlier.",
            "BULLET:It can make advisor work more efficient by clarifying owner objectives and decision paths.",
            "BULLET:It can help advisors identify whether a situation is really about brand selection, operator selection, repositioning, market feasibility, valuation, asset management, or something else.",
        ],
        blank_between=False,
    )

    # Section 6
    paragraphs.append(section(6, "Demo / Walkthrough Flow", color="EE0000"))
    paragraphs.append(subject_para("Keep the walkthrough narrow — do not show everything"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Step 1 — Owner Opportunity: Imagine an owner has a hotel asset or development opportunity and is trying to decide whether to keep the current flag, convert, affiliate with a soft brand, bring in a third-party operator, reposition, or explore other options.",
            "Step 2 — Structured Intake: Instead of starting with scattered emails and calls, Dealality helps structure the opportunity: asset profile, owner objectives, timing, market, current operating situation, desired level of control, and known constraints.",
            "Step 3 — Readiness Gaps: The platform surfaces what is missing before the owner starts outreach — for example, performance data, capex assumptions, ownership objectives, operator requirements, brand standards, or decision timeline.",
            "Step 4 — Brand / Operator Fit: Dealality helps organize directional brand and operator fit. It is not pretending to make the final decision, but it creates a more disciplined comparison framework.",
            "Step 5 — Next Steps: The output is meant to help the owner and advisor decide what to do next: which conversations to have, what information to prepare, who should be involved, and what questions need to be answered.",
        ],
        blank_between=False,
    )

    # Section 7
    paragraphs.append(section(7, "Best Use-Case Prompts"))
    paragraphs.append(subject_para("Where is Dealality most useful?"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Where do you think this would be most useful? Would it be:",
            "BULLET:conversion / reflagging",
            "BULLET:soft brand evaluation",
            "BULLET:operator search",
            "BULLET:new development",
            "BULLET:repositioning",
            "BULLET:underperforming asset",
            "BULLET:owner preparing before advisor engagement",
            "BULLET:investor comparing strategic paths",
            "BULLET:brand/operator outreach preparation",
            "Then ask: Which of those situations do you see most often?",
        ],
        blank_between=False,
    )

    # Section 8
    paragraphs.append(section(8, "Advisor-Fit Questions"))
    paragraphs.append(subject_para("Most important questions for Charlie"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "BULLET:From an advisor perspective, would this feel helpful, redundant, threatening, or something else?",
            "BULLET:Where could Dealality make an advisor's work easier?",
            "BULLET:Where would advisors be skeptical?",
            "BULLET:What would Dealality need to get right for an advisor to trust the output?",
            "BULLET:What should absolutely remain advisor-led or offline?",
            "BULLET:Would this be more useful before an advisor is formally engaged, during the early advisory process, or as a client-preparation tool?",
        ],
        blank_between=False,
    )

    # Section 9
    paragraphs.append(section(9, "Product Credibility Questions"))
    paragraphs.append(subject_para("What makes output credible vs. simplistic"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "BULLET:What information would need to be captured for the output to be credible?",
            "BULLET:What are the biggest mistakes a tool like this could make?",
            "BULLET:What would make the brand/operator comparison feel too simplistic?",
            "BULLET:What would make it genuinely useful?",
            "BULLET:What would owners not know how to answer at the beginning?",
            "BULLET:What data would you expect an owner to have versus not have?",
        ],
        blank_between=False,
    )

    # Section 10
    paragraphs.append(section(10, "HVS-Specific Questions", color="EE0000"))
    paragraphs.append(subject_para("Complementary — not replacement"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "BULLET:For a firm like HVS, where could a structured workflow like Dealality be complementary?",
            "BULLET:Could Dealality help owners prepare better before engaging advisors?",
            "BULLET:Could it help clarify when a feasibility study, valuation, market study, operator search, or asset-management review is needed?",
            "BULLET:Are there situations where HVS sees owners too late, after they have already had scattered or misaligned conversations?",
        ],
        blank_between=False,
    )

    # Section 11
    paragraphs.append(section(11, "Referral Questions"))
    paragraphs.append(subject_para("Near the end — do not ask too early"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "BULLET:Based on what I've shown you, who would be the most useful next person for me to speak with — an owner, investor, operator, brand development person, or advisor?",
            "BULLET:Can you think of a specific type of owner or situation where this would be relevant?",
            "BULLET:Would you be comfortable introducing me to someone who could pressure-test this from an owner or investor perspective?",
            "BULLET:If helpful, I can send you a short forwardable blurb after the call.",
        ],
        blank_between=False,
    )

    # Sections 12-15 objection handling
    paragraphs.append(section(12, "If Charlie Asks, \"Who Pays?\""))
    paragraphs.append(subject_para("Still testing commercial model"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "I'm still testing the commercial model, and I don't want to force it too early.",
            "The strongest possibilities are owner-paid workflow access, advisor-supported pilot work, or a structured project-support model. But the key principle is that the model cannot compromise owner control or make Dealality feel like a broker or lead marketplace.",
            "Right now, I'm focused on proving which workflow is most useful and who feels the pain most clearly.",
        ],
        blank_between=True,
    )

    paragraphs.append(section(13, "If Charlie Asks, \"Is This Advisory?\""))
    paragraphs.append(subject_para("Workflow and decision-support layer"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "No — Dealality is not intended to be a broker or traditional advisor.",
            "It is a structured workflow and decision-support layer. The platform helps organize the opportunity, compare potential paths, and manage next steps, while trusted advisors, consultants, lawyers, brands, and operators still play their normal roles.",
        ],
        blank_between=True,
    )

    paragraphs.append(section(14, "If Charlie Asks Why Brands / Operators Would Participate"))
    paragraphs.append(subject_para("Better-prepared owners create better conversations"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Because better-prepared owners create better conversations.",
            "Brands and operators waste time on incomplete or poorly framed opportunities. If Dealality helps owners present clearer information and only engage relevant parties, it can reduce mismatched conversations and make outreach more efficient.",
        ],
        blank_between=True,
    )

    paragraphs.append(section(15, "If Charlie Asks Whether This Competes With HVS"))
    paragraphs.append(subject_para("Complementary — may surface need for advisor earlier"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "I see it as complementary.",
            "Dealality is not replacing feasibility, valuation, market studies, brand advisory, or strategic consulting. It helps organize the owner's opportunity and decision process so that when advisors are involved, the conversation is cleaner and better prepared.",
            "In many cases, Dealality may actually surface the need for an advisor earlier.",
        ],
        blank_between=True,
    )

    # Section 16
    paragraphs.append(section(16, "Strong Signals to Listen For"))
    paragraphs.append(subject_para("Positive vs. caution signals"))
    paragraphs.append(blank_para())
    paragraphs.append(body_para("Positive signals:"))
    for item in [
        "\"Owners need this.\"",
        "\"This would help prepare clients.\"",
        "\"This could reduce confusion.\"",
        "\"This would be useful for conversions.\"",
        "\"This could help with operator selection.\"",
        "\"I know someone you should speak with.\"",
        "\"Send me something I can forward.\"",
        "\"I'd like to see a sample output.\"",
    ]:
        paragraphs.append(bullet_para(item))
    paragraphs.append(blank_para())
    paragraphs.append(body_para("Caution signals:"))
    for item in [
        "\"Advisors already do this.\"",
        "\"Owners may not want to enter data.\"",
        "\"The output needs to be very credible.\"",
        "\"Brands may not engage unless there is a real deal.\"",
        "\"Confidentiality will be critical.\"",
        "\"You need to define the buyer.\"",
    ]:
        paragraphs.append(bullet_para(item))

    # Section 17
    paragraphs.append(section(17, "Strong Close", color="EE0000"))
    paragraphs.append(subject_para("Three asks — use case · advisor trust · referral"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Charlie, this has been very helpful.",
            "Based on what you've seen, I'd love your view on three things:",
            "First, which owner situations are the best fit for this?",
            "Second, what would Dealality need to get right for advisors to trust it?",
            "Third, who would be the most useful next person for me to speak with — an owner, investor, operator, brand development person, or advisor?",
            "If you are comfortable, I can send a short forwardable blurb after the call that makes it easy to introduce me to someone relevant.",
        ],
        blank_between=True,
    )

    # Section 18
    paragraphs.append(section(18, "Best Possible Outcome"))
    paragraphs.append(subject_para("Not just \"he likes it\""))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "The best outcome is not simply that Charlie says he likes it.",
            "The best outcome is one of these:",
            "BULLET:He identifies the strongest use case.",
            "BULLET:He names a specific owner, investor, advisor, operator, or brand-development person to speak with.",
            "BULLET:He agrees to review a sample output.",
            "BULLET:He explains what would make Dealality credible to advisors.",
            "BULLET:He clarifies whether HVS-like firms would see this as complementary.",
        ],
        blank_between=False,
    )

    # Section 19
    paragraphs.append(section(19, "Post-Call Notes Template"))
    paragraphs.append(subject_para("Charlie Shi Call Notes — July 7"))
    paragraphs.append(blank_para())
    for field in [
        "Overall signal: Positive / Neutral / Negative",
        "Most resonant use case:",
        "Least clear part:",
        "Advisor fit: Helpful / Redundant / Threatening / Complementary / Unclear",
        "What Charlie said advisors would trust:",
        "What Charlie said advisors would not trust:",
        "Best-fit owner situations:",
        "Potential buyer/user:",
        "Referral potential:",
        "Specific people or companies mentioned:",
        "Follow-up promised:",
        "Materials to send:",
        "Next action:",
        "Airtable update needed:",
    ]:
        paragraphs.append(body_para(field))

    # Section 20
    paragraphs.append(section(20, "Follow-Up Email Template After a Good Call"))
    paragraphs.append(subject_para("Send after positive call"))
    paragraphs.append(blank_para())
    add_block(
        paragraphs,
        [
            "Hi Charlie,",
            "Thank you again for taking the time today. I really appreciated your perspective.",
            "Your feedback was helpful as I think through where Dealality fits best alongside advisors and where the workflow can be most useful for owners evaluating brand, operator, affiliation, or repositioning decisions.",
            "As discussed, I'll follow up with [insert promised item: short overview / sample output / forwardable blurb / specific question].",
            "I'd also be grateful for any introductions you think would be useful — particularly to owners, investors, advisors, operators, or brand development professionals who regularly see these early-stage decision points.",
            "Thanks again,",
            "Joan",
        ],
        blank_between=True,
    )

    # Section 21 / Tone guardrails
    paragraphs.append(title_para("TONE GUARDRAILS", color="EE0000", leading_blank=True))
    paragraphs.append(subject_para("Remember on the call"))
    paragraphs.append(blank_para())
    paragraphs.append(
        body_para(
            "Be confident: Charlie is not deciding whether Dealality deserves to exist. "
            "He is helping clarify where it fits, how advisors would trust it, and who should see it next."
        )
    )

    return paragraphs


def replace_document_xml(template_path: Path, output_path: Path, paragraphs):
    shutil.copy2(template_path, output_path)

    with zipfile.ZipFile(template_path, "r") as zin:
        original_doc = zin.read("word/document.xml")

    root = ET.fromstring(original_doc)
    body = root.find(qn("w:body"))
    if body is None:
        raise RuntimeError("Could not find document body")

    # Remove existing paragraphs/tables but keep sectPr
    sect_pr = body.find(qn("w:sectPr"))
    for child in list(body):
        body.remove(child)

    for p in paragraphs:
        body.append(p)
    if sect_pr is not None:
        body.append(sect_pr)

    new_xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    # Rewrite docx in place
    temp_path = output_path.with_suffix(".tmp.docx")
    with zipfile.ZipFile(output_path, "r") as zin, zipfile.ZipFile(temp_path, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == "word/document.xml":
                data = new_xml
            zout.writestr(item, data)

    temp_path.replace(output_path)


def main():
    paragraphs = build_document_body()
    replace_document_xml(TEMPLATE, OUTPUT, paragraphs)
    print(f"Created: {OUTPUT}")
    print(f"Paragraphs: {len(paragraphs)}")


if __name__ == "__main__":
    main()
