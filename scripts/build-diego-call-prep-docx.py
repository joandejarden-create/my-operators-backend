#!/usr/bin/env python3
"""Build Diego Call Prep Package docx from Dealality pilot call script template."""

from __future__ import annotations

import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"

TEMPLATE = Path(
    r"g:\My Drive\Dealality™\Dealality - Pilot Call Script (Pilot Wave 1) - English.docx"
)
OUTPUT = Path(
    r"g:\My Drive\Dealality™\Diego Call Prep Package — San José del Cabo (Mijares 32).docx"
)


def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    return f"{{{W[1:-1] if prefix == 'w' else prefix}}}{local}" if ":" in tag else tag


def w(tag: str) -> str:
    return f"{W}{tag}"


def run(text: str, *, bold: bool = False, color: str | None = None, size: int = 20) -> ET.Element:
    r = ET.Element(w("r"))
    rpr = ET.SubElement(r, w("rPr"))
    fonts = ET.SubElement(rpr, w("rFonts"))
    fonts.set(w("ascii"), "Poppins")
    fonts.set(w("hAnsi"), "Poppins")
    if bold:
        ET.SubElement(rpr, w("b"))
    if color:
        c = ET.SubElement(rpr, w("color"))
        c.set(w("val"), color)
    sz = ET.SubElement(rpr, w("sz"))
    sz.set(w("val"), str(size))
    t = ET.SubElement(r, w("t"))
    if text.startswith(" ") or text.endswith(" "):
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    t.text = text
    return r


def empty_run() -> ET.Element:
    r = ET.Element(w("r"))
    return r


def paragraph(*runs, empty_first: bool = False) -> ET.Element:
    p = ET.Element(w("p"))
    if empty_first:
        p.append(empty_run())
    for item in runs:
        if isinstance(item, ET.Element):
            p.append(item)
        elif item is None:
            p.append(ET.Element(w("p")))
        else:
            raise TypeError(item)
    return p


def header_para(title: str, *, accent: str = "C00000") -> ET.Element:
    return paragraph(empty_run(), run(title, bold=True, color=accent))


def subject_para(subject: str) -> ET.Element:
    return paragraph(
        run("Subject: ", bold=True, color="C00000"),
        run(subject, bold=True),
    )


def body_para(text: str) -> ET.Element:
    return paragraph(run(text))


def blank_para() -> ET.Element:
    return ET.Element(w("p"))


def content_cell(*blocks) -> ET.Element:
    tc = ET.Element(w("tc"))
    tcpr = ET.SubElement(tc, w("tcPr"))
    tcw = ET.SubElement(tcpr, w("tcW"))
    tcw.set(w("type"), "dxa")
    tcw.set(w("w"), "10081")
    tc.append(empty_run_para())
    for block in blocks:
        if block == "blank":
            tc.append(blank_para())
        elif isinstance(block, str):
            tc.append(body_para(block))
        elif isinstance(block, ET.Element):
            tc.append(block)
        elif block is None:
            tc.append(blank_para())
    return tc


def empty_run_para() -> ET.Element:
    return paragraph(empty_run())


def header_cell(title: str, subject: str | None = None, *, accent: str = "C00000") -> ET.Element:
    tc = ET.Element(w("tc"))
    tcpr = ET.SubElement(tc, w("tcPr"))
    tcw = ET.SubElement(tcpr, w("tcW"))
    tcw.set(w("type"), "dxa")
    tcw.set(w("w"), "10081")
    tc.append(header_para(title, accent=accent))
    if subject:
        tc.append(subject_para(subject))
    return tc


def table_row(cell: ET.Element) -> ET.Element:
    tr = ET.Element(w("tr"))
    tr.append(cell)
    return tr


def section(title: str, subject: str | None, content: list, *, accent: str = "C00000") -> list:
    rows = [table_row(header_cell(title, subject, accent=accent))]
    rows.append(table_row(content_cell(*content)))
    return rows


def build_sections() -> list:
    rows: list = []

    rows.append(table_row(header_cell("DIEGO CALL PREP PACKAGE", None)))
    rows.append(table_row(content_cell(
        body_para("San José del Cabo Mixed-Use Project / Mijares 32"),
        "blank",
        body_para("AO-led. Dealality-powered. Owner-focused."),
    )))

    rows.extend(section(
        "1. CALL OBJECTIVE",
        "Project discovery — not a generic validation call",
        [
            body_para(
                "This is a real project discovery call, not a generic Dealality validation call."
            ),
            "blank",
            body_para(
                "Your goal is to understand whether Diego needs help moving from:"
            ),
            "blank",
            body_para('"We have a compelling mixed-use hotel/residential concept"'),
            "blank",
            body_para("to:"),
            "blank",
            body_para(
                '"We know which brand/operator structure to pursue, who to approach, what to ask, and how to compare responses."'
            ),
            "blank",
            body_para("The ideal outcome:"),
            "blank",
            body_para(
                "Diego agrees that AO Hospitality Advisors should support a focused brand/operator market approach, powered by Dealality's structured workflow."
            ),
            "blank",
            body_para("This serves both businesses:"),
            body_para("AO wins a paid advisory engagement."),
            body_para(
                "Dealality gets a real owner-side workflow case study for a mixed-use hotel/residential brand/operator decision."
            ),
            "blank",
            body_para(
                "The project is a strong fit because the deck frames Mijares 32 as a boutique hospitality-led mixed-use hotel and condo project in historic San José del Cabo, with hotel, branded residences, destination F&B, rooftop amenities, operating assumptions, and a development timeline that connects hotel brand, operator, sales, debt, equity, marketing, and construction milestones."
            ),
        ],
    ))

    rows.extend(section(
        "2. YOUR CORE POSITIONING",
        "AO-led. Dealality-powered. Owner-focused.",
        [
            body_para("Do not lead with Dealality."),
            "blank",
            body_para("Lead with AO judgment."),
            "blank",
            body_para("Use this:"),
            "blank",
            body_para(
                "AO Hospitality Advisors would lead the strategic work. Dealality would support the process as the structured workflow to organize the opportunity, clarify decision criteria, identify information gaps, compare brand/operator paths, and manage the outreach process."
            ),
            "blank",
            body_para("The positioning:"),
            body_para("AO-led. Dealality-powered. Owner-focused."),
            "blank",
            body_para("Avoid saying:"),
            body_para("This is a Dealality pilot."),
            "blank",
            body_para("Say:"),
            body_para(
                "This is a structured brand/operator market approach for a mixed-use hotel and residential project."
            ),
        ],
    ))

    rows.extend(section(
        "3. YOUR OPENING SCRIPT",
        "Say this near the start of the call",
        [
            body_para(
                "Diego, thank you again for sending the deck and for making the time. German gave me helpful context, and I reviewed the project as a boutique hotel and residential opportunity in the historic center of San José del Cabo."
            ),
            "blank",
            body_para(
                "What stood out to me is that this does not look like a simple hotel brand question. The hotel, residences, restaurant, operating model, buyer story, and development timeline all seem connected."
            ),
            "blank",
            body_para(
                "I do not want to ask you to repeat what is already in the deck. I'd rather use the time to understand what is fixed, what is still open, and where the brand or operator decision can create the most value."
            ),
            "blank",
            body_para(
                "My goal is not to jump straight to recommendations, but to understand what you are solving for so we can think through the right decision framework."
            ),
        ],
    ))

    rows.extend(section(
        "4. RUN OF SHOW — 0–5 MINUTES",
        "Warm opening and agenda",
        [
            body_para("Objective: Set the tone as advisory, not sales."),
            "blank",
            body_para("Say:"),
            body_para(
                "I thought we could use the time in three parts: first, I'd like to understand where the project stands today; second, clarify the brand/operator questions that are still open; and third, if helpful, discuss what a structured next step could look like."
            ),
            "blank",
            body_para("Ask:"),
            body_para("Does that sound like a good use of the time?"),
            "blank",
            body_para("Likely answer:"),
            body_para("Yes, that works."),
            "blank",
            body_para("Your response:"),
            body_para(
                "Great. I reviewed the deck, so I'll try not to ask you to repeat what is already there. I'll focus more on what is fixed, what is still flexible, and what decisions need to happen next."
            ),
        ],
        accent="EE0000",
    ))

    run_of_show_blocks = [
        ("5–15 minutes — Current project status", "Understand where they are in the process.", [
            ("Ask:", "The deck gives a clear concept, but where is the project today in terms of planning, design, approvals, capital, and timing?"),
            ("Possible answer A:", "We are still early. We have the concept and site, but a lot is still open.", "That is actually a good time to structure the brand/operator question, because the wrong brand or operating model can create design, economics, or control issues later."),
            ("Possible answer B:", "We are moving quickly and need to make decisions soon.", "Then the value is in narrowing the field quickly — not broad outreach, but a focused process that identifies which structures and partners are actually worth testing."),
            ("Possible answer C:", "We already have some parties in mind.", "That is helpful. I would still want to understand whether those names fit the structure you need, or whether the structure is being shaped by the names already in the conversation."),
        ]),
        ("15–25 minutes — Vision and role of the hotel", "Understand what the hotel is supposed to do.", [
            ("Ask:", "The deck frames the project as a boutique, hospitality-led urban project rooted in downtown San José's culture, art, gastronomy, and walkability. Is that still the core direction, or has the thinking evolved?"),
            ("Likely answer:", "Yes, that is still the vision.", None),
            ("Follow-up:", "Within that vision, what is the hotel's main job: to be the primary profit center, the lifestyle anchor for the residences, the credibility layer for the whole project, or some combination?", None),
            ("Possible answer A:", "The hotel is the anchor. It gives the whole project identity.", "Then the operator and brand decision need to protect the identity of the project, not just improve distribution."),
            ("Possible answer B:", "The residences are really the bigger economic driver.", "Then the brand/operator question becomes less about hotel P&L alone and more about buyer confidence, sales velocity, rental program credibility, and long-term asset management."),
            ("Possible answer C:", "We need both to work.", "That is where the decision becomes more complex. The best structure may not be the best hotel brand in isolation. It has to work across hotel, residences, F&B, and owner control."),
        ]),
        ("25–35 minutes — Residential strategy", "Understand how much the brand/operator affects condo value.", [
            ("Ask:", "For the residences, what do you think the brand or operator needs to prove: higher sales price, faster absorption, buyer confidence, rental income, lifestyle value, or long-term asset management?"),
            ("Possible answer A:", "Buyer confidence and sales velocity matter most.", "Then we should test whether the project needs a recognized hospitality brand, or whether a strong operator plus independent affiliation can create enough confidence."),
            ("Possible answer B:", "Rental income is important.", "Then the operator's rental program capability becomes central. We would need to understand owner-use rules, optional versus mandatory rental participation, revenue management, reporting, and how the hotel and residences interact commercially."),
            ("Possible answer C:", "Lifestyle is the key. We want buyers to feel part of something.", "Then the brand or operator has to reinforce the story. A heavy brand might help credibility but hurt authenticity. A lighter collection or strong boutique operator may be more consistent with the vision."),
            ("Possible answer D:", "We are not sure yet.", "That is exactly the kind of issue worth organizing before outreach. Different brand/operator paths solve different problems, so the first step is clarifying what the residences actually need from the hospitality layer."),
        ]),
        ("35–45 minutes — Brand need", "Understand why they want a brand.", [
            ("Ask:", "Are you looking for a brand because the hotel needs distribution, because the residences need credibility, because investors or lenders expect it, or because the project needs a stronger lifestyle identity?"),
            ("Possible answer A:", "Distribution.", "Then we should compare soft brands, collections, and independent affiliations, not just hard brands. The question is how much distribution is needed relative to fees, standards, and control."),
            ("Possible answer B:", "Residential credibility.", "Then we need to look at branded-residential capability, not just hotel brand awareness. Some brands may help the hotel but not necessarily support residential value."),
            ("Possible answer C:", "Lender or investor comfort.", "Then the framework should include capital-market credibility and operating track record, not just consumer-facing brand fit."),
            ("Possible answer D:", "Lifestyle identity.", "Then the key is whether the brand enhances the local San José story or overwhelms it. That points toward a more curated set of lifestyle or collection options."),
        ]),
        ("45–55 minutes — Operator need", "Operator-first, brand-first, or simultaneous?", [
            ("Ask:", "The deck refers to operator-led refinement around programming, service, brand alignment, and the participation model. Do you already have a view on whether the operator should come before the brand, after the brand, or together with the brand?"),
            ("Possible answer A:", "We thought brand first.", "That can work if the brand brings the operating model or helps define the residential strategy. But for a boutique mixed-use project, sometimes operator-first gives more clarity on service, F&B, rental program, staffing, and commercial execution."),
            ("Possible answer B:", "We need the operator first.", "That may be the right path if the biggest questions are operational: rental program, F&B coordination, residential services, owner reporting, and pre-opening support."),
            ("Possible answer C:", "We need both.", "Then the process should compare combinations, not isolated names. Some operators fit certain brands better; some brands require or prefer certain operator types."),
            ("Ask:", "Is the operator mainly needed for hotel operations, residential rental management, F&B coordination, owner services, commercial strategy, pre-opening support, or all of the above?", None),
            (None, None, "Listen carefully. This will define the target list."),
        ]),
        ("55–65 minutes — F&B strategy", "Brand story, economic driver, or third-party placemaking?", [
            ("Ask:", "The restaurant is positioned as a major placemaking anchor, but potentially independently operated. How important is F&B to the brand story versus the actual operating economics?"),
            ("Possible answer A:", "It is central to the identity.", "Then the hotel operator and restaurant operator need to be compatible. Even if F&B is separately operated, it cannot feel disconnected from the hotel and residences."),
            ("Possible answer B:", "We want a third party to handle it.", "That may reduce operational complexity, but the interface matters: breakfast, room charges, service standards, guest experience, revenue sharing, and brand alignment."),
            ("Possible answer C:", "We are still deciding.", "Then F&B should be a separate criterion in the brand/operator framework. Some hotel operators will want control; others may be comfortable coordinating with a third-party restaurateur."),
        ]),
        ("65–75 minutes — Control and deal terms", "Identify likely friction with brands/operators.", [
            ("Ask:", "Where does ownership want to retain the most control: design, F&B, residential program, rental rules, commercial strategy, operating decisions, or brand standards?"),
            ("Possible answer A:", "Design and concept are very important to us.", "Then we need to be careful with hard-brand standards or operators that may dilute the concept. Fit is not only about who likes the project, but who can work within the owner's vision."),
            ("Possible answer B:", "We want control over residential rules.", "Then the residential overlay is critical. Some brands/operators may require more standardization around rental participation, owner usage, services, and association rules."),
            ("Possible answer C:", "We are flexible if the economics work.", "Then the process should compare tradeoffs: what control is worth giving up if it improves distribution, buyer confidence, or operating performance."),
            ("Ask:", "Are there any deal terms or operating constraints that would be non-negotiable?", None),
        ]),
        ("75–85 minutes — Timeline and decision pressure", "Understand urgency and sequencing.", [
            ("Ask:", "The deck shows brand/operator decisions early in the process. What is the real decision deadline now, and what milestone is driving it — design, permits, residential sales, debt, equity, or marketing?"),
            ("Possible answer A:", "We need clarity before sales.", "Then the brand/operator story needs to support buyer messaging, not just hotel operations."),
            ("Possible answer B:", "We need it for debt or equity.", "Then the package needs to show credibility, operating logic, and a clear market approach. Investors will want to understand why the structure fits the project."),
            ("Possible answer C:", "We need it for design.", "Then early brand/operator input matters because standards, back-of-house, F&B, room mix, residence services, and amenity programming may affect design."),
            ("Possible answer D:", "No immediate pressure.", "Then there is an opportunity to be thoughtful and not let the first interested party define the project."),
        ]),
        ("85–95 minutes — Information gaps and readiness", "Identify the need for a structured workflow.", [
            ("Ask:", "Before approaching or narrowing brands/operators, what do you feel is still missing from the project package?"),
            ("Possible answer A:", "We need a clearer story.", "Then the first deliverable should be an outreach-ready project brief that clearly explains the opportunity, owner objectives, decision criteria, and what you are asking from each party."),
            ("Possible answer B:", "We need to know who to approach.", "I would not start with a huge list. I would build a curated target list based on the structure: what the project needs from the brand, what it needs from the operator, and what the residences require."),
            ("Possible answer C:", "We need to understand what brands/operators will ask.", "That is exactly where a readiness snapshot is useful. It identifies what is known, what is missing, and where external parties may push back."),
            ("Possible answer D:", "We need help managing the conversations.", "Then the engagement should include outreach support, response tracking, Q&A coordination, and comparison of the paths that come back."),
        ]),
        ("95–105 minutes — Introduce the AO + Dealality approach", "Only after confirming a real open decision", [
            ("Say:", "This is actually close to the type of situation Dealality is being built to support. Not because the platform says \"pick this brand,\" but because it helps organize the decision.", None),
            (None, "For this project, the core workflow is very relevant: what is known, what is missing, which structures are worth comparing, what questions each brand/operator needs to answer, and how responses should be tracked.", None),
            (None, "AO would lead the advisory work. I would use Dealality as the structured workflow behind the process. And because this is mixed-use, I would add a residential and F&B overlay, because those are not side issues here — they directly affect the brand/operator decision.", None),
        ]),
        ("105–115 minutes — Proposed next step", "Close with structured approach", [
            ("Say:", "Based on what we discussed, I think the right next step would be a focused brand/operator market approach.", None),
            (None, "I would not start by randomly calling brands or operators. I would first clarify the structure: what the hotel needs, what the residences need, how much control ownership wants, what role F&B should play, and what milestones are driving timing.", None),
            (None, "Then we would build a curated target list, prepare the outreach package, approach selected brands and operators, track responses, and compare which paths are actually worth advancing.", None),
            (None, "AO would lead the work, and I would use Dealality as the structured workflow to organize the opportunity, information gaps, comparison paths, outreach, and next steps.", None),
            ("Ask:", "Would that type of structured approach be useful for where you are right now?", None),
        ]),
    ]

    for title, objective, items in run_of_show_blocks:
        accent = "EE0000" if "0–5" in title or "95–105" in title or "105–115" in title else "C00000"
        blocks: list = [body_para(f"Objective: {objective}")]
        for raw_item in items:
            item = tuple(raw_item) + (None,) * (3 - len(raw_item))
            label, text, response = item[:3]
            blocks.append("blank")
            if label and text:
                blocks.append(body_para(f"{label}\n\n{text}"))
            elif label:
                blocks.append(body_para(label))
            elif text:
                blocks.append(body_para(text))
            if response:
                blocks.append("blank")
                if label and (
                    str(label).startswith("Possible answer")
                    or str(label).startswith("Likely answer")
                    or str(label).startswith("Your response")
                ):
                    blocks.append(body_para(f"Your response:\n\n{response}"))
                else:
                    blocks.append(body_para(response))
        section_title = title.upper()
        if not section_title.startswith("4."):
            section_title = f"4. RUN OF SHOW — {section_title}"
        rows.extend(section(section_title, objective, blocks, accent=accent))

    key_questions = {
        "Project status": [
            "Where is the project today in terms of planning, design, approvals, capital, and timing?",
            "What decisions are already fixed?",
            "What decisions are still open?",
            "Who is involved in the decision-making process?",
            "What would make this process successful from your perspective?",
        ],
        "Vision": [
            "The deck frames the project as a boutique, hospitality-led urban project rooted in downtown San José. Is that still the core direction?",
            "What do you want buyers, investors, lenders, and brand partners to understand first?",
            "Is the project more hotel-led, residential-led, or evenly balanced?",
        ],
        "Hotel role": [
            "Is the hotel the main profit center, lifestyle anchor, credibility layer, or operating platform?",
            "If the hotel performs well but does not materially improve residential value, is that still a good outcome?",
            "What kind of guest is most valuable to the project economically?",
        ],
        "Residential strategy": [
            "What does the residential component need from the hotel?",
            "What does it need from the brand?",
            "What does it need from the operator?",
            "Is the priority sales price, absorption, rental income, buyer confidence, lifestyle value, or long-term asset management?",
            "How flexible is the rental participation model?",
            "Are owner-use rules already defined?",
            "What services do you expect residence owners to receive?",
        ],
        "Brand strategy": [
            "Why do you want a brand?",
            "Is the brand mainly for hotel distribution, residential credibility, lender comfort, lifestyle identity, or exit value?",
            "Are you open to soft brand, hard brand, collection, independent affiliation, or brand-managed?",
            "Are there brands already in conversation?",
            "Are there brands you would not want?",
        ],
        "Operator strategy": [
            "Do you see this as brand-first, operator-first, or simultaneous?",
            "What must the operator be excellent at?",
            "How important is Baja/Mexico experience?",
            "How important is boutique/lifestyle experience?",
            "How important is branded residence or rental program experience?",
            "How important is pre-opening support?",
            "Would you prefer a large institutional operator or a smaller lifestyle operator?",
        ],
        "F&B": [
            "Is F&B primarily an economic driver, placemaking anchor, or brand story?",
            "Should the hotel operator control F&B?",
            "Would you prefer a separate restaurant operator?",
            "How important is the restaurant to locals, not just guests?",
            "What must not happen with F&B?",
        ],
        "Control": [
            "Where does ownership need to retain control?",
            "What are the non-negotiables?",
            "What would you be willing to give up control over if the right brand/operator improved value?",
            "What risks worry you most with a brand?",
            "What risks worry you most with an operator?",
        ],
        "Timeline": [
            "What decision has to happen first?",
            "What milestone is driving timing?",
            "When do you need external conversations to begin?",
            "When do you need to narrow options?",
            "What happens if the decision is delayed?",
        ],
        "Outreach support": [
            "Do you need help deciding who to approach?",
            "Do you need help preparing the outreach package?",
            "Do you need help managing responses?",
            "Do you want AO to participate in calls with brands/operators?",
            "What would you want to have in hand at the end of the process?",
        ],
    }

    kq_blocks: list = [
        body_para(
            "Use these as your core list. You will not ask all of them, but these are the ones that matter."
        )
    ]
    for group, questions in key_questions.items():
        kq_blocks.extend(["blank", body_para(group)])
        for q in questions:
            kq_blocks.append(body_para(q))

    rows.extend(section(
        "5. KEY QUESTIONS TO ASK",
        "Core list — use conversationally, not as a rigid checklist",
        kq_blocks,
        accent="EE0000",
    ))

    faqs = [
        ("Q1. \"So what exactly would AO do?\"", "AO would help organize the brand/operator market approach. That means clarifying the project's decision criteria, comparing the viable structures, preparing the outreach package, building a curated target list, supporting selected outreach, tracking responses, and helping ownership compare which paths are worth advancing."),
        ("Q2. \"Is this a Dealality pilot?\"", "I would not position it that way. AO would lead the advisory work. Dealality would support the process as the structured workflow to organize the project, the criteria, the information gaps, the outreach, and the comparison.\n\nSo from your perspective, this is an AO-led market approach supported by a structured tool."),
        ("Q3. \"Can Dealality handle a mixed-use project like this?\"", "The core workflow can handle it because the main issue is organizing the hotel brand/operator decision: positioning, economics, control, operator fit, brand path, and next steps.\n\nWhat I would add for this project is a residential and F&B overlay, because the residences and restaurant are central to the decision. So I would not treat this like a standard hotel matching process."),
        ("Q4. \"Can you introduce us to brands/operators?\"", "Yes, but I would not start with introductions first. The better approach is to clarify the structure and criteria, then approach a curated set of brands/operators with a clear package. That makes the conversations more productive and avoids letting the first interested party define the project."),
        ("Q5. \"Do you already have brands/operators in mind?\"", "I have some hypotheses, but I would not want to jump to names before understanding what you need the brand and operator to solve.\n\nAt a high level, I would probably compare a few paths: independent plus lifestyle operator, soft brand or collection, operator-first, and possibly a stronger branded-residential path if condo buyer confidence is the key value driver."),
        ("Q6. \"Which path do you think is best?\"", "Based only on the deck, my instinct is that this may not be a hard-brand-first project. It feels more like a boutique independent or soft-branded hotel with a strong lifestyle operator, a clear residential services/rental framework, and carefully curated F&B.\n\nBut I would want to test that against your priorities: residential value, control, distribution, capital needs, and timeline."),
        ("Q7. \"How long would this take?\"", "For a focused market approach, I would suggest around six weeks. That gives enough time to organize the criteria, prepare the package, build a curated target list, support outreach, track responses, and compare which paths are worth advancing."),
        ("Q8. \"How much would this cost?\"", "Given what we discussed, I would not separate the framework from outreach. For this project, the value is in organizing the decision and then testing the right paths with selected brands and operators.\n\nI would structure it as a six-week engagement: decision framework, curated target list, outreach package, outreach support, response comparison, and recommended next steps.\n\nI would normally price that around $55,000. Given the relationship through German and the fact that this could be a strong structured use case through Dealality, I would be comfortable proposing a founder/platform partner rate of $45,000.\n\nSofter version:\n\nDepending on how much direct outreach support you want from AO, I would expect the engagement to be in the $40,000 to $55,000 range. I can shape the scope so it is focused and practical."),
        ("Q9. \"What do we get at the end?\"", "You would have a clear decision framework, a curated target list, an outreach-ready project brief, a question set for brands/operators, an outreach tracker, a response comparison matrix, and a recommended path for which conversations to advance."),
        ("Q10. \"Can you guarantee interest?\"", "No, and I would not want to promise that. What I can do is make sure the project is presented clearly, the right parties are approached, the right questions are asked, and ownership has a structured way to compare responses. That improves the quality of the process and the likelihood of productive conversations."),
        ("Q11. \"Would you negotiate the agreements?\"", "Not in this initial engagement. I can help identify issues, compare structures, and prepare the owner for next steps, but legal review and final agreement negotiation would be separate. If the process advances, we can discuss what support is needed then."),
        ("Q12. \"Would you be acting as a broker?\"", "No. This would be an advisory and process-support engagement. The goal is to help ownership organize the decision, prepare the opportunity, approach selected parties, and compare responses. It is not a brokerage mandate or a guarantee of placement."),
    ]

    faq_blocks: list = []
    for q, a in faqs:
        faq_blocks.extend(["blank", body_para(q), "blank", body_para("Answer:"), body_para(a)])

    rows.extend(section(
        "6. LIKELY QUESTIONS DIEGO MAY ASK — AND SUGGESTED ANSWERS",
        "Prepare responses — do not over-script",
        faq_blocks[1:],
    ))

    rows.extend(section(
        "7. ENGAGEMENT TO PROPOSE",
        "San José del Cabo Brand & Operator Market Approach",
        [
            body_para("San José del Cabo Brand & Operator Market Approach"),
            body_para("AO Hospitality Advisors, powered by Dealality"),
            "blank",
            body_para("Recommended structure:"),
            body_para("$45,000 fixed fee"),
            body_para("6 weeks"),
            body_para("AO-led, Dealality-powered"),
            "blank",
            body_para("You can frame it as:"),
            body_para("Standard fee: $55,000"),
            body_para("Founder / platform partner rate: $45,000"),
            "blank",
            body_para("Do not go below $35,000 if outreach is included."),
        ],
        accent="EE0000",
    ))

    services = [
        ("1. Project intake and decision criteria", "Clarify owner priorities across: hotel profitability; residential value; buyer confidence; brand credibility; distribution; lifestyle positioning; F&B role; owner control; operating model; timeline; capital / lender / investor relevance; long-term hold or exit value."),
        ("2. Dealality readiness snapshot", "Use Dealality to organize: what is known; what is missing; what is ready for external conversations; where the project is strong; where brands/operators may push back; what must be clarified before outreach."),
        ("3. Brand/operator structure framework", "Compare: hard brand; soft brand / collection; independent hotel + affiliation; brand-managed; third-party managed; operator-first; residential-brand-first; independent + distribution affiliation."),
        ("4. Residential + F&B overlay", "Add criteria for: branded residence value proposition; buyer confidence; rental participation model; owner-use rules; service model; HOA / asset management implications; destination F&B versus hotel amenity; restaurant lease versus management agreement; hotel operator and restaurant operator interface."),
        ("5. Target list development", "Build a curated list: 6–8 brand / affiliation targets; 6–8 operator targets; optional F&B / lifestyle partner archetypes; prioritized by fit, not name recognition alone."),
        ("6. Outreach package", "Prepare: concise project brief; owner objectives; brand/operator decision criteria; key project facts; information gaps; questions for brands/operators; required response items; comparison scorecard."),
        ("7. Outreach support", "Support: outreach strategy; warm-intro sequencing; email language; direct outreach support where appropriate; tracking responses; coordinating follow-up questions; preparing Diego for calls; helping interpret brand/operator reactions."),
        ("8. Response comparison and owner decision support", "Organize: who is interested; who needs more information; who is not a fit; what concerns emerged; what terms or structure each party may require; which paths remain viable; which conversations should advance."),
        ("9. Final recommendation session", "Summarize: best-fit structures; strongest candidates to continue with; key watch-outs; unresolved information gaps; recommended next steps; negotiation priorities."),
    ]

    svc_blocks: list = []
    for title, desc in services:
        svc_blocks.extend(["blank", body_para(title), body_para(desc)])

    rows.extend(section(
        "8. SERVICES INCLUDED",
        "Scope of the six-week engagement",
        svc_blocks[1:],
    ))

    deliverables = [
        "Project Decision Criteria Framework",
        "Dealality Readiness Snapshot",
        "Brand / Operator Structure Comparison",
        "Residential + F&B Overlay",
        "Curated Brand / Operator Target List",
        "Outreach-Ready Project Brief",
        "Brand / Operator Question Set",
        "Outreach Tracker",
        "Response Comparison Matrix",
        "Recommended Path & Next-Step Memo",
    ]

    rows.extend(section(
        "9. DELIVERABLES",
        "What ownership receives at the end",
        [body_para(d) for d in deliverables],
    ))

    out_of_scope = [
        "guaranteed brand/operator interest;",
        "legal review;",
        "contract negotiation;",
        "final franchise or management agreement negotiation;",
        "residential sales brokerage;",
        "formal valuation;",
        "full financial underwriting;",
        "lender solicitation;",
        "architectural/design advisory;",
        "acting as exclusive broker unless separately agreed.",
    ]

    rows.extend(section(
        "10. OUT OF SCOPE",
        "Set expectations clearly",
        [body_para("This engagement does not include:")]
        + [body_para(item) for item in out_of_scope],
    ))

    rows.extend(section(
        "11. FOUNDER / PLATFORM PARTNER TRADE",
        "If offering the founder/platform rate",
        [
            body_para(
                "Given that I would be structuring part of the work through Dealality, I would ask that we be able to use the process as an anonymized workflow case study and that you provide candid feedback on what was useful, what was missing, and what would make the workflow more valuable for owners."
            ),
            "blank",
            body_para("Ask for:"),
            body_para("anonymized workflow case study rights;"),
            body_para("product feedback;"),
            body_para("permission to structure the work through Dealality;"),
            body_para("opportunity to discuss additional support if the process advances."),
            "blank",
            body_para("Do not ask for a public testimonial yet."),
        ],
    ))

    brand_paths = [
        ("Design-led independent / collection affiliation", "Examples: Design Hotels; Preferred Hotels & Resorts; Small Luxury Hotels of the World.", "Best if: Ownership values authenticity and control more than heavy brand standards."),
        ("Soft brand / collection", "Examples: Autograph Collection; Tapestry Collection; Vignette Collection; MGallery; Handwritten Collection.", "Best if: The project needs distribution and credibility but wants to preserve local identity and design flexibility."),
        ("Stronger branded-residential platform", "Examples: Marriott Residences-related path; Autograph Collection Residences-style path; other residential-capable hospitality brands.", "Best if: Residential sales value is the primary reason to pursue a brand.\n\nWatch-out: This may be too heavy for a small boutique hotel unless the residential economics justify it."),
        ("Independent hotel + strong operator", "Best if: Ownership wants maximum control, authentic positioning, and an operator that can carry the experience.", None),
    ]

    operator_paths = [
        ("Lifestyle / boutique operator", "Examples: Bunkhouse; Grupo Habita; Casetta-type operators; Lark / Life House-type operators.", "Best if: Atmosphere, F&B, programming, and boutique sensibility matter most."),
        ("Institutional third-party operator", "Examples: Aimbridge LATAM; Highgate; Davidson / Pivot-type operators.", "Best if: Reporting, commercial systems, lender comfort, pre-opening process, and brand relationships matter most."),
        ("Operator-first structure", "Best if: The biggest open questions are operational: rental program, F&B coordination, staffing, commercial engine, residential services, owner reporting, and pre-opening support.", None),
    ]

    hyp_blocks = [
        body_para("Do not present these as recommendations unless asked."),
        "blank",
        body_para("Brand paths to evaluate"),
    ]
    for title, examples, best_if in brand_paths:
        hyp_blocks.extend(["blank", body_para(title), body_para(examples)])
        if best_if:
            hyp_blocks.append(body_para(best_if))

    hyp_blocks.extend(["blank", body_para("Operator paths to evaluate")])
    for title, examples, best_if in operator_paths:
        hyp_blocks.extend(["blank", body_para(title), body_para(examples)])
        if best_if:
            hyp_blocks.append(body_para(best_if))

    rows.extend(section(
        "12. INTERNAL BRAND/OPERATOR HYPOTHESES",
        "Internal only — do not lead with names",
        hyp_blocks,
    ))

    email_body = """Hi Diego,

Thank you again for the conversation today. I enjoyed learning more about the San José del Cabo project and the thinking behind the hotel, residences, restaurant, and broader mixed-use vision.

What stood out to me is that the brand/operator decision should not be evaluated in isolation. It connects directly to the residential strategy, buyer perception, operating model, owner control, commercial engine, F&B positioning, economics, and development timeline.

As a next step, I think it would be useful to organize a focused brand/operator market approach.

I would not start with a list of names or broad outreach. I would first clarify the structure:

what the hotel needs from a brand or operator;
what the residences need to support buyer confidence and value;
how much control ownership wants to retain;
how F&B should be handled;
which decision milestones are driving timing;
and which brand/operator paths are actually worth comparing.

From there, AO Hospitality Advisors can help build a curated target list, prepare the outreach package, support selected brand/operator conversations, track responses, and compare which paths are worth advancing.

I would use Dealality as the structured workflow to organize the opportunity, information gaps, comparison paths, outreach, and next steps.

That should give ownership a clearer basis for deciding how to approach the market and what type of partner would truly fit the project.

Thanks again, and I look forward to continuing the conversation.

At your service,
Joan"""

    email_blocks = [body_para("Subject: San José del Cabo project — brand/operator market approach"), "blank"]
    for para in email_body.split("\n\n"):
        email_blocks.append(body_para(para))

    rows.extend(section(
        "13. UPDATED POST-CALL FOLLOW-UP EMAIL",
        "Send within 24 hours of the call",
        email_blocks,
        accent="EE0000",
    ))

    return rows


def build_document_xml(rows: list) -> bytes:
    tbl = ET.Element(w("tbl"))
    tbl_pr = ET.SubElement(tbl, w("tblPr"))
    tbl_w = ET.SubElement(tbl_pr, w("tblW"))
    tbl_w.set(w("w"), "5385")
    tbl_w.set(w("type"), "pct")
    tbl_look = ET.SubElement(tbl_pr, w("tblLook"))
    tbl_look.set(w("val"), "0600")
    tbl_look.set(w("firstRow"), "0")
    tbl_look.set(w("lastRow"), "0")
    tbl_look.set(w("firstColumn"), "0")
    tbl_look.set(w("lastColumn"), "0")
    tbl_look.set(w("noHBand"), "1")
    tbl_look.set(w("noVBand"), "1")

    grid = ET.SubElement(tbl, w("tblGrid"))
    col = ET.SubElement(grid, w("gridCol"))
    col.set(w("w"), "10081")

    for row in rows:
        tbl.append(row)

    body = ET.Element(w("body"))
    body.append(tbl)

    sect = ET.SubElement(body, w("sectPr"))
    sect.set(w("rsidR"), "00125FF5")
    sect.set(w("rsidSect"), "007C070D")

    for ref, rid in [
        ("headerReference", "rId11"),
        ("footerReference", "rId12"),
        ("headerReference", "rId13"),
        ("footerReference", "rId14"),
    ]:
        el = ET.SubElement(sect, w(ref))
        el.set(w("type"), "default" if rid in ("rId11", "rId12") else "first")
        el.set(f"{{{R[1:-1]}}}id", rid)

    pg_sz = ET.SubElement(sect, w("pgSz"))
    pg_sz.set(w("w"), "12240")
    pg_sz.set(w("h"), "15840")
    pg_mar = ET.SubElement(sect, w("pgMar"))
    for key, val in [
        ("top", "1296"), ("right", "1440"), ("bottom", "0"), ("left", "1440"),
        ("header", "0"), ("footer", "0"), ("gutter", "0"),
    ]:
        pg_mar.set(w(key), val)
    cols = ET.SubElement(sect, w("cols"))
    cols.set(w("space"), "720")
    ET.SubElement(sect, w("titlePg"))
    doc_grid = ET.SubElement(sect, w("docGrid"))
    doc_grid.set(w("linePitch"), "360")

    ns = {
        "wpc": "http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas",
        "mc": "http://schemas.openxmlformats.org/markup-compatibility/2006",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
        "w14": "http://schemas.microsoft.com/office/word/2010/wordml",
        "w15": "http://schemas.microsoft.com/office/word/2012/wordml",
        "w16cex": "http://schemas.microsoft.com/office/word/2018/wordml/cex",
        "w16cid": "http://schemas.microsoft.com/office/word/2016/wordml/cid",
        "w16": "http://schemas.microsoft.com/office/word/2018/wordml",
        "w16du": "http://schemas.microsoft.com/office/word/2023/wordml/word16du",
        "w16sdtdh": "http://schemas.microsoft.com/office/word/2020/wordml/sdtdatahash",
        "w16sdtfl": "http://schemas.microsoft.com/office/word/2024/wordml/sdtformatlock",
        "w16se": "http://schemas.microsoft.com/office/word/2015/wordml/symex",
    }

    root = ET.Element(
        f"{W}document",
        {
            "xmlns:wpc": ns["wpc"],
            "xmlns:mc": ns["mc"],
            "xmlns:r": ns["r"],
            "xmlns:w": ns["w"],
            "xmlns:w14": ns["w14"],
            "xmlns:w15": ns["w15"],
            "xmlns:w16cex": ns["w16cex"],
            "xmlns:w16cid": ns["w16cid"],
            "xmlns:w16": ns["w16"],
            "xmlns:w16du": ns["w16du"],
            "xmlns:w16sdtdh": ns["w16sdtdh"],
            "xmlns:w16sdtfl": ns["w16sdtfl"],
            "xmlns:w16se": ns["w16se"],
            "{http://schemas.openxmlformats.org/markup-compatibility/2006}Ignorable": "w14 w15 w16se w16cid w16 w16cex w16sdtdh w16sdtfl w16du",
        },
    )
    root.append(body)

    xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    return xml_bytes


def update_core_metadata(work_dir: Path) -> None:
    core_path = work_dir / "docProps" / "core.xml"
    if not core_path.exists():
        return
    text = core_path.read_text(encoding="utf-8")
    text = re.sub(r"<dc:title>.*?</dc:title>", "<dc:title>Diego Call Prep Package — San José del Cabo (Mijares 32)</dc:title>", text)
    text = re.sub(r"<dc:subject>.*?</dc:subject>", "<dc:subject>AO-led brand/operator market approach call prep</dc:subject>", text)
    core_path.write_text(text, encoding="utf-8")


def main() -> None:
    if not TEMPLATE.exists():
        raise SystemExit(f"Template not found: {TEMPLATE}")

    work_dir = Path(tempfile.mkdtemp(prefix="diego_call_prep_"))

    try:
        with zipfile.ZipFile(TEMPLATE, "r") as zin:
            zin.extractall(work_dir)

        rows = build_sections()
        doc_xml = build_document_xml(rows)
        (work_dir / "word" / "document.xml").write_bytes(doc_xml)
        update_core_metadata(work_dir)

        if OUTPUT.exists():
            OUTPUT.unlink()

        with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as zout:
            for path in sorted(work_dir.rglob("*")):
                if path.is_file():
                    zout.write(path, path.relative_to(work_dir).as_posix())

        print(f"Created: {OUTPUT}")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
