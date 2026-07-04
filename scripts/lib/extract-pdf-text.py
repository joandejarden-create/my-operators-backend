"""Extract plain text from a PDF path (stdout). Usage: python extract-pdf-text.py <path>"""
import sys
from pypdf import PdfReader

def main():
    if len(sys.argv) < 2:
        print("usage: extract-pdf-text.py <pdf>", file=sys.stderr)
        sys.exit(1)
    path = sys.argv[1]
    reader = PdfReader(path)
    parts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        if t.strip():
            parts.append(t)
    print("\n\n".join(parts))

if __name__ == "__main__":
    main()
