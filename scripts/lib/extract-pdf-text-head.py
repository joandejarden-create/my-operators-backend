"""First N pages of PDF to stdout. Usage: extract-pdf-text-head.py <path> [max_pages=3]"""
import sys
from pypdf import PdfReader

def main():
    path = sys.argv[1]
    max_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    reader = PdfReader(path)
    parts = []
    for i, page in enumerate(reader.pages):
        if i >= max_pages:
            break
        t = page.extract_text() or ""
        if t.strip():
            parts.append(t)
    print("\n\n".join(parts))

if __name__ == "__main__":
    main()
