import re

def normalize_company_name(raw_name):
    if not raw_name: return ""
    name = " ".join(raw_name.strip().upper().split())
    name = re.sub(r'\([^)]+\)', '', name)
    name = re.sub(r'\s+\b(LIMITED|LTD|PVT|PRIVATE)\b\.?$', '', name)
    return name.strip()