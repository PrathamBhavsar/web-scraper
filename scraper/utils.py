import re, os

def sanitize_filename(s):
    s = s or ''
    s = s.strip()
    s = re.sub(r'[\\\\/*?:"<>|]', '_', s)
    return s[:240]

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
