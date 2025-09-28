from pathlib import Path

def is_nonzero_file(path):
    p = Path(path)
    return p.exists() and p.stat().st_size > 1024

def basic_mp4_check(path):
    try:
        with open(path,'rb') as fh:
            head = fh.read(512)
            return b'ftyp' in head or b'moov' in head
    except:
        return False
