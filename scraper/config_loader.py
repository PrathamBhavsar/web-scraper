import json, pathlib

def load_config(path='config.json'):
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError('config.json not found')
    return json.loads(p.read_text(encoding='utf-8'))
