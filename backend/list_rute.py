from importlib import reload
import main_fastapi as mf

reload(mf)   # ensure the module is reloaded from disk
app = mf.app

for r in app.routes:
    print(r.path, sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")]))
