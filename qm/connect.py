import json
from pathlib import Path
from qm.db.DB import POSTGRESCRUD

headers = {"Content-type": "application/json"}

SECRET_PATH = Path(__file__).resolve().parent.parent
SECRET_FILE = SECRET_PATH / "secrets.json"
secrets = json.loads(open(SECRET_FILE).read())

for key, value in secrets.items():
    # postgresql connect
    if key == "lightsail_db":
        pgdb_properties = value
    
    if key == "kiwoom":
        kiwoom_info = value


def postgres_connect():
    return POSTGRESCRUD(pgdb_properties)

