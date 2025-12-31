import requests
import os
import json
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

load_dotenv()

BASE_PATH = os.environ.get("DATA_FOLDERS_PATH")
if not BASE_PATH:
    raise RuntimeError("DATA_FOLDERS_PATH is not set")

STATE_FILE = Path("download_state.json")

url_1 = "https://data.gov.rs/api/1/datasets/?q=cenovnici"
url_2 = "https://data.gov.rs/api/2/datasets/?q=cenovnici"

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

unique_datasets = {}
count = 0

while (count < 27):
    response_1 = requests.get(url_1)
    response_2 = requests.get(url_2)

    response_1.raise_for_status()
    response_2.raise_for_status()

    data_1 = response_1.json()["data"]
    data_2 = response_2.json()["data"]

    for item in data_1 + data_2:
        unique_datasets[item["id"]] = item

    deduped_data = list(unique_datasets.values())
    count = len(deduped_data)

state = load_state()

for dataset in deduped_data:
    org = dataset["organization"]
    org_name = org["name"].replace('"', '').strip()

    last_modified = dataset["last_modified"]
    dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
    year, week, _ = dt.isocalendar()
    week_folder_name = f"{year}-W{week:02d}"

    shop_folder = os.path.join(BASE_PATH, org_name)
    os.makedirs(shop_folder, exist_ok=True)

    week_folder = os.path.join(shop_folder, week_folder_name)
    os.makedirs(week_folder, exist_ok=True)

    resources = dataset["resources"]
    if not isinstance(resources, list):
        uri = dataset.get("uri")
        if not uri:
            print("resources is not a list")
            continue

        response = requests.get(uri, timeout=30)
        response.raise_for_status()
        full_dataset = response.json()

        resources = full_dataset.get("resources")
        if not isinstance(resources, list):
            print("resources still not a list after uri fetch")
            continue

    for resource in resources:
        if not isinstance(resource, dict):
            print("resource is not a dict")
            continue

        if resource["filetype"] != "file":
            print("not a file resource")
            continue

        resource_id = resource["id"]
        last_modified = resource["last_modified"]
        filename = resource["title"]

        prev = state.get(resource_id)
        if prev:
            if prev["iso_week"] == week_folder_name and prev["last_modified"] == last_modified:
                print(f"skip: {filename}")
                continue

        url = resource["url"]
        ext = resource["format"]
        
        file_path = os.path.join(week_folder, filename)
        if os.path.exists(file_path):
            print(f"exists: {filename}")
            continue

        print(f"downloading: {filename}")

        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        state[resource_id] = {
            "last_modified": last_modified,
            "iso_week": week_folder_name
        }

        save_state(state)

        print(f"saved to: {file_path}")