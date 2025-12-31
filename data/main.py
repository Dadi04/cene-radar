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

URLS = [
    "https://data.gov.rs/api/1/datasets/?q=cenovnici",
    "https://data.gov.rs/api/2/datasets/?q=cenovnici"
]

TOTAL_ORG_NUMBER = 27

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

def get_json(url: str, timeout: int = 30) -> dict:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()

def download_file(url: str, target: Path, timeout: int = 60) -> None:
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    with target.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

def fetch_unique_datasets() -> list[dict]:
    unique = {}

    while len(unique) < TOTAL_ORG_NUMBER:
        for url in URLS:
            data = get_json(url).get("data", [])
            for item in data:
                unique[item.get("id")] = item

    return list(unique.values())

def ensure_resources(dataset: dict) -> list[dict] | None:
    resources = dataset.get("resources")

    if isinstance(resources, list):
        return resources
    
    # weird data.gov.rs API behaviour
    # sometimes the resources variable has one resource (its not a list of dicts, but a dict instead)
    # so you have to get URI of the in that dict first 
    # and then get the resources from that URI
    uri = dataset.get("uri")
    if not uri:
        print(f"Missing URI for dataset id: {dataset.get("id")}")
        return None
    
    full_dataset = get_json(uri)
    resources = full_dataset.get("resources")
    if not isinstance(resources, list):
        print("Resources still invalid after URI fetch")
        return None

    return resources

def get_week_folder(last_modified: str) -> str:
    dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"

def prepare_folders(org_name: str, week_folder: str) -> Path:
    org_path = os.path.join(BASE_PATH, org_name)
    week_path = Path(os.path.join(org_path, week_folder))

    week_path.mkdir(parents=True, exist_ok=True)
    return week_path

def should_skip(resource: dict, state: dict, week_folder: str) -> bool:
    resource_id = resource.get("id")
    last_modified = resource.get("last_modified")

    prev = state.get(resource_id)
    if not prev:
        return False
    
    if prev["iso_week"] == week_folder and prev["last_modified"] == last_modified:
        return True
    return False

def process_resource(resource: dict, target_dir: Path, state: dict, week_folder: str) -> None:
    if resource.get("filetype") != "file":
        return None
    
    filename = resource.get("title")
    file_path = Path(os.path.join(target_dir, filename))

    if file_path.exists():
        print(f"up to date: {filename}")
        return

    if should_skip(resource, state, week_folder):
        print(f"skip: {filename}")
        return
    
    print(f"downloading: {filename}")
    download_file(resource.get("url"), file_path)

    state[resource.get("id")] = {
        "last_modified": resource.get("last_modified"),
        "iso_week": week_folder
    }

    save_state(state)
    print(f"saved to {file_path}")

def main():
    state = load_state()
    datasets = fetch_unique_datasets()

    for dataset in datasets:
        org = dict(dataset.get("organization"))
        org_name = org.get("name").replace('"', "").strip()

        week_folder = get_week_folder(dataset.get("last_modified"))
        target_dir = prepare_folders(org_name, week_folder)

        resources = ensure_resources(dataset)
        if not resources:
            continue

        for resource in resources:
            if isinstance(resource, dict):
                process_resource(resource, target_dir, state, week_folder)

main()