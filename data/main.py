import requests
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

BASE_PATH = os.environ.get("DATA_FOLDERS_PATH")
TODAY = date.today().isoformat()

url_1 = "https://data.gov.rs/api/1/datasets/?q=cenovnici"
url_2 = "https://data.gov.rs/api/2/datasets/?q=cenovnici"
unique_datasets = {}

count = 0
while (count < 27):
    response_1 = requests.get(url_1)
    response_2 = requests.get(url_2)
    data_1 = response_1.json()["data"]
    data_2 = response_2.json()["data"]

    for item in data_1 + data_2:
        unique_datasets[item["id"]] = item

    deduped_data = list(unique_datasets.values())
    count = len(deduped_data)

for dataset in deduped_data:
    org = dataset["organization"]
    name = org["name"].replace('"', '').strip()

    shop_folder = os.path.join(BASE_PATH, name)
    os.makedirs(shop_folder, exist_ok=True)

    date_folder = os.path.join(shop_folder, TODAY)
    os.makedirs(date_folder, exist_ok=True)

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

        url = resource["url"]
        ext = resource["format"]
        resource_id = resource["id"]

        filename = f"{resource_id}.{ext}"
        file_path = os.path.join(date_folder, filename)
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

        print(f"saved to: {file_path}")            