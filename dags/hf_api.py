import re
import time

import requests

BASE_URL = "https://huggingface.co"
HEADERS = {"User-Agent": "ppad-lab3 student project (https://github.com/AndrewFalse/ppad-lab3)"}

# поля списка моделей, которые сохраняем в stg
MODEL_FIELDS = [
    "cardData", "gated", "tags", "sha", "lastModified", "createdAt",
    "pipeline_tag", "library_name", "disabled", "private", "downloads", "likes", "siblings",
]

# поля состояния модели на конкретный коммит
REVISION_FIELDS = ["cardData", "siblings", "sha", "lastModified", "tags", "pipeline_tag", "library_name"]


def parse_ratelimit(header):
    # заголовок вида "api";r=499;t=281: r сколько запросов осталось, t через сколько секунд новое окно
    match = re.search(r"r=(\d+);t=(\d+)", header or "")
    if match is None:
        return None, 60
    return int(match.group(1)), int(match.group(2))


def get(url, params=None):
    for attempt in range(1, 7):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=60)
        except requests.RequestException as error:
            print("network error", error)
            time.sleep(10 * attempt)
            continue

        remaining, reset = parse_ratelimit(response.headers.get("ratelimit"))

        if response.status_code == 429:
            print("rate limit, wait", reset + 1)
            time.sleep(reset + 1)
            continue

        if response.status_code >= 500 or response.status_code == 408:
            print("server error", response.status_code)
            time.sleep(10 * attempt)
            continue

        # запросы в окне почти кончились, ждём следующее окно
        if remaining is not None and remaining < 5:
            print("rate limit almost used, wait", reset + 1)
            time.sleep(reset + 1)

        return response

    raise Exception("too many errors for " + url)


def list_models(org):
    url = BASE_URL + "/api/models"
    # сортировка по дате создания даёт стабильный порядок страниц
    params = {"author": org, "limit": 1000, "sort": "createdAt", "direction": 1, "expand[]": MODEL_FIELDS}
    models = []
    pages = 0
    while url:
        response = get(url, params)
        if response.status_code != 200:
            raise Exception("cannot list models of " + org + ", status " + str(response.status_code))
        models.extend(response.json())
        pages += 1
        # ссылка на следующую страницу приходит в заголовке Link, параметры уже в ней
        url = response.links.get("next", {}).get("url")
        params = None
    return models, pages


def list_commits(repo_id):
    url = BASE_URL + "/api/models/" + repo_id + "/commits/main"
    params = {"limit": 1000}
    commits = []
    while url:
        response = get(url, params)
        if response.status_code != 200:
            return response.status_code, []
        commits.extend(response.json())
        url = response.links.get("next", {}).get("url")
        params = None
    return 200, commits


def get_revision(repo_id, sha):
    url = BASE_URL + "/api/models/" + repo_id + "/revision/" + sha
    response = get(url, {"expand[]": REVISION_FIELDS})
    if response.status_code != 200:
        return response.status_code, None
    return 200, response.json()


def get_model(repo_id):
    url = BASE_URL + "/api/models/" + repo_id
    response = get(url, {"expand[]": MODEL_FIELDS})
    if response.status_code != 200:
        return response.status_code, None
    return 200, response.json()


def get_license_tags():
    response = get(BASE_URL + "/api/models-tags-by-type", {"type": "license"})
    if response.status_code != 200:
        raise Exception("cannot load license tags, status " + str(response.status_code))
    return response.json()
