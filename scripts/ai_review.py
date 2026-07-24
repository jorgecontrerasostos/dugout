import requests
from requests.exceptions import ConnectionError
import sys

from constants import URL, prompt
from log import log


def main() -> None:
    pr_metadata = sys.argv[1]
    pr_diff = sys.argv[2]
    try:
        with (
            open(pr_metadata, "r", encoding="utf-8") as metadata,
            open(pr_diff, "r", encoding="utf-8") as diff,
        ):
            metadata_file = metadata.read()
            diff_file = diff.read()
    except FileNotFoundError:
        log.error("File not found")
        sys.exit(1)

    try:
        response: requests.Response = requests.post(
            URL,
            json={
                "model": "qwen2.5-coder:7b",
                "prompt": prompt(metadata_file, diff_file),
                "options": {"num_ctx": 8192},
                "stream": False,
            },
        )
    except ConnectionError as e:
        log.error(f"Could not connect to Ollama's API: {e}")

    response.raise_for_status()

    response_data = response.json()["response"]

    try:
        with open("pr_comment.txt", "w", encoding="utf-8") as f:
            f.write(response_data)
    except Exception as e:
        log.error(f"Could not write file: {e}")


if __name__ == "__main__":
    main()
