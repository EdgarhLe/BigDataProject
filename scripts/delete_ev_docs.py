"""Delete or preview documents related to 'xe điện' / EV across raw collections.

Usage:
  py -3 scripts/delete_ev_docs.py         # preview counts
  py -3 scripts/delete_ev_docs.py --delete  # perform deletion

The script reads Mongo connection from src.config (MONGO_URI, MONGO_DB).
"""
import re
import sys
from pathlib import Path

# Ensure project root is on sys.path so `import src` works when script run directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import MONGO_URI, MONGO_DB
from pymongo import MongoClient

COLLECTIONS = ["youtube_raw","google_news_raw","vnexpress_raw","tuoitre_raw","reddit_raw","social_raw_posts"]
EV_KEYWORDS = [
    "xe điện", "xe máy điện", "electric", "ev", "vinfast", "byd", "xiaomi", "xiao", "atto", "su7", "dat bike", "selex", "yadea", "dibao"
]

pattern = "|".join(re.escape(k) for k in EV_KEYWORDS)
regex_opts = {"$regex": pattern, "$options": "i"}

query = {"$or": [
    {"brand": regex_opts},
    {"title": regex_opts},
    {"content": regex_opts},
    {"description": regex_opts},
    {"url": regex_opts}
]}


def main(do_delete=False):
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command('ping')
    except Exception as e:
        print(f"ERROR: cannot reach MongoDB ({e})")
        sys.exit(2)

    db = client[MONGO_DB]
    total_matches = 0
    per_collection = []
    for col_name in COLLECTIONS:
        col = db[col_name]
        try:
            cnt = col.count_documents(query)
        except Exception:
            cnt = 0
        per_collection.append((col_name, cnt))
        total_matches += cnt

    print("Preview of EV-related documents:")
    for name, cnt in per_collection:
        print(f"  {name}: {cnt}")
    print(f"TOTAL matches: {total_matches}\n")

    if do_delete:
        if total_matches == 0:
            print("Nothing to delete.")
            return
        confirm = input("Type DELETE to confirm permanent deletion: ")
        if confirm != "DELETE":
            print("Aborted by user.")
            return
        deleted = 0
        for name, cnt in per_collection:
            if cnt == 0:
                continue
            res = db[name].delete_many(query)
            deleted += res.deleted_count
            print(f"  Deleted {res.deleted_count} documents from {name}")
        print(f"Total deleted: {deleted}")


if __name__ == '__main__':
    do_delete = '--delete' in sys.argv
    main(do_delete)
