# Convert existing wide shards -> long posts (one post per line)

import ujson, json
from pathlib import Path

# Input directory: wide-format JSONL shards
IN = Path("data/shards")

# Output directory: long-format shards (one post per line)
OUT = Path("data/shards_posts")
OUT.mkdir(parents=True, exist_ok=True)

# Load the column mapping that defines which fields correspond
# to question/response IDs, content, timestamps, etc.
with open("config/columns.json") as f:
    m = json.load(f)


# Helper function to extract one post (question or response)
# from a wide record. Returns a normalized dict containing only
# fields we need for downstream NLP.
def build_post(rec, side):
    s = m[side]  # column map for this side ("question" or "response")

    # Get the text content for this side
    txt = rec.get(s["content"])
    if not txt or str(txt).strip() == "":
        return None  # skip empty posts

    # Get unique post ID
    rid = rec.get(s["id"])
    if not rid or str(rid).strip() == "":
        return None  # skip if missing ID

    # Return a standardized post record
    return {
        # Prefix IDs to distinguish questions from responses
        "id": ("q:" if side == "question" else "r:") + str(rid),
        # Thread linkage (so we can group posts from same thread later)
        "thread_id": rec.get(m["thread_id"]),
        # Timestamps and author info for temporal/user analyses
        "created_at": rec.get(s["created_at"]),
        "author": rec.get(s["user_id"]),
        # Original language code (used before normalization)
        "lang_hint": rec.get(s["language"]),
        # The post text itself
        "text": str(txt),
        # Whether this was a question or a response
        "role": side,
    }


# Main conversion loop: Iterate over each input shard, read each 
# wide record, and write out its question and response posts as 
# separate lines.
count_in, count_out = 0, 0  # simple counters for logging

for src in sorted(IN.glob("*.jsonl")):
    dst = OUT / src.name  # output file with same name
    with src.open() as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            rec = ujson.loads(line)  # parse one wide record
            count_in += 1
            # Create both posts (question + response)
            for side in ("question", "response"):
                p = build_post(rec, side)
                if p:
                    # Write each valid post as its own JSON line
                    fout.write(ujson.dumps(p, ensure_ascii=False) + "\n")
                    count_out += 1
    print("wrote", dst.name)

# Summary statistics
print(f"rows in (wide) ~{count_in:,}, rows out (posts) ~{count_out:,}")
