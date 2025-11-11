# Script to convert a large CSV file into multiple JSONL shard files.

import ujson
import pandas as pd
from pathlib import Path

# Path to original forum CSV
SRC = Path(
    "data/raw/b0cd514b-b9cc-4972-a0c2-c91726e6d825.csv"
)

# Directory where shard files will be saved
OUT = Path("data/shards")
OUT.mkdir(parents=True, exist_ok=True)   # create folders if they don't exist

# Number of rows to read per chunk — adjust to your memory limits
CHUNK = 100_000

# Pandas will read the file in streaming mode, returning
# one DataFrame of CHUNK rows at a time instead of the whole file.
reader = pd.read_csv(SRC, chunksize=CHUNK, low_memory=False)

# Counter for shard filenames
shard_idx = 0

# Iterate through each chunk of the CSV
for df in reader:

    # Convert the DataFrame into a list of dictionaries,one dictionary per row
    recs = df.to_dict(orient="records")

    # Create a file name like: shard_00000.jsonl, shard_00001.jsonl, etc.
    out = OUT / f"shard_{shard_idx:05d}.jsonl"

    # Open a new shard file for writing (UTF-8 so we keep special characters)
    with out.open("w", encoding="utf-8") as f:

        # Loop through each row dictionary in this chunk
        for r in recs:
            # Convert the Python dict → JSON string using ujson (faster than json.dumps)
            # ensure_ascii=False keeps non-ASCII text (e.g. Swahili, Luganda) readable
            line = ujson.dumps(r, ensure_ascii=False)
            # Write the JSON string as a line in the file
            f.write(line + "\n")

    shard_idx += 1

    print(f"Wrote {out.name} with {len(recs):,} rows")

print(f"✅ Done. {shard_idx} shard files created in {OUT.resolve()}")
