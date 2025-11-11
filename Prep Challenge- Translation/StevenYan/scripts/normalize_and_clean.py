
import argparse
import json
import re
from pathlib import Path
import ujson

# ---------- Sentence splitter setup ----------
def make_splitter():
    try:
        from syntok.segmenter import analyze

        def syntok_split(t: str) -> str:
            # syntok returns tokens grouped into sentences and paragraphs
            sents = []
            for paragraph in analyze(t):
                for sentence in paragraph:
                    sents.append(
                        "".join([tok.spacing + tok.value for tok in sentence]).strip()
                    )
            return "\n".join(sents)

        return syntok_split
    except Exception:
        pass
    # Fallback: simple regex split on ., !, ? (keeps it deterministic)
    SENT_RE = re.compile(r"(?<=[.!?])\s+")
    return lambda t: "\n".join([s.strip() for s in SENT_RE.split(t) if s.strip()])

text_to_sentences = make_splitter()

# ---------- Default paths (override via CLI flags) ----------
DEF_IN  = Path("data/shards_posts")        # input shards (long format)
DEF_OUT = Path("data/shards_lang")   # output folder (normalized & cleaned)
DEF_LANG_MAP = Path("config/lang_map.json")

# ---------- Regex precompile for speed ----------
URL_RE = re.compile(r"https?://\S+")   # find raw URLs
WS_RE  = re.compile(r"\s+")            # collapse whitespace

# ---------- Cleaning helpers ----------
def normalize_text(txt: str) -> str:
    """
    Light, safe text normalization for forum data.
    - Remove zero-width + NBSP-like chars
    - Collapse whitespace to single spaces
    - Replace raw URLs with <URL> token to reduce hallucinations
    """
    if not txt:
        return ""
    # Remove invisible cruft
    txt = txt.replace("\u200b", " ").replace("\xa0", " ")
    txt = txt.strip()
    # Standardize whitespace
    txt = WS_RE.sub(" ", txt)
    # Replace http(s) links (we keep a token as placeholder)
    txt = URL_RE.sub("<URL>", txt)
    return txt

def load_label_map(path: Path) -> dict:
    """
    Load your label -> NLLB mapping and normalize keys:
    - lowercased
    - trimmed
    So variants like "LUGANDA " or " Kiswahili" still map cleanly.
    """
    with path.open() as f:
        raw = json.load(f)
    # Normalize keys once to avoid per-record overhead
    return {str(k).strip().lower(): v for k, v in raw.items()}

def map_label_to_nllb(label: str, mapping: dict) -> str | None:
    """
    Map your dataset's language label to NLLB code.
    Returns None if label isn't recognized.
    """
    if not label:
        return None
    return mapping.get(str(label).strip().lower())

# ---------- Per-file processing ----------
def process_shard(in_path: Path, out_path: Path, label_map: dict, keep_hint: bool=False) -> dict:
    """
    Stream a single shard:
    - Read each JSON line
    - Clean text
    - Map lang_hint -> lang (NLLB code)
    - Sentence-split (newline-delimited string)
    - Write out a normalized record
    Returns summary stats for logging.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    kept = 0                  # rows written
    skipped_empty = 0         # rows dropped for empty text after cleaning
    skipped_unknown = 0       # rows dropped for unknown label

    with in_path.open() as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            r = ujson.loads(line)

            # 1) Clean text
            txt = normalize_text(r.get("text") or "")
            if not txt:
 
                # Drop posts that went empty after cleaning
                skipped_empty += 1
                continue

            # 2) Map your label (lang_hint) to NLLB
            lang = map_label_to_nllb(r.get("lang_hint"), label_map)
            if not lang:
                # Unknown labels are skipped to keep the dataset pristine
                skipped_unknown += 1
                continue

            # 3) Sentence-split (newline-delimited string)
            #    Note: translating at post-level is usually fine for forum text,
            #    but sentence segmentation helps downstream scoring/analysis.
            sents = text_to_sentences(txt)

            # 4) Normalize output record
            r["lang"]  = lang      # new canonical code
            r["text"]  = txt       # cleaned text
            r["sents"] = sents     # newline-joined sentences

            if not keep_hint:
                # Drop original label field to avoid confusion downstream
                r.pop("lang_hint", None)

            # 5) Write one JSON line
            fout.write(ujson.dumps(r, ensure_ascii=False) + "\n")
            kept += 1

    return {
        "file": in_path.name,
        "kept": kept,
        "skipped_empty": skipped_empty,
        "skipped_unknown_label": skipped_unknown
    }

# ---------- CLI entrypoint ----------
def main():
    parser = argparse.ArgumentParser(
        description="Normalize labeled languages -> NLLB, clean text, sentence-split shards."
    )
    parser.add_argument("--in_dir", type=Path, default=DEF_IN,
                        help="Input directory with long-format JSONL shards")
    parser.add_argument("--out_dir", type=Path, default=DEF_OUT,
                        help="Output directory for normalized shards")
    parser.add_argument("--lang_map", type=Path, default=DEF_LANG_MAP,
                        help="Path to config/lang_map.json")
    parser.add_argument("--keep-hint", action="store_true",
                        help="Keep original lang_hint field (defaults to dropping it)")
    args = parser.parse_args()

    # Load mapping file once
    label_map = load_label_map(args.lang_map)

    # Enumerate input shards
    shards = sorted(args.in_dir.glob("*.jsonl"))
    if not shards:
        print(f"[warn] No .jsonl files found in {args.in_dir.resolve()}")
        return

    # Process all shards (streaming; low memory)
    totals = {"kept":0, "skipped_empty":0, "skipped_unknown_label":0}
    for i, shard in enumerate(shards, 1):
        out_path = args.out_dir / shard.name
        stats = process_shard(shard, out_path, label_map, keep_hint=args.keep_hint)

        # Per-file progress
        print(f"[{i}/{len(shards)}] {stats['file']}: "
              f"kept={stats['kept']} empty={stats['skipped_empty']} "
              f"unknown={stats['skipped_unknown_label']}")

        # Aggregate totals
        for k in totals:
            totals[k] += stats[k]

    # Final summary
    print("\nDone.",
          f"Kept={totals['kept']},",
          f"Skipped(empty)={totals['skipped_empty']},",
          f"Skipped(unknown_label)={totals['skipped_unknown_label']}")

if __name__ == "__main__":
    main()
