import gzip
import json
import requests
import re

# ---------- CONFIG ----------
WET_INDEX_URL = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/wet.paths.gz"
MAX_COMPANIES = 215000
OUTPUT_FILE = "aus_output.jsonl"
AU_DOMAINS = [".com.au", ".org.au", ".net.au", ".edu.au"]

# ---------- FUNCTION: Get WET File Paths ----------
def get_wet_paths(index_url):
    print("Downloading WET index...")
    paths = []
    with requests.get(index_url, stream=True) as r:
        with gzip.open(r.raw, mode='rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                paths.append(line.strip())
    print(f"Found {len(paths)} WET files.")
    return paths

# ---------- FUNCTION: Parse and Write Record Immediately ----------
def parse_and_write_record(record, url, outfile, seen_urls, total_count):
    try:
        content_start = False
        content_lines = []
        for rline in record:
            if content_start:
                content_lines.append(rline)
            if rline.strip() == "":
                content_start = True
        content = "\n".join(content_lines).strip()

        # Extract title
        title_match = re.search(r"<title>(.*?)</title>", content, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).strip()
        else:
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            title = lines[0] if lines else ""

        # Clean title
        if " - " in title:
            title = title.split(" - ")[0]
        elif " | " in title:
            title = title.split(" | ")[0]
        title = title.title()

        if any(domain in url for domain in AU_DOMAINS) and url not in seen_urls:
            entry = {"url": url.strip(), "title": title[:120], "industry": ""}
            json.dump(entry, outfile)
            outfile.write("\n")
            seen_urls.add(url)
            print(f"[{total_count}] Extracted: {entry}")
            return 1  # increment count

    except Exception as e:
        print(f"Error parsing record: {e}")
    return 0

# ---------- FUNCTION: Extract and Write AU Company Records ----------
def extract_and_stream(wet_file_url, outfile, seen_urls, total_count):
    try:
        with requests.get(wet_file_url, stream=True, timeout=60) as r:
            with gzip.open(r.raw, mode='rt', encoding='utf-8', errors='ignore') as f:
                record = []
                url = ""

                for line in f:
                    if line.startswith("WARC/1.0"):
                        if record and total_count < MAX_COMPANIES:
                            total_count += parse_and_write_record(record, url, outfile, seen_urls, total_count + 1)
                        record = [line]
                        url = ""
                    elif line.startswith("WARC-Target-URI:"):
                        url = line[len("WARC-Target-URI:"):].strip()
                        record.append(line)
                    else:
                        record.append(line)

                # Final record
                if record and total_count < MAX_COMPANIES:
                    total_count += parse_and_write_record(record, url, outfile, seen_urls, total_count + 1)

    except Exception as e:
        print(f"Error reading WET file: {e}")
    return total_count

# ---------- MAIN ----------
def main():
    paths = get_wet_paths(WET_INDEX_URL)
    seen_urls = set()
    total_count = 0

    with open(OUTPUT_FILE, "a", encoding="utf-8") as outfile:
        for idx, path in enumerate(paths):
            wet_url = f"https://data.commoncrawl.org/{path}"
            print(f"[{idx + 1}/{len(paths)}] Processing: {wet_url}")
            total_count = extract_and_stream(wet_url, outfile, seen_urls, total_count)

            print(f" -> Total records written: {total_count}")
            if total_count >= MAX_COMPANIES:
                print("🎯 Reached target. Stopping.")
                break

    print(f"\n✅ Finished. {total_count} records written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
