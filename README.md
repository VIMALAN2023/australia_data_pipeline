 Project Title: Australian Company Data Pipeline 
## Overview 

This project builds a complete data pipeline using Common Crawl and Australian Business Register (ABR) data to generate integrated insights. The pipeline includes data extraction, cleaning, normalization, deduplication, and storage using open-source tools. 
## Database Schema (PostgreSQL DDL) 

-- Table: common_crawl_data  

CREATE TABLE common_crawl_data ( industry VARCHAR(100), title VARCHAR(255), url VARCHAR(300) PRIMARY KEY ); 

-- Table: abr_data  

CREATE TABLE abr_data ( abn CHAR(11) PRIMARY KEY, entity_name VARCHAR(255), entity_type VARCHAR(100), entity_status CHAR(3), entity_address VARCHAR(255), entity_postcode CHAR(4), entity_state CHAR(3), entity_start_date DATE ); 
## Tech Stack Justification 


Component 

Technology 

Justification 

Data Extraction 

Python 

Fast, flexible, XML + WARC parsing support 

 

Data Processing 

Apache Spark 

Scalable distributed processing 

 

Data Transformation 

dbt 

Clean, test, and document SQL-based models 


Data Storage 

PostgreSQL 

Structured, normalized entity data storage 


Intermediate Formats 

CSV, JSON 

Efficient data interchange formats
 

Database Interaction 

psycopg2 

Lightweight Python PostgreSQL adapter 


## Setup & Running Instructions

Create and Activate Python Environment 

python -m venv venv 
source venv/bin/activate 
pip install -r requirements.txt 

 

Set Up PostgreSQL Database 

 

Install pgadmin 

Create Database test_db 

 

Extract and Ingest Data 

Run the scripts in the right order: 

# Extract ABR XML → CSV/JSON 
python extract/extract_abr.py 
 
# Extract Common Crawl WAT → JSON 
python extract/extract_commoncrawl.py 
 
# Ingest both datasets into PostgreSQL 
python ingest/ingest.py 

 

Configure dbt 

~/.dbt/profiles.yml 

my_project: 
  target: dev 
  outputs: 
    dev: 
      type: postgres 
      host: localhost 
      user: company_user 
      password: your_password 
      dbname: company_data 
      schema: public 
      threads: 1 

 

Run dbt Transformations 

cd my_project 
 
# Run staging models (standardize schema) 
dbt run --select staging 
 
# Run intermediate models (deduplication) 
dbt run --select intermediate 
 
# Run final model 
dbt run --select final_merged_data 

 

Run dbt Tests 

# Test entity name is not null 
dbt test --select tests/test_non_null_entity_name.sql 
 
# Test ABN is unique 
dbt test --select tests/test_unique_abn.sql 

 

Query Final Merged Table 

SELECT * FROM final_merged_data LIMIT 10; 

 
## Code Implementation

Code Implementation 

DBT Transformation Logic 

models/staging/stg_abr_data.sql 

 

-- Clean and normalize abr_data 

WITH clean_abr AS ( 

    SELECT 

        trim(abn) AS abn, 

        lower(trim(entity_name)) AS entity_name, 

        lower(trim(entity_type)) AS entity_type, 

        lower(trim(entity_status)) AS entity_status, 

        lower(trim(entity_address)) AS entity_address, 

        trim(entity_postcode) AS entity_postcode, 

        trim(entity_state) AS entity_state, 

        entity_start_date 

    FROM {{ source('public', 'abr_data') }} 

    WHERE abn IS NOT NULL AND entity_name IS NOT NULL 

) 

SELECT * FROM clean_abr 


models/staging/stg_common_crawl.sql 

-- Clean and normalize common_crawl_data 

WITH clean_common_crawl AS ( 

    SELECT 

        lower(trim(industry)) AS industry, 

        lower(trim(title)) AS title, 

        trim(url) AS url 

    FROM {{ source('public', 'common_crawl_data') }} 

    WHERE title IS NOT NULL AND url IS NOT NULL 

) 

SELECT * FROM clean_common_crawl 

 

 

models/intermediate/dedup_abr.sql 

WITH deduplicated_abr AS ( 

    SELECT DISTINCT ON (abn) 

        abn, 

        entity_name, 

        entity_type, 

        entity_status, 

        entity_address, 

        entity_postcode, 

        entity_state, 

        entity_start_date 

    FROM {{ ref('stg_abr_data') }} 

    ORDER BY abn 

) 

SELECT * FROM deduplicated_abr 

 

 

models/intermediate/dedup_common_crawl.sql 

WITH deduplicated_common_crawl AS ( 

    SELECT DISTINCT ON (url) 

        industry, 

        title, 

        url 

    FROM {{ ref('stg_common_crawl') }} 

    ORDER BY url 

) 

SELECT * FROM deduplicated_common_crawl 

 

 

 

models/final_merged_data.sql 

 

{{ config(materialized='table') }} 

 

WITH abr_ranked AS ( 

    SELECT *, ROW_NUMBER() OVER () AS rn 

    FROM {{ ref('dedup_abr') }} 

), 

crawl_ranked AS ( 

    SELECT *, ROW_NUMBER() OVER () AS rn 

    FROM {{ ref('dedup_common_crawl') }} 

) 

 

SELECT 

    a.abn, 

    a.entity_name, 

    a.entity_type, 

    a.entity_status, 

    a.entity_address, 

    a.entity_postcode, 

    a.entity_state, 

    a.entity_start_date, 

    c.industry, 

    c.title, 

    c.url 

FROM abr_ranked a 

JOIN crawl_ranked c 

ON a.rn = c.rn 

 
## Data Pipeline Implementation 

extract/extract_abr.py 

import xml.etree.ElementTree as ET 

import csv 

import os 

 

# Set your folder path containing the XML files 

folder_path = r"C:\Users\Vimalan\Downloads\common\abr_bulk_extract_10"  # Replace with your folder 

 

# Prepare CSV 

with open("abr_bulk_extract_10.csv", "w", newline="", encoding="utf-8") as f: 

    writer = csv.writer(f) 

 

    # Required CSV Header 

    writer.writerow([ 

        "ABN (Australian Business Number)", 

        "Entity Name", 

        "Entity Type", 

        "Entity Status", 

        "Entity Address", 

        "Entity Postcode", 

        "Entity State", 

        "Entity Start Date" 

    ]) 

 

    # Loop through each XML file in the folder 

    for filename in os.listdir(folder_path): 

        if filename.endswith(".xml"): 

            file_path = os.path.join(folder_path, filename) 

            try: 

                tree = ET.parse(file_path) 

                root = tree.getroot() 

 

                for abr in root.findall('ABR'): 

                    abn_elem = abr.find('ABN') 

                    abn = abn_elem.text if abn_elem is not None else '' 

                    abn_status = abn_elem.attrib.get('status', '') if abn_elem is not None else '' 

                    abn_start_date = abn_elem.attrib.get('ABNStatusFromDate', '') if abn_elem is not None else '' 

 

                    entity_type_text = '' 

                    entity_type = abr.find('EntityType') 

                    if entity_type is not None: 

                        entity_type_text = entity_type.findtext('EntityTypeText', default='') 

 

                    entity_name = abr.findtext('./MainEntity/NonIndividualName/NonIndividualNameText', default='') 

 

                    state = abr.findtext('./MainEntity/BusinessAddress/AddressDetails/State', default='') 

                    postcode = abr.findtext('./MainEntity/BusinessAddress/AddressDetails/Postcode', default='') 

                    entity_address = f"{state} {postcode}".strip() 

 

                    # Write formatted row 

                    writer.writerow([ 

                        abn, 

                        entity_name, 

                        entity_type_text, 

                        abn_status, 

                        entity_address, 

                        postcode, 

                        state, 

                        abn_start_date 

                    ]) 

 

                # Print the filename after processing 

                print(f"Processed file: {filename}") 

 

            except ET.ParseError as e: 

                print(f"Error parsing {filename}: {e}") 

 

print("CSV created with transformed data from all XML files.") 

 

 

extract/extract_commoncrawl.py 

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

 

 

Ingest/ingest.py 

# data loading in postgres 

 

from pyspark.sql import SparkSession 

 

# Initialize Spark session with PostgreSQL JDBC driver 

spark = SparkSession.builder \ 

    .appName("Load Data into PostgreSQL") \ 

    .config("spark.jars", "postgresql-42.7.5.jar") \ 

    .config("spark.driver.extraClassPath", "postgresql-42.7.5.jar") \ 

    .getOrCreate() 

 

# JDBC connection properties 

url = "jdbc:postgresql://localhost:5432/test_db" 

properties = { 

    "user": "root", 

    "password": "root", 

    "driver": "org.postgresql.Driver" 

} 

 

# Load Common Crawl data in JSONL format 

common_crawl_data = spark.read.json(r"C:\Users\Vimalan\Downloads\common\aus_output.jsonl") 

 

# Load ABR data (CSV) 

abr_data = spark.read.option("header", "true").csv(r"C:\Users\Vimalan\Downloads\common\abr_bulk_extract_10.csv") 

 

# Rename columns for easier access in ABR data 

abr_data = abr_data.withColumnRenamed("ABN (Australian Business Number)", "abn") \ 

                   .withColumnRenamed("Entity Name", "entity_name") \ 

                   .withColumnRenamed("Entity Type", "entity_type") \ 

                   .withColumnRenamed("Entity Status", "entity_status") \ 

                   .withColumnRenamed("Entity Address", "entity_address") \ 

                   .withColumnRenamed("Entity Postcode", "entity_postcode") \ 

                   .withColumnRenamed("Entity State", "entity_state") \ 

                   .withColumnRenamed("Entity Start Date", "entity_start_date") 

 

# Perform any necessary transformations or renaming for Common Crawl data 

common_crawl_data = common_crawl_data.select("industry", "title", "url") 

 

# Perform any necessary transformations or renaming for ABR data 

abr_data = abr_data.select("abn", "entity_name", "entity_type", "entity_status",  

                           "entity_address", "entity_postcode", "entity_state", "entity_start_date") 

 

# Write the Common Crawl data to PostgreSQL (overwrite mode) 

common_crawl_data.write.jdbc(url=url, table="common_crawl_data", mode="overwrite", properties=properties) 

 

# Write the ABR data to PostgreSQL (overwrite mode) 

abr_data.write.jdbc(url=url, table="abr_data", mode="overwrite", properties=properties) 

 

print("Data ingestion into PostgreSQL completed successfully.") 

 

 
## Sample queries for business analysis 

Top 5 States by Number of Registered Companies 

SELECT entity_state, COUNT(*) AS total_companies FROM final_merged_data GROUP BY entity_state ORDER BY total_companies DESC LIMIT 5; 

Count of Companies by Entity Type 

SELECT entity_type, COUNT(*) AS total FROM final_merged_data GROUP BY entity_type ORDER BY total

Companies with the Most Recent Start Date 

SELECT entity_name, entity_start_date FROM final_merged_data ORDER BY entity_start_date DESC LIMIT 5; 
## Architecture

[Common Crawl (S3)] --> [Python Extraction] --> [Staging (JSON/csv)]
[ABR ]          -->                    --> 
                                    |
                                    v
[Apache Spark ETL] --> [S3 Data Lake (Parquet)] --> [PostgreSQL Staging Schema]
                                    |
                                    v
[dbt Transformations] --> [PostgreSQL Production Schema]
                                    |
                                    v
[Analytics/Reporting] <-- [Analyst Queries]
## Test cases and quality checks

$ dbt test
13:32:38  Running with dbt=1.9.4
13:32:38  Registered adapter: postgres=1.9.0
13:32:39  Found 9 models, 4 data tests, 2 sources, 433 macros
13:32:39  
13:32:39  Concurrency: 1 threads (target='dev')
13:32:39
13:32:40  1 of 4 START test not_null_my_first_dbt_model_id ............................... [RUN]
13:32:40  1 of 4 PASS not_null_my_first_dbt_model_id ..................................... [PASS in 0.10s]
13:32:40  2 of 4 START test not_null_my_second_dbt_model_id .............................. [RUN]
13:32:40  2 of 4 PASS not_null_my_second_dbt_model_id .................................... [PASS in 0.09s]
13:32:40  3 of 4 START test unique_my_first_dbt_model_id ................................. [RUN]
13:32:40  3 of 4 PASS unique_my_first_dbt_model_id ....................................... [PASS in 0.08s]
13:32:40  4 of 4 START test unique_my_second_dbt_model_id ................................ [RUN]
13:32:40  4 of 4 PASS unique_my_second_dbt_model_id ...................................... [PASS in 0.07s]
13:32:40
13:32:40  Finished running 4 data tests in 0 hours 0 minutes and 0.66 seconds (0.66s).
13:32:40
13:32:40  Completed successfully
13:32:40
13:32:40  Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4