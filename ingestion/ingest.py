from jupyterlab_server import slugify
from pymongo import ASCENDING,MongoClient, errors as mongo_errors
import argparse
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()
MONGO_URI= os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME= os.getenv("DB_NAME", "default_database")
COLLECTION_NAME= os.getenv("COLLECTION_NAME", "default_collection")
SUPPORTED_EXT = {".pdf", ".docx", ".txt", ".md", ".html", ".htm"}
from tqdm import tqdm



def ensure_indexes(coll):
    """- checksum: unique index, prevents inserting duplicate chunks
       - (source_id, chunk_index): speeds up per-document queries later
       - language: lets you filter by language quickly"""
    try:
        coll.create_index([("checksum", ASCENDING)], unique=True, name = "unique_checksum")
        coll.create_index([("source_id", ASCENDING), ("chunk_index", ASCENDING)], name = "source_chunk")
        coll.create_index([("language", ASCENDING)], name="lang_idx")
    except mongo_errors.PyMongoError as e:
        print(f"Error creating indexes: {e}")

def guess_mime(ext: str)-> str:
    return {
        ".pdf":  "application/pdf",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".txt":  "text/plain",
        ".md":   "text/markdown",
        ".html": "text/html",
        ".htm":  "text/html",
    }.get(ext, "application/octet-stream")


def main():
    parser = argparse.ArgumentParser(description="Ingest documents with OCR fallback")
    parser.add_argument("--inputs", type= str, required=True, help="Input folder which contains files")
    parser.add_argument("--out", type=str, required=True, help="Output folder to for metadata.jsonl")
    args = parser.parse_args()

    inputs= Path(args.inputs).resolve()
    outdir= Path(args.out).resolve()
    outdir.mkdir(parents=True, exist_ok=True)


    #now we connect to mongo
    client= MongoClient(MONGO_URI)
    db= client[DB_NAME]
    coll=db[COLLECTION_NAME]
    ensure_indexes(coll)

    jsonl_path= outdir / "metadata.jsonl"
    jsonl_file= open(jsonl_path, "a", encoding="utf-8")

    files = [p for p in inputs.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXT]
    if not files:
        print(f"[info] No supported files found in  {inputs}")
        jsonl_file.close()
        return 0
    
    ##dedupe
    seen_checksums = set() 

    for path in tqdm(files, desc= "Ingesting"):
        try:
            #first we extract
            text, source_type = extract_text(path) #also has OCR fallback
            text= normalize_text(text)
            if not text:
                print(f"[warning] No text extracted from {path.name}, skipping.")
                continue

            #metadata
            ext= path.suffix.lower()
            mime = guess_mime(ext)
            title = path.stem
            lang = detect_lang(text)

            #saving a clean doc copy
            clean_dir= ROOT/ "data" / "clean"  
            clean_dir.mkdir(parents=True, exist_ok=True)
            (clean_dir/ f"{slugify(title)}.txt").write_text(text, encoding="utf-8")

            #now we chunk
            chunks = chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)

            for idx, (start, end, chunk) in enumerate(chunks):
                checksum = sha256(chunk.lower().strip())






            


    






        
    