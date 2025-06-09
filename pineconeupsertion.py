import os
import time
import sqlite3
import re
import boto3
from langchain_pinecone import PineconeVectorStore
from PyPDF2 import PdfReader
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
import logging
from typing import List, Dict
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv(override=True)

# ---------------- CONFIG ----------------
BUCKET = 'denningdata'
PREFIX = 'documents/2025/06'
DB_PATH = '/tmp/processed_docs.db'
PINECONE_INDEX_NAME = 'denningv2'

# Batch processing config
BATCH_SIZE = 20  # Increased batch size for faster processing
MAX_RETRIES = 1  # Reduced retries - skip faster
RETRY_DELAY = 2  # Reduced delay

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------- INIT SERVICES ------------
s3 = boto3.client('s3')
embedding_model = OpenAIEmbeddings(api_key=os.getenv('OPENAI_API_KEY'))
pc = PineconeVectorStore(
    index_name=PINECONE_INDEX_NAME,
    embedding=embedding_model
)

# ------------- SQLITE TRACKING ----------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS processed (
        key TEXT PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'success',
        retry_count INTEGER DEFAULT 0,
        last_error TEXT,
        file_type TEXT
    )
""")
conn.commit()

def already_processed(key: str) -> bool:
    cur.execute("SELECT 1 FROM processed WHERE key = ?", (key,))
    return cur.fetchone() is not None

def mark_as_processed(key: str, status: str = 'success', error: str = None, file_type: str = None):
    try:
        cur.execute("""
            INSERT OR REPLACE INTO processed (key, status, last_error, file_type, processed_at) 
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (key, status, error, file_type))
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to mark {key} as processed: {e}")

def get_failed_files() -> List[str]:
    """Get files that failed but haven't exceeded max retries"""
    try:
        cur.execute("""
            SELECT key FROM processed 
            WHERE status = 'failed' AND retry_count < ?
        """, (MAX_RETRIES,))
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.warning(f"Failed to get failed files: {e}")
        return []

def increment_retry_count(key: str):
    try:
        cur.execute("""
            UPDATE processed 
            SET retry_count = retry_count + 1 
            WHERE key = ?
        """, (key,))
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to increment retry count for {key}: {e}")

# ------------- CHUNKING -----------------
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=768,
    chunk_overlap=128
)

def clean_text(text: str) -> str:
    try:
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)]', '', text)
        return text.strip()
    except Exception:
        return text  # Return as-is if cleaning fails

def extract_pdf_title(file_path: str) -> str:
    try:
        reader = PdfReader(file_path)
        title = reader.metadata.title if reader.metadata else None
        if title and title.strip():
            return title.strip()
    except Exception:
        pass  # Skip any errors
    
    # Fallback: use filename
    try:
        filename = os.path.basename(file_path)
        return os.path.splitext(filename)[0]
    except Exception:
        return "Unknown Document"

# ------------- PROCESS PDF --------------
def process_and_upsert(key: str) -> None:
    """
    Process a single PDF - skip ALL errors and mark as processed
    """
    temp_path = '/tmp/temp_pdf.pdf'
    
    try:
        logger.info(f"Processing: {key}")
        
        # Download file
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            with open(temp_path, 'wb') as f:
                f.write(obj['Body'].read())
        except Exception as e:
            logger.warning(f"Download failed for {key}: {e}")
            mark_as_processed(key, 'skipped', f'Download failed: {e}', 'Download Error')
            return
        
        # Quick file size check
        try:
            file_size = os.path.getsize(temp_path)
            if file_size < 50:  # Very small files
                logger.warning(f"Skipping {key}: too small ({file_size} bytes)")
                mark_as_processed(key, 'skipped', 'File too small', 'Small File')
                return
        except Exception:
            pass  # Continue anyway
        
        # Try to process as PDF
        try:
            title = extract_pdf_title(temp_path)
            loader = PyPDFLoader(temp_path)
            raw_docs = loader.load()
            
            if not raw_docs:
                logger.warning(f"No content from {key}")
                mark_as_processed(key, 'skipped', 'No content extracted', 'Empty PDF')
                return
            
            full_text = " ".join([doc.page_content for doc in raw_docs])
            
            if len(full_text.strip()) < 10:  # Very minimal content
                logger.warning(f"Minimal content in {key}")
                mark_as_processed(key, 'skipped', 'Minimal content', 'Low Content')
                return
            
            # Create chunks
            try:
                chunks = text_splitter.create_documents([clean_text(full_text)])
            except Exception:
                # If chunking fails, try with raw text
                chunks = text_splitter.create_documents([full_text])
            
            if not chunks:
                logger.warning(f"No chunks created for {key}")
                mark_as_processed(key, 'skipped', 'No chunks created', 'Chunking Failed')
                return
            
            # Create documents
            documents = []
            for i, chunk in enumerate(chunks):
                try:
                    doc = Document(
                        page_content=chunk.page_content,
                        metadata={
                            'source': key,
                            'chunk_index': i,
                            'title': title,
                            'text_preview': chunk.page_content[:200] if chunk.page_content else "",
                            'chunk_length': len(chunk.page_content) if chunk.page_content else 0,
                            'document_id': f"{key}-{i}"
                        }
                    )
                    documents.append(doc)
                except Exception:
                    continue  # Skip problematic chunks
            
            if not documents:
                logger.warning(f"No valid documents created for {key}")
                mark_as_processed(key, 'skipped', 'No valid documents', 'Document Creation Failed')
                return
            
            # Try to upsert to Pinecone
            try:
                # Process in smaller batches
                upsert_batch_size = 25
                successful_batches = 0
                
                for i in range(0, len(documents), upsert_batch_size):
                    try:
                        batch = documents[i:i+upsert_batch_size]
                        pc.add_documents(batch)
                        successful_batches += 1
                        time.sleep(0.5)  # Brief pause between batches
                    except Exception as batch_error:
                        logger.warning(f"Batch {i//upsert_batch_size + 1} failed for {key}: {batch_error}")
                        continue  # Skip failed batches but continue with others
                
                if successful_batches > 0:
                    logger.info(f"Successfully processed {key}: {successful_batches} batches upserted")
                    mark_as_processed(key, 'success', None, 'PDF')
                else:
                    logger.warning(f"All batches failed for {key}")
                    mark_as_processed(key, 'skipped', 'All upsert batches failed', 'Upsert Failed')
                
            except Exception as upsert_error:
                logger.warning(f"Upsert completely failed for {key}: {upsert_error}")
                mark_as_processed(key, 'skipped', f'Upsert failed: {upsert_error}', 'Upsert Error')
                
        except Exception as pdf_error:
            logger.warning(f"PDF processing failed for {key}: {pdf_error}")
            mark_as_processed(key, 'skipped', f'PDF processing failed: {pdf_error}', 'PDF Error')
        
    except Exception as general_error:
        logger.warning(f"General error processing {key}: {general_error}")
        mark_as_processed(key, 'skipped', f'General error: {general_error}', 'General Error')
    
    finally:
        # Always clean up temp file
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass  # Ignore cleanup errors

# ------------- S3 ORDERED FETCH ---------
def list_s3_files_sorted(bucket: str, prefix: str) -> List[Dict]:
    """Get S3 objects sorted by LastModified (oldest first)"""
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        objects = []
        for page in page_iterator:
            objects.extend(page.get('Contents', []))
        return sorted(objects, key=lambda obj: obj['LastModified'])
    except Exception as e:
        logger.error(f"Failed to list S3 objects: {e}")
        return []

# ------------- MAIN LOOP ----------------
def run_indexer():
    logger.info("Starting aggressive PDF indexer (skip all errors)...")
    
    while True:
        try:
            logger.info("Checking for new documents...")
            
            # Quick retry of a few failed files (but skip most)
            failed_files = get_failed_files()
            if failed_files:
                logger.info(f"Retrying {min(5, len(failed_files))} previously failed files...")
                for key in failed_files[:5]:  # Only retry 5 files max
                    try:
                        increment_retry_count(key)
                        process_and_upsert(key)
                    except Exception as e:
                        logger.warning(f"Retry failed for {key}: {e}")
                        mark_as_processed(key, 'skipped', f'Retry failed: {e}', 'Retry Failed')
            
            # Process new files aggressively
            s3_objects = list_s3_files_sorted(BUCKET, PREFIX)
            new_files = [
                obj['Key'] for obj in s3_objects 
                if obj['Key'].endswith('.pdf') and not already_processed(obj['Key'])
            ]
            
            if new_files:
                logger.info(f"Found {len(new_files)} new files to process")
                
                # Process in larger batches, skip errors quickly
                for key in new_files[:BATCH_SIZE]:
                    try:
                        process_and_upsert(key)
                    except Exception as e:
                        logger.warning(f"Completely failed to process {key}: {e}")
                        mark_as_processed(key, 'skipped', f'Complete failure: {e}', 'Complete Failure')
                    
                    # Very brief pause
                    time.sleep(0.2)
            else:
                logger.info("No new files found")
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            # Continue anyway - don't let main loop errors stop processing
        
        logger.info("Sleeping before next poll...\n")
        time.sleep(30)  # Reduced sleep time for faster processing

if __name__ == "__main__":
    try:
        run_indexer()
    except KeyboardInterrupt:
        logger.info("Indexer stopped by user")
    except Exception as e:
        logger.error(f"Indexer crashed: {e}")
        # Could add auto-restart logic here if needed