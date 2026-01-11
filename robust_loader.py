import argparse
import asyncio
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta, UTC
from typing import Literal

from rich.logging import RichHandler
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn

from parliament_mcp.settings import settings

# Configure logging
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger("robust_loader")
console = Console()

DB_PATH = "loader_state.db"

class QueueManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def init_db(self):
        """Initialize the SQLite database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main queue table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,  -- 'hansard' or 'pq'
                date TEXT NOT NULL,         -- YYYY-MM-DD
                status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, PROCESSING, COMPLETED, FAILED
                error_message TEXT,
                attempts INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                metadata TEXT               -- JSON blob for extra info needed for processing
            )
        ''')
        
        # Index for faster retrieval of pending items
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON queue (status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON queue (date)')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")

    def add_item(self, id: str, source_type: str, date: str, metadata: str = None):
        """Add an item to the queue if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO queue (id, source_type, date, metadata)
                VALUES (?, ?, ?, ?)
            ''', (id, source_type, date, metadata))
            conn.commit()
            return cursor.rowcount > 0 # True if inserted, False if ignored
        finally:
            conn.close()

    def get_pending_batch(self, limit: int = 100):
        """Get a batch of pending items."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT * FROM queue 
                WHERE status = 'PENDING' 
                ORDER BY date ASC, id ASC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def mark_processing(self, ids: list[str]):
        """Mark items as PROCESSING."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.executemany('''
                UPDATE queue 
                SET status = 'PROCESSING', last_attempt = CURRENT_TIMESTAMP, attempts = attempts + 1
                WHERE id = ?
            ''', [(id,) for id in ids])
            conn.commit()
        finally:
            conn.close()

    def mark_completed(self, ids: list[str]):
        """Mark items as COMPLETED."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.executemany('''
                UPDATE queue 
                SET status = 'COMPLETED', error_message = NULL
                WHERE id = ?
            ''', [(id,) for id in ids])
            conn.commit()
        finally:
            conn.close()

    def mark_failed(self, id: str, error: str):
        """Mark a single item as FAILED."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE queue 
                SET status = 'FAILED', error_message = ?
                WHERE id = ?
            ''', (error, id))
            conn.commit()
        finally:
            conn.close()

    def get_stats(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT status, COUNT(*) FROM queue GROUP BY status')
            return dict(cursor.fetchall())
        finally:
            conn.close()
            
    def get_daily_stats(self, date_str: str, source_type: str = None):
        """Get item counts for a specific day."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            query = 'SELECT status, COUNT(*) FROM queue WHERE date = ?'
            params = [date_str]
            if source_type:
                query += ' AND source_type = ?'
                params.append(source_type)
            query += ' GROUP BY status'
            
            cursor.execute(query, params)
            return dict(cursor.fetchall())
        finally:
            conn.close()

    def reset_processing(self):
        """Reset stuck PROCESSING items back to PENDING (e.g. after crash)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE queue 
                SET status = 'PENDING'
                WHERE status = 'PROCESSING'
            ''')
            count = cursor.rowcount
            conn.commit()
            if count > 0:
                logger.info(f"Reset {count} stuck items from PROCESSING to PENDING")
        finally:
            conn.close()

    def retry_failed(self):
        """Reset FAILED items back to PENDING for retry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE queue 
                SET status = 'PENDING', error_message = NULL
                WHERE status = 'FAILED'
            ''')
            count = cursor.rowcount
            conn.commit()
            if count > 0:
                logger.info(f"Reset {count} FAILED items to PENDING for retry")
            else:
                logger.info("No FAILED items found to retry")
        finally:
            conn.close()

def init_db_command(args):
    QueueManager().init_db()

from parliament_mcp.qdrant_data_loaders import (
    HANSARD_BASE_URL, 
    PQS_BASE_URL
)
from parliament_mcp.models import ContributionsResponse, ParliamentaryQuestionsResponse
import httpx
from aiolimiter import AsyncLimiter

# HTTP rate limiter
_http_client_rate_limiter = AsyncLimiter(max_rate=settings.HTTP_MAX_RATE_PER_SECOND, time_period=1.0)

async def cached_limited_get(*args, **kwargs) -> httpx.Response:
    """
    A wrapper around httpx.get that limits the rate of requests.
    Caching is disabled for the robust loader to avoid dependency issues.
    """
    async with (
        httpx.AsyncClient(
            timeout=120,
            headers={"User-Agent": "parliament-mcp"},
            transport=httpx.AsyncHTTPTransport(retries=3),
        ) as client,
        _http_client_rate_limiter,
    ):
        return await client.get(*args, **kwargs)

class Harvester:
    def __init__(self, queue_manager: QueueManager):
        self.qm = queue_manager
        self.page_size = 100 # Large page size for faster harvesting

    async def harvest_date_range(self, start_date: date, end_date: date, harvest_type: str = "all"):
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logger.info(f"Harvesting {date_str}...")
            
            tasks = []
            if harvest_type in ["all", "hansard"]:
                for c_type in ["Spoken", "Written", "Corrections", "Petitions"]:
                    tasks.append(self.harvest_hansard(date_str, c_type))
            
            if harvest_type in ["all", "pqs"]:
                tasks.append(self.harvest_pqs(date_str, "tabled"))
                tasks.append(self.harvest_pqs(date_str, "answered"))
            
            await asyncio.gather(*tasks)
            current_date += timedelta(days=1)

    async def harvest_hansard(self, date_str: str, contrib_type: str):
        url = f"{HANSARD_BASE_URL}/search/contributions/{contrib_type}.json"
        base_params = {
            "orderBy": "SittingDateAsc",
            "startDate": date_str,
            "endDate": date_str,
            "take": self.page_size,
        }
        
        skip = 0
        while True:
            params = base_params | {"skip": skip}
            try:
                response = await cached_limited_get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Check for total results on first page to know if we are done? 
                # Actually, we can just check if Results is empty.
                if not data.get("Results"):
                    break
                
                items = data.get("Results", [])
                if not items:
                    break

                for item in items:
                    item_id = item.get("ContributionExtId") or item.get("Id")
                    queue_id = f"hansard_{item_id}" 
                    
                    import json
                    meta = json.dumps({
                        "id": item_id, 
                        "type": contrib_type,
                        "item_data": item
                    })
                    
                    self.qm.add_item(queue_id, "hansard", date_str, meta)

                skip += self.page_size
                
                if skip >= data.get("TotalResultCount", 0):
                    break
                    
            except Exception as e:
                logger.error(f"Error harvesting hansard {date_str} {contrib_type} page {skip}: {e}")
                break

    async def harvest_pqs(self, date_str: str, date_type: str):
        url = f"{PQS_BASE_URL}/writtenquestions/questions"
        base_params = {
            "take": self.page_size,
            f"{date_type}WhenFrom": date_str,
            f"{date_type}WhenTo": date_str,
        }
        
        skip = 0
        while True:
            params = base_params | {"skip": skip}
            try:
                response = await cached_limited_get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                items = data.get("results", [])
                if not items:
                    break
                    
                for item in items:
                    val = item.get("value", {})
                    pq_id = val.get("id")
                    queue_id = f"pq_{pq_id}"
                    
                    import json
                    meta = json.dumps({
                        "id": pq_id,
                        "type": date_type # tabled or answered
                    })
                    
                    self.qm.add_item(queue_id, "pq", date_str, meta)
                
                skip += self.page_size
                if skip >= data.get("totalResults", 0):
                    break
                    
            except Exception as e:
                logger.error(f"Error harvesting PQs {date_str} {date_type} page {skip}: {e}")
                break

async def harvest_command(args):
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        return

    qm = QueueManager()
    qm.init_db() # Ensure tables exist
    
    harvester = Harvester(qm)
    await harvester.harvest_date_range(start_date, end_date, args.type)
    
    stats = qm.get_stats()
    logger.info(f"Harvest complete. Queue stats: {stats}")

from parliament_mcp.qdrant_data_loaders import (
    QdrantHansardLoader, 
    QdrantParliamentaryQuestionLoader,
    cached_limited_get as original_cached_limited_get
)
from parliament_mcp.qdrant_helpers import get_async_qdrant_client
from parliament_mcp.models import Contribution, ParliamentaryQuestion
import parliament_mcp.qdrant_data_loaders
import json
from qdrant_client import AsyncQdrantClient

# Monkeypatch cached_limited_get to use our local version (without hishel)
parliament_mcp.qdrant_data_loaders.cached_limited_get = cached_limited_get

class Processor:
    def __init__(self, queue_manager: QueueManager):
        self.qm = queue_manager
        
    async def process_queue_loop(self, batch_size: int = 50, loop: bool = False, max_items: int = 0):
        # Override settings for host execution
        # We explicitly use localhost here to avoid any ambiguity
        qdrant_url = "http://localhost:6333"
        
        client = AsyncQdrantClient(url=qdrant_url, api_key=settings.QDRANT_API_KEY, timeout=30)
        try:
            qdrant_client = client
            # Initialize Loaders
            hansard_loader = QdrantHansardLoader(
                qdrant_client=qdrant_client,
                collection_name=settings.HANSARD_CONTRIBUTIONS_COLLECTION,
                settings=settings
            )
            pq_loader = QdrantParliamentaryQuestionLoader(
                qdrant_client=qdrant_client,
                collection_name=settings.PARLIAMENTARY_QUESTIONS_COLLECTION,
                settings=settings
            )
            
            total_processed_session = 0
            
            while True:
                # Check limit
                if max_items > 0 and total_processed_session >= max_items:
                    logger.info(f"Reached limit of {max_items} items. Stopping.")
                    break

                items = self.qm.get_pending_batch(limit=batch_size)
                if not items:
                    if loop:
                        stats = self.qm.get_stats()
                        logger.info(f"Queue empty. Stats: {stats}. Waiting 10s...")
                        await asyncio.sleep(10)
                        continue
                    else:
                        stats = self.qm.get_stats()
                        logger.info(f"Queue empty. Stats: {stats}. Processing complete.")
                        break
                
                # Calculate progress
                stats = self.qm.get_stats()
                total = sum(stats.values())
                completed = stats.get('COMPLETED', 0)
                failed = stats.get('FAILED', 0)
                processed_count = completed + failed
                progress_pct = (processed_count / total * 100) if total > 0 else 0.0

                logger.info(f"Processing batch of {len(items)} items... ({progress_pct:.2f}% | {processed_count}/{total})")
                
                # Mark as processing
                item_ids = [item['id'] for item in items]
                self.qm.mark_processing(item_ids)
                
                # Split by type
                hansard_items = [i for i in items if i['source_type'] == 'hansard']
                pq_items = [i for i in items if i['source_type'] == 'pq']
                
                # Process Hansard
                if hansard_items:
                    await self.process_hansard_items(hansard_loader, hansard_items)
                    
                # Process PQs
                if pq_items:
                    await self.process_pq_items(pq_loader, pq_items)
                    
                total_processed_session += len(items)

        finally:
            await client.close()

    async def process_hansard_items(self, loader: QdrantHansardLoader, items: list[dict]):
        docs_to_store = []
        processed_ids = []
        
        for item in items:
            try:
                meta = json.loads(item['metadata'])
                if 'item_data' not in meta:
                    self.qm.mark_failed(item['id'], "Missing item_data in metadata")
                    continue
                
                # Reconstruct Contribution
                contribution = Contribution.model_validate(meta['item_data'])
                
                if contribution.SittingDate:
                    contribution.debate_parents = await loader.get_debate_parents(
                        contribution.SittingDate.strftime("%Y-%m-%d"),
                        contribution.House,
                        contribution.DebateSectionExtId,
                    )
                
                docs_to_store.append(contribution)
                processed_ids.append(item['id'])

            except Exception as e:
                logger.error(f"Failed to process Hansard item {item['id']}: {e}")
                self.qm.mark_failed(item['id'], str(e))
        
        if docs_to_store:
            try:
                await loader.store_in_qdrant_batch(docs_to_store)
                self.qm.mark_completed(processed_ids)
                logger.info(f"Successfully processed {len(processed_ids)} Hansard items")
            except Exception as e:
                logger.error(f"Batch upsert failed: {e}")
                for pid in processed_ids:
                    self.qm.mark_failed(pid, f"Batch upsert error: {e}")
    
    async def process_pq_items(self, loader: QdrantParliamentaryQuestionLoader, items: list[dict]):
        docs_to_store = []
        processed_ids = []
        
        for item in items:
            try:
                meta = json.loads(item['metadata'])
                pq_id = meta.get('id')
                
                url = f"{PQS_BASE_URL}/writtenquestions/questions/{pq_id}"
                response = await cached_limited_get(url, params={"expandMember": True})
                response.raise_for_status()
                data = response.json()
                
                pq = ParliamentaryQuestion.model_validate(data["value"])
                docs_to_store.append(pq)
                processed_ids.append(item['id'])
                
            except Exception as e:
                logger.error(f"Failed to process PQ {item['id']}: {e}")
                self.qm.mark_failed(item['id'], str(e))

        if docs_to_store:
            try:
                await loader.store_in_qdrant_batch(docs_to_store)
                self.qm.mark_completed(processed_ids)
                logger.info(f"Successfully processed {len(processed_ids)} PQs")
            except Exception as e:
                logger.error(f"Batch upsert failed: {e}")
                for pid in processed_ids:
                    self.qm.mark_failed(pid, f"Batch upsert error: {e}")

async def process_command(args):
    qm = QueueManager()
    processor = Processor(qm)
    await processor.process_queue_loop(args.batch_size, args.loop, args.limit)

class AuditManager:
    def __init__(self, queue_manager: QueueManager):
        self.qm = queue_manager

    async def audit_date_range(self, start_date: date, end_date: date, audit_type: str = "all"):
        current_date = start_date
        logger.info(f"Auditing from {start_date} to {end_date}...")
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            
            # 1. Check local DB stats
            stats_hansard = self.qm.get_daily_stats(date_str, 'hansard')
            stats_pq = self.qm.get_daily_stats(date_str, 'pq')
            
            # Audit Hansard
            if audit_type in ["all", "hansard"]:
                await self.check_day(date_str, "hansard", stats_hansard)

            # Audit PQs
            if audit_type in ["all", "pqs"]:
                await self.check_day(date_str, "pq", stats_pq)
                
            current_date += timedelta(days=1)

    async def check_day(self, date_str: str, source_type: str, stats: dict):
        total_local = sum(stats.values())
        failed = stats.get('FAILED', 0)
        pending = stats.get('PENDING', 0)
        processing = stats.get('PROCESSING', 0)
        
        prefix = f"[{date_str}] [{source_type.upper()}]"
        
        # Check for processing issues
        if failed > 0 or pending > 0 or processing > 0:
            logger.warning(f"{prefix} INCOMPLETE: {pending} pending, {failed} failed, {processing} processing. (Total: {total_local})")
            return # Don't check API if we already know we have work to do

        # If local is 0, verify with API if it SHOULD be 0
        if total_local == 0:
            api_count = await self.get_api_count(date_str, source_type)
            if api_count > 0:
                logger.error(f"{prefix} MISSING: API reports {api_count} items, but Queue has 0. Run harvest!")
            else:
                # API confirms 0 results. It's a valid empty day.
                # logger.debug(f"{prefix} EMPTY: Verified with API.") 
                pass
        else:
             # We have items, and they are all completed.
             # Ideally we could check if Local Count == API Count, but API counts change slightly or require summing.
             # For now, if we have COMPLETED items, we assume success.
             # logger.info(f"{prefix} OK: {total_local} items completed.")
             pass

    async def get_api_count(self, date_str: str, source_type: str) -> int:
        try:
            if source_type == 'hansard':
                # Check total contributions (sum of types)
                total = 0
                for c_type in ["Spoken", "Written", "Corrections", "Petitions"]:
                    url = f"{HANSARD_BASE_URL}/search/contributions/{c_type}.json"
                    params = {"startDate": date_str, "endDate": date_str, "take": 1}
                    response = await cached_limited_get(url, params=params)
                    if response.status_code == 200:
                        total += response.json().get("TotalResultCount", 0)
                return total
            
            elif source_type == 'pq':
                # Check tabled and answered
                total = 0
                url = f"{PQS_BASE_URL}/writtenquestions/questions"
                
                # Tabled
                params = {f"tabledWhenFrom": date_str, f"tabledWhenTo": date_str, "take": 1}
                response = await cached_limited_get(url, params=params)
                if response.status_code == 200:
                    total += response.json().get("totalResults", 0)
                
                # Answered
                params = {f"answeredWhenFrom": date_str, f"answeredWhenTo": date_str, "take": 1}
                response = await cached_limited_get(url, params=params)
                if response.status_code == 200:
                    total += response.json().get("totalResults", 0)
                    
                return total
                
        except Exception as e:
            logger.warning(f"Failed to fetch API count for {date_str} {source_type}: {e}")
            return 0
        return 0

async def audit_command(args):
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        return

    qm = QueueManager()
    am = AuditManager(qm)
    await am.audit_date_range(start_date, end_date, args.type)

def main():
    parser = argparse.ArgumentParser(description="Robust Data Loader for Parliament MCP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init-db
    subparsers.add_parser("init-db", help="Initialize the local state database")

    # harvest
    harvest_parser = subparsers.add_parser("harvest", help="Fetch IDs and populate the queue")
    harvest_parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    harvest_parser.add_argument("--end-date", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    harvest_parser.add_argument("--type", choices=["all", "hansard", "pqs"], default="all")

    # process
    process_parser = subparsers.add_parser("process", help="Process the queue (fetch full data, embed, upsert)")
    process_parser.add_argument("--batch-size", type=int, default=50, help="Number of items to process at once")
    process_parser.add_argument("--loop", action="store_true", help="Keep running until queue is empty")
    process_parser.add_argument("--limit", type=int, default=0, help="Maximum number of items to process")

    # reset
    subparsers.add_parser("reset", help="Reset stuck items from PROCESSING to PENDING")

    # retry-failed
    subparsers.add_parser("retry-failed", help="Reset FAILED items to PENDING for retry")

    # audit
    audit_parser = subparsers.add_parser("audit", help="Audit data completeness against Parliament API")
    audit_parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    audit_parser.add_argument("--end-date", default=datetime.now().strftime("%Y-%m-%d"), help="YYYY-MM-DD")
    audit_parser.add_argument("--type", choices=["all", "hansard", "pqs"], default="all")

    args = parser.parse_args()

    if args.command == "init-db":
        init_db_command(args)
    elif args.command == "harvest":
        asyncio.run(harvest_command(args))
    elif args.command == "process":
        asyncio.run(process_command(args))
    elif args.command == "reset":
        QueueManager().reset_processing()
    elif args.command == "retry-failed":
        QueueManager().retry_failed()
    elif args.command == "audit":
        asyncio.run(audit_command(args))

if __name__ == "__main__":
    main()