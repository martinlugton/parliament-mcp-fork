"Parliamentary Data Query Builder (Advanced Version)

A powerful CLI for semantic and contextual search against the UK Parliamentary vector store.

Core Modes:
    hansard:      Standard semantic search across speeches.
    contributors: Group speeches by MP to find experts/vocal members.
    pqs:         Search Written Questions and Answers.
    recommend:   Find speeches similar to one or more example IDs.
    discover:    Find speeches similar to a target ID, refined by positive/negative context pairs.

Advanced Filtering:
    --limit:      Number of results.
    --date-from / --date-to: Time bounding.
    --party:      Filter by political party (currently PQs only).
    --house:      "Commons" or "Lords".
    --member-id:  Include only this MP.
    --exclude-member: Exclude these MP IDs.
    --diversify:  Spread results across different debates (Hansard mode).

Example - Discover (Contextual):
    docker compose exec mcp-server uv run python query_builder.py discover "TARGET_ID" --context "POS_ID,NEG_ID"

""

import asyncio
import argparse
import sys
import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from parliament_mcp.mcp_server.qdrant_query_handler import QdrantQueryHandler
from parliament_mcp.openai_helpers import get_openai_client
from parliament_mcp.qdrant_helpers import get_async_qdrant_client
from parliament_mcp.settings import settings

console = Console()

def format_date(date_str):
    if not date_str: return "Unknown"
    return str(date_str).split('T')[0]

async def run_contributors_query(handler, args):
    """Identifies MPs/Lords who have spoken most relevantly about a topic."""
    results = await handler.find_relevant_contributors(
        query=args.query,
        num_contributors=args.limit,
        num_contributions=args.contributions_per_member,
        date_from=args.date_from,
        date_to=args.date_to,
        house=args.house
    )
    
    if not results:
        console.print("[yellow]No relevant contributors found.[/yellow]")
        return

    for group in results:
        if not group: continue
        first = group[0]
        table = Table(title=f"MP: {first['member_name']} (ID: {first['member_id']})", show_header=True, header_style="bold magenta")
        table.add_column("Date", style="dim", width=12)
        table.add_column("Contribution Snippet")
        
        for hit in group:
            date = format_date(hit.get('date'))
            text = hit.get('text', '').replace('\n', ' ')[:200] + "..."
            table.add_row(date, text)
        
        console.print(table)
        console.print("\n")

async def run_hansard_query(handler, args):
    results = await handler.search_hansard_contributions(
        query=args.query if args.mode == "hansard" else None,
        max_results=args.limit,
        date_from=args.date_from,
        date_to=args.date_to,
        member_id=args.member_id,
        house=args.house,
        exclude_member_ids=args.exclude_member,
        parties=args.party,
        group_by="DebateSectionExtId" if args.diversify else None
    )
    
    if not results:
        console.print("[yellow]No results found.[/yellow]")
        return

    if args.diversify:
        for group in results:
            if not group: continue
            first = group[0]
            console.print(Panel(f"[bold cyan]Debate:[/bold cyan] {first.get('debate_title')}\n[bold magenta]MP:[/bold magenta] {first.get('member_name')}\n\n{first.get('text')[:300]}...", title=f"Result ID: {first['id']}"))
    else:
        table = Table(title=f"Search Results: '{args.query or 'Recent'}'")
        table.add_column("ID", style="dim")
        table.add_column("Date", width=12)
        table.add_column("Member", style="cyan")
        table.add_column("Text")
        
        for r in results:
            table.add_row(
                str(r.get('id', '')),
                format_date(r.get('date')),
                r.get('member_name', 'Unknown'),
                r.get('text', '')[:150] + "..."
            )
        console.print(table)

async def run_pqs_query(handler, args):
    results = await handler.search_parliamentary_questions(
        query=args.query,
        max_results=args.limit,
        date_from=args.date_from,
        date_to=args.date_to,
        party=args.party[0] if args.party else None
    )
    
    if not results:
        console.print("[yellow]No Parliamentary Questions found.[/yellow]")
        return

    for r in results:
        console.print(Panel(
            f"[bold cyan]Question:[/bold cyan] {r['question_text'][:300]}...\n\n"
            f"[bold green]Answer:[/bold green] {r['answer_text'][:300]}...",
            title=f"PQ {r.get('dateTabled')} - {r.get('askingMember', {}).get('name', 'Unknown')}",
            subtitle=f"Answering Body: {r.get('answeringBodyName')}"
        ))

async def run_recommend_query(handler, args):
    positives = args.query.split(',')
    negatives = args.negatives.split(',') if args.negatives else []
    
    console.print(f"Finding contributions similar to: {positives}...")
    results = await handler.recommend_contributions(
        positive_ids=positives,
        negative_ids=negatives,
        max_results=args.limit
    )
    
    table = Table(title="Recommendations")
    table.add_column("ID")
    table.add_column("Member")
    table.add_column("Text")
    for r in results:
        table.add_row(str(r['id']), r['member_name'], r['text'][:200] + "...")
    console.print(table)

async def run_discover_query(handler, args):
    pairs = []
    if args.context:
        for p in args.context.split(';'):
            parts = p.split(',')
            if len(parts) == 2:
                pairs.append((parts[0], parts[1]))

    console.print(f"Discovering points relative to target {args.query} with context {pairs}...")
    results = await handler.discover_contributions(
        target_id=args.query,
        context_pairs=pairs,
        max_results=args.limit
    )
    
    table = Table(title="Discovery Results")
    table.add_column("ID")
    table.add_column("Member")
    table.add_column("Score")
    table.add_column("Text")
    for r in results:
        table.add_row(str(r['id']), r['member_name'], f"{r['score']:.4f}", r['text'][:200] + "...")
    console.print(table)

async def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("mode", choices=["hansard", "contributors", "pqs", "recommend", "discover"])
    parser.add_argument("query", help="Query text, or ID(s) for recommend/discover modes")
    
    # Filtering
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--date-from", help="YYYY-MM-DD")
    parser.add_argument("--date-to", help="YYYY-MM-DD")
    parser.add_argument("--party", nargs='+', help="One or more parties")
    parser.add_argument("--house", choices=["Commons", "Lords"])
    parser.add_argument("--member-id", type=int)
    parser.add_argument("--exclude-member", type=int, nargs='+', help="Exclude these Member IDs")
    parser.add_argument("--diversify", action="store_true", help="Spread results across different debates")
    
    # Mode-specific
    parser.add_argument("--negatives", help="Comma-separated IDs for recommend mode")
    parser.add_argument("--context", help="Context pairs for discover mode: 'pos1,neg1;pos2,neg2'")
    parser.add_argument("--contributions-per-member", type=int, default=3)

    args = parser.parse_args()

    openai_client = get_openai_client(settings)
    async with get_async_qdrant_client(settings) as qdrant_client:
        handler = QdrantQueryHandler(qdrant_client, openai_client, settings)
        
        if args.mode == "hansard":
            await run_hansard_query(handler, args)
        elif args.mode == "contributors":
            await run_contributors_query(handler, args)
        elif args.mode == "pqs":
            await run_pqs_query(handler, args)
        elif args.mode == "recommend":
            await run_recommend_query(handler, args)
        elif args.mode == "discover":
            await run_discover_query(handler, args)

if __name__ == "__main__":
    asyncio.run(main())
