#!/usr/bin/env python3
"""
Load a team's CSV export into DuckDB and assign roto auction prices.

Assumptions:
- AL-only, standard 5x5 categories.
- Default 12 teams, $260 budget per team.
- Default hitter/pitcher budget split of 69%/31%.
"""

import argparse
import csv
import statistics
from pathlib import Path
from typing import Dict, List

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load team stats into DuckDB with roto auction pricing."
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to the CSV file (e.g. bee.csv).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("fantasy.duckdb"),
        help="DuckDB database file to write to.",
    )
    parser.add_argument(
        "--team",
        default="bees",
        help="Source team name to tag rows with.",
    )
    parser.add_argument(
        "--teams",
        type=int,
        default=12,
        help="Number of teams in the league.",
    )
    parser.add_argument(
        "--budget-per-team",
        type=float,
        default=260.0,
        help="Auction budget per team.",
    )
    parser.add_argument(
        "--hitter-budget-share",
        type=float,
        default=0.69,
        help="Fraction of total budget allocated to hitters (rest goes to pitchers).",
    )
    return parser.parse_args()


def to_float(value) -> float:
    try:
        return float(str(value).replace(",", "").strip() or 0.0)
    except (TypeError, ValueError):
        return 0.0


def to_int(value) -> int:
    try:
        return int(float(str(value).replace(",", "").strip() or 0))
    except (TypeError, ValueError):
        return 0


def safe_mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def safe_stdev(values: List[float]) -> float:
    if len(values) < 2:
        return 1.0
    try:
        stdev = statistics.stdev(values)
        return stdev if stdev != 0 else 1.0
    except statistics.StatisticsError:
        return 1.0


def read_sections(csv_path: Path) -> Dict[str, List[Dict[str, str]]]:
    sections: Dict[str, List[Dict[str, str]]] = {}
    current_section = None
    headers: List[str] = []

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue

            label = row[1].strip() if len(row) > 1 else ""
            if label in {"Hitting", "Pitching"}:
                current_section = label.lower()
                headers = []
                continue

            if current_section:
                if not headers:
                    headers = [h.strip() for h in row]
                    sections[current_section] = []
                    continue

                record = {
                    headers[i]: row[i].strip() if i < len(row) else ""
                    for i in range(len(headers))
                }
                sections[current_section].append(record)

    return sections


def compute_prices(
    records: List[Dict[str, str]],
    categories: List[str],
    inverse_categories: List[str],
    budget: float,
) -> None:
    """Mutates records to include a '_price' field based on z-score sums."""
    category_values: Dict[str, List[float]] = {cat: [] for cat in categories}
    for record in records:
        for cat in categories:
            category_values[cat].append(to_float(record.get(cat, 0.0)))

    means = {cat: safe_mean(vals) for cat, vals in category_values.items()}
    stdevs = {cat: safe_stdev(vals) for cat, vals in category_values.items()}

    z_totals: List[float] = []
    for record in records:
        z_sum = 0.0
        for cat in categories:
            val = to_float(record.get(cat, 0.0))
            z = (val - means[cat]) / stdevs[cat]
            if cat in inverse_categories:
                z *= -1
            z_sum += z
        z_totals.append(z_sum)

    positive_total = sum(max(z, 0.0) for z in z_totals) or 1.0
    for record, z_sum in zip(records, z_totals):
        record["_price"] = max(z_sum, 0.0) / positive_total * budget


def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitters (
            source_team TEXT,
            id TEXT,
            pos TEXT,
            player TEXT,
            team TEXT,
            eligible TEXT,
            status TEXT,
            age INTEGER,
            opponent TEXT,
            salary INTEGER,
            contract TEXT,
            ab INTEGER,
            h INTEGER,
            r INTEGER,
            hr INTEGER,
            rbi INTEGER,
            sb INTEGER,
            avg DOUBLE,
            gp INTEGER,
            price DOUBLE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pitchers (
            source_team TEXT,
            id TEXT,
            pos TEXT,
            player TEXT,
            team TEXT,
            eligible TEXT,
            status TEXT,
            age INTEGER,
            opponent TEXT,
            salary INTEGER,
            contract TEXT,
            ip DOUBLE,
            w INTEGER,
            sv INTEGER,
            k INTEGER,
            era DOUBLE,
            whip DOUBLE,
            h INTEGER,
            ab INTEGER,
            r INTEGER,
            rbi INTEGER,
            hr INTEGER,
            sb INTEGER,
            avg DOUBLE,
            gp INTEGER,
            price DOUBLE
        );
        """
    )


def insert_data(
    conn: duckdb.DuckDBPyConnection,
    team: str,
    hitters: List[Dict[str, str]],
    pitchers: List[Dict[str, str]],
) -> None:
    ensure_tables(conn)

    conn.execute("DELETE FROM hitters WHERE source_team = ?", [team])
    conn.execute("DELETE FROM pitchers WHERE source_team = ?", [team])

    hitter_rows = [
        (
            team,
            h.get("ID", ""),
            h.get("Pos", ""),
            h.get("Player", ""),
            h.get("Team", ""),
            h.get("Eligible", ""),
            h.get("Status", ""),
            to_int(h.get("Age")),
            h.get("Opponent", ""),
            to_int(h.get("Salary")),
            h.get("Contract", ""),
            to_int(h.get("AB")),
            to_int(h.get("H")),
            to_int(h.get("R")),
            to_int(h.get("HR")),
            to_int(h.get("RBI")),
            to_int(h.get("SB")),
            to_float(h.get("AVG")),
            to_int(h.get("GP")),
            to_float(h.get("_price", 0.0)),
        )
        for h in hitters
    ]

    pitcher_rows = [
        (
            team,
            p.get("ID", ""),
            p.get("Pos", ""),
            p.get("Player", ""),
            p.get("Team", ""),
            p.get("Eligible", ""),
            p.get("Status", ""),
            to_int(p.get("Age")),
            p.get("Opponent", ""),
            to_int(p.get("Salary")),
            p.get("Contract", ""),
            to_float(p.get("IP")),
            to_int(p.get("W")),
            to_int(p.get("SV")),
            to_int(p.get("K")),
            to_float(p.get("ERA")),
            to_float(p.get("WHIP")),
            to_int(p.get("H")),
            to_int(p.get("AB")),
            to_int(p.get("R")),
            to_int(p.get("RBI")),
            to_int(p.get("HR")),
            to_int(p.get("SB")),
            to_float(p.get("AVG")),
            to_int(p.get("GP")),
            to_float(p.get("_price", 0.0)),
        )
        for p in pitchers
    ]

    conn.executemany(
        """
        INSERT INTO hitters VALUES (
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?
        );
        """,
        hitter_rows,
    )
    conn.executemany(
        """
        INSERT INTO pitchers VALUES (
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?
        );
        """,
        pitcher_rows,
    )


def main() -> None:
    args = parse_args()

    sections = read_sections(args.csv_path)
    hitters = sections.get("hitting", [])
    pitchers = sections.get("pitching", [])

    total_budget = args.budget_per_team * args.teams
    hitter_budget = total_budget * args.hitter_budget_share
    pitcher_budget = total_budget * (1 - args.hitter_budget_share)

    compute_prices(
        hitters,
        categories=["R", "HR", "RBI", "SB", "AVG"],
        inverse_categories=[],
        budget=hitter_budget,
    )
    compute_prices(
        pitchers,
        categories=["W", "SV", "K", "ERA", "WHIP"],
        inverse_categories=["ERA", "WHIP"],
        budget=pitcher_budget,
    )

    conn = duckdb.connect(args.db)
    insert_data(conn, args.team, hitters, pitchers)
    conn.close()

    print(
        f"Loaded {len(hitters)} hitters and {len(pitchers)} pitchers for team '{args.team}' "
        f"into {args.db}"
    )


if __name__ == "__main__":
    main()
