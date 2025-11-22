import os
from pathlib import Path
from typing import Dict, List

import duckdb
import streamlit as st


DB_PATH = Path(os.getenv("DUCKDB_PATH", "fantasy.duckdb"))


def get_connection(db_path: Path) -> duckdb.DuckDBPyConnection | None:
    if not db_path.exists():
        st.error(f"DuckDB file not found at {db_path}. Load data first with load_team.py.")
        return None
    return duckdb.connect(str(db_path), read_only=True)


def fetch_table(conn: duckdb.DuckDBPyConnection, query: str) -> object:
    try:
        return conn.execute(query).fetch_arrow_table()
    except duckdb.CatalogException:
        st.warning("Expected tables not found. Run load_team.py to populate data.")
    return None


def ensure_custom_sections() -> List[str]:
    if "custom_sections" not in st.session_state:
        st.session_state["custom_sections"] = []
    return st.session_state["custom_sections"]


def sidebar_sections(base_sections: List[str]) -> str:
    custom_sections = ensure_custom_sections()
    with st.sidebar:
        st.header("Sections")
        st.write("Hitters and pitchers are built-in. Add more placeholders as you grow.")
        new_section = st.text_input("Add a section", placeholder="Prospects, Trades, etc.")
        if st.button("Add", disabled=not new_section.strip()):
            name = new_section.strip()
            if name not in custom_sections and name not in base_sections:
                custom_sections.append(name)
            st.session_state["custom_sections"] = custom_sections
            st.rerun()

        selection = st.radio(
            "View",
            base_sections + custom_sections,
            label_visibility="collapsed",
        )
    return selection


def render_hitters(conn: duckdb.DuckDBPyConnection) -> None:
    query = """
        SELECT
            source_team,
            player,
            pos,
            team,
            salary,
            contract,
            ab,
            h,
            r,
            hr,
            rbi,
            sb,
            avg,
            gp,
            price
        FROM hitters
        ORDER BY source_team, price DESC;
    """
    table = fetch_table(conn, query)
    if table:
        st.subheader("Hitters")
        st.dataframe(table, use_container_width=True)


def render_pitchers(conn: duckdb.DuckDBPyConnection) -> None:
    query = """
        SELECT
            source_team,
            player,
            pos,
            team,
            salary,
            contract,
            ip,
            w,
            sv,
            k,
            era,
            whip,
            h,
            ab,
            r,
            rbi,
            hr,
            sb,
            avg,
            gp,
            price
        FROM pitchers
        ORDER BY source_team, price DESC;
    """
    table = fetch_table(conn, query)
    if table:
        st.subheader("Pitchers")
        st.dataframe(table, use_container_width=True)


def render_placeholder(name: str) -> None:
    st.subheader(name)
    st.info("New section placeholder. Wire this up to DuckDB when data is ready.")


def main() -> None:
    st.set_page_config(page_title="AL Only Auction", layout="wide")
    st.title("AL Only Auction Dashboard")
    st.caption("Backed by DuckDB. Use load_team.py to import CSVs before viewing.")

    base_sections = ["Hitters", "Pitchers"]
    selection = sidebar_sections(base_sections)

    conn = get_connection(DB_PATH)
    if not conn:
        return

    if selection == "Hitters":
        render_hitters(conn)
    elif selection == "Pitchers":
        render_pitchers(conn)
    else:
        render_placeholder(selection)


if __name__ == "__main__":
    main()
