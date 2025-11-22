# sammyba
AL Only Auction Baseball

# Installations

## Mac

```
brew install uv
```


# Environment setup

```
uv venv
source .venv/bin/activate
```

# Streamlit UI

Launch locally (after loading data into `fantasy.duckdb` with `load_team.py`):

```
uv run streamlit run streamlit_app.py
```

Docker (mounts your `fantasy.duckdb` into the container):

```
docker compose up --build
```

Then open http://localhost:8501 to browse hitters/pitchers and add placeholder sections from the sidebar.
