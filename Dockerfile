FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DUCKDB_PATH=/app/fantasy.duckdb

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip streamlit duckdb

COPY pyproject.toml uv.lock ./ 

# Install dependencies pinned by uv.lock

COPY streamlit_app.py load_team.py README.md ./

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
