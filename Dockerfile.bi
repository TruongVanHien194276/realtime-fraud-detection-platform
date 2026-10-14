FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /workspace

RUN python -m pip install --no-cache-dir \
    "psycopg[binary]==3.2.10" "pyarrow==23.0.1" "duckdb==1.5.6" \
    "streamlit==1.50.0" "pandas==2.3.3"
RUN python -m pip install --no-cache-dir "openpyxl==3.1.5"

CMD ["bash"]
