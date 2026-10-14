FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    OMP_NUM_THREADS=2 \
    OPENBLAS_NUM_THREADS=2 \
    MKL_NUM_THREADS=2

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir \
    -e ".[dev]" \
    pandas==2.3.3 \
    scikit-learn==1.8.0 \
    mlflow==3.16.1 \
    matplotlib==3.10.7

CMD ["bash"]
