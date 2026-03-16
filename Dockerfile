FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV FINANCE_CLI_BASE_DIR=/app
ENV FINANCE_CLI_DB_PATH=/app/state/finance_api.db

EXPOSE 8000

CMD ["uvicorn", "finance_cli.api_app:app", "--host", "0.0.0.0", "--port", "8000"]
