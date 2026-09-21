FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py server.py smoke.py test_project.py schema.sql events.jsonl ./
RUN python pipeline.py
USER 10001:10001
CMD ["python", "smoke.py"]
