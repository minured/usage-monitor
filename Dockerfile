FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app/usage-monitor

COPY usage_monitor ./usage_monitor
COPY README.md ./
COPY .env.example ./

EXPOSE 8765

CMD ["python", "-m", "usage_monitor.web"]
