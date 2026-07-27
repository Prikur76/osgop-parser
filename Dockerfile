# Build stage — установка зависимостей
FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir --user .

# Final stage
FROM python:3.12-slim
WORKDIR /app
COPY --from=build /root/.local /root/.local
COPY app ./app
ENV PATH=/root/.local/bin:$PATH

# Непривилегированный пользователь
RUN addgroup --system app && adduser --system --ingroup app app
RUN chown -R app:app /app
USER app

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://localhost:8080/health')"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
