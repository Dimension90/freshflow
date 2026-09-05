FROM python:3.12.11-alpine3.22
WORKDIR /app
COPY ml/eta-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && addgroup -S freshflow \
    && adduser -S -G freshflow freshflow
COPY ml/eta-service/app ./app
USER freshflow
EXPOSE 8090
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
