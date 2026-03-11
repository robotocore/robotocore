FROM python:3.12-slim

WORKDIR /app

# Install backend dependencies
COPY vm/requirements.txt /app/vm/requirements.txt
RUN pip install --no-cache-dir -r /app/vm/requirements.txt

# Copy backend
COPY vm/ /app/vm/

# Copy UI
COPY ui/ /app/ui/

# Copy metadata
COPY metadata.json /app/
COPY docker-compose.yaml /app/

LABEL com.docker.desktop.extension.api.version="0.3.4"
LABEL com.docker.desktop.extension.icon="robotocore.svg"

EXPOSE 8080

CMD ["python", "/app/vm/main.py"]
