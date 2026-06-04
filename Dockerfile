FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir grpcio grpcio-tools
COPY generated/ generated/
COPY src/ src/
CMD ["python", "src/node.py"]
