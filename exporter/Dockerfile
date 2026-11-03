# Sample code, provided as is under the MIT License. See DISCLAIMER.md.
# Build: docker build -t atscale-aggregates-exporter:0.1.0 exporter/
# Pinned by digest for reproducible builds and image scanning; update tag and digest together.
FROM python:3.12.16-slim@sha256:962f801b08c870ac2a51b72bf02dae38c9a4162319e5e882ec244e198188d54e

WORKDIR /app
COPY exporter.py /app/exporter.py

USER 65534:65534
EXPOSE 9102
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "-I", "/app/exporter.py"]
