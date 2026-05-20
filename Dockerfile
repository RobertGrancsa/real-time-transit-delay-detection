FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./

RUN pip install --no-cache-dir --upgrade pip \
	&& pip install --no-cache-dir \
		"aiohttp>=3.10.0" \
		"confluent-kafka>=2.6.0" \
		"gtfs-realtime-bindings>=1.0.0" \
		"joblib>=1.4.0" \
		"numpy>=1.26.0" \
		"pandas>=2.2.0" \
		"protobuf>=5.27.0" \
		"psycopg2-binary>=2.9.9" \
		"python-dotenv>=1.0.0" \
		"requests>=2.32.0" \
		"scikit-learn>=1.5.0" \
		"setuptools>=68.0" \
		"wheel" \
	&& pip cache purge

COPY config ./config
COPY services ./services
COPY scripts ./scripts

RUN pip install --no-cache-dir --no-build-isolation --no-deps -e .

CMD ["python", "-m", "services.gtfs_matching.matcher"]
