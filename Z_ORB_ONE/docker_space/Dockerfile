FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir awscli

COPY wheels/ ./wheels/
RUN pip install --no-cache-dir \
    ./wheels/esun_marketdata-2.2.0-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl \
    ./wheels/esun_trade-2.2.0-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

COPY . .

CMD ["python", "execute_strategy_broken_high_falling_docker_v17.py"]