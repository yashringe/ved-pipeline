# Spark curation job: pip-installed PySpark in local[*] mode plus the S3A connector.
FROM python:3.11-slim-bookworm

# hadoop-aws must match the Hadoop client bundled with pyspark 3.5.3 (3.3.4), and the AWS
# SDK bundle must be the one that hadoop-aws release was built against.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG SPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# procps: Spark's launcher scripts call `ps`.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system app \
    && useradd --system --gid app --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[spark]"

ADD --chmod=644 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    ${SPARK_JARS}/
ADD --chmod=644 \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    ${SPARK_JARS}/

USER app
