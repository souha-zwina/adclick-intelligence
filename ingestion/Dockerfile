FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2
COPY producer.py check_topic.py ./
ENTRYPOINT ["python"]
CMD ["producer.py", "--help"]
