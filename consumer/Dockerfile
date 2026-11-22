FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer_hdfs.py .

CMD ["python", "-u", "consumer_hdfs.py"]
