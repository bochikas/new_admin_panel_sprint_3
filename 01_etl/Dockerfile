FROM python:3.9.12-slim

WORKDIR /etl
COPY . .
RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir
CMD python main.py