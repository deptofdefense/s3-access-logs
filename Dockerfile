FROM python:3.8

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pandas pyarrow pytz s3fs

COPY dist/s3-access-logs-0.0.1.tar.gz .
RUN pip install install s3-access-logs-0.0.1.tar.gz

COPY ./cmd/export.py export.py

CMD ["/usr/local/bin/python3", "export.py"]
