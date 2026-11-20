FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[glue,pyarrow]==0.8.1"
COPY count.py /count.py
ENTRYPOINT ["python", "-u", "/count.py"]
