FROM python:3.13-alpine3.21

RUN apk add --no-cache ffmpeg \
    && pip install --no-cache-dir aiosendspin==10.2.0 numpy==2.3.5

COPY bridge.py /bridge.py

CMD ["python3", "-I", "/bridge.py"]
