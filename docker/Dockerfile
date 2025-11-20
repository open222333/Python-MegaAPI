FROM python:3.11-slim

WORKDIR /app

# 複製 requirements.txt
COPY ../requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r environment.txt

# 複製專案所有檔案
COPY .. .

CMD ["python", "main.py"]
