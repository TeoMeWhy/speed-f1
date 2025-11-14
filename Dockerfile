FROM python:3.13

RUN apt-get update && apt-get install -y wget gnupg git && \
    wget -O- https://apt.corretto.aws/corretto.key | gpg --dearmor | tee /usr/share/keyrings/corretto-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-archive-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list && \
    apt-get update && apt-get install -y java-21-amazon-corretto-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN pip install jupyter 

COPY *.py .
COPY ./etl ./etl

CMD ["python", "main.py"]