FROM python:3.13

RUN pip install -r requirements.txt

WORKDIR /app

COPY . .

COPY ../spark_ops.py ../

CMD ['streamlit', 'run', 'app.py']