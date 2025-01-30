FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY insert_data.py insert_data.py

ENTRYPOINT [ "python", "insert_data.py" ]



