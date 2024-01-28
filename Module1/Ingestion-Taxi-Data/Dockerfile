FROM python:3.9

#RUN pip list -> this was to know version of packkage wheel
RUN pip install pandas sqlalchemy psycopg2 fsspec

# declare working directory
WORKDIR /

#copy all files into current directory
COPY . ./

#container will run in python mode and immediatly run pipeline.py
ENTRYPOINT [ "python", "ingest_data_class_refactor.py" ]