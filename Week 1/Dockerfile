
FROM python:3.9

# Set the working directory in the contianer
WORKDIR /app

# Install and update wget
RUN apt-get install wget

# Copy the requirements.txt ffile into the container
COPY requirements.txt .

# Install the python dependency
RUN pip install -r requirements.txt 
#pandas sqlalchemy psycopg2 pyarrow

# Copy the python script to execute
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]