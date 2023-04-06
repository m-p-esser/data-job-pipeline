FROM prefecthq/prefect:2.7.6-python3.9
COPY requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt
COPY . /tmp/