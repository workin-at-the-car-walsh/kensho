from python:3.10

WORKDIR /app

COPY . /app/
RUN ls -la

RUN python setup.py bdist_wheel
RUN pip install ./dist/*.whl