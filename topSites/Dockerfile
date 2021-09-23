FROM python:3.9-slim-buster

WORKDIR /app

RUN useradd -m python

COPY --chown=python:python requirements.txt database.ini ./

RUN pip3 install -r requirements.txt

COPY --chown=python:python src topSites

USER python

CMD [ "python3.9", "-m" , "topSites"]
