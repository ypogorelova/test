FROM python:3.8-slim-buster

WORKDIR /app
COPY . ./backend

RUN pip install -r ./backend/requirements.txt

CMD [ "python3", "-m",  "backend.wiki"]