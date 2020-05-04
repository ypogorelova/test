FROM python:3.8-slim-buster
WORKDIR /frontend

COPY . ./frontend
RUN pip install -r ./frontend/requirements.txt

ARG EXPOSE_PORT
EXPOSE $EXPOSE_PORT

CMD [ "python3", "-m", "frontend.app" ]
