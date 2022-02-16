# Bym test 
Develop service for searching articles from Wikipedia based on input string. Use Kafka and PostgreSQL 

## Built and Run With Docker
DOCKER_HOST_IP="local ip" EXPOSE_PORT=9999 docker-compose up --build

### How to use:

Application is running on:
http://"docker ip":9999/urls

To retrieve results from wiki user should send POST request with "chars" key and value that he wants to search. 


