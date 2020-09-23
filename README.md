# Replicant
Short coding test for replicant

# Setting up the data
## For full ingest (Approx 12 hours):
1. From the root folder run `docker-compose build` then `docker-compose up -d` to start the webserver and database
2. From the ingest-docker folder run `docker build -t ingest .` `docker run -it -d --network replican_default ingest` to begin the ingest process. 

 To save (some) time, you can download the movie dataset and put it in the ingest folder as `the-movies-dataset.zip`. It will be copied into the docker image. Otherwise the script will download it

## Restore the database from neo4j.dump (Few minutes)
 This procedure is more involved, but saves a considerable amount of time over injesting the data from scratch

1. From the root folder run `docker-compose up -d --build` to start the webserver and database
2. Wait for them to initialize, then shut them down
3. Copy the neo4j.dump file from the root to `neo4j/data/`
3. Run `docker-compose -f docker-compose-restore.yml up -d --build` to start a container for only the database, without having the database start
4. Run `docker ps` and identify the id of the database container
5. Run `docker exec -it <container id> bin/neo4j-admin load --from data/neo4j.dump --force`
6. Shut down the restore container
7. Run `docker-compose up -d` to restart the server

# Using the api
1. Go to `http://127.0.0.1/bacon_number?name=Tom Hanks`
2. Replace the name in the url bar with other actor names


