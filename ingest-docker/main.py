import urllib.request
import zipfile
import time
import subprocess
import csv
import ast
import itertools
from os import path
from neo4j import GraphDatabase


def create_movie(tx, id, title, release_date):
    result = tx.run("CREATE (a:Movie {id:$id, title:$title, release_date:$date}) "
                    "RETURN a.title + ', from node ' + id(a)", title=title, date=release_date, id=id)
    return result.single()[0]


def create_cast_crew(tx, id, name):
    result = tx.run("CREATE (a:Person {id:$id, name:$name}) "
                    "RETURN a.name", id=id, name=name)
    return result.single()[0]


def add_cast_assoc(tx, person_id, movie_id):
    result = tx.run("MATCH (p:Person {id:$person_id}) "
                    "MATCH (m:Movie {id:$movie_id}) "
                    "CREATE (p)-[rel:CAST_IN]->(m) "
                    "RETURN p.id + ' joined to ' + m.id", movie_id=movie_id, person_id=person_id)
    ret = result.single()
    if ret == None:
        print(f"Problem joining {name} with {title}")
        return None
    return ret[0]


def create_movies(session, tx, reader, batch_size, cache):
    temp_line = []
    for i, line in enumerate(reader):
        if len(line) < 24 and temp_line == []:
            # If the line is too short we presume it's broken by a
            # newline in the string, and the rest of the line is on the next
            # line. So, store
            temp_line = line
            continue
        elif len(temp_line) > 0:
            # If there's already a temp_line, then append the first string in
            # temp_line to the last string in line
            line[-1] = line[-1] + temp_line[0]
            # Then append the two lists together
            line = line + temp_line[1:]

            if len(line) < 24:
                # If we're still too short (2+ newlines) then do it again
                temp_line = line
                continue
            else:
                temp_line = []
        adult, belongs_to_collection, budget, genres, homepage, id, \
            imdb_id, original_language, original_title, overview, popularity,\
            poster_path, production_companies, production_countries,\
            release_date, revenue, runtime, spoken_languages, status,\
            tagline, title, video, vote_average, vote_count = line
        movies_by_id[id] = title
        if not title in cache:
            create_movie(tx, id, title, release_date)
            cache.add(title)
        if (i + 1) % batch_size == 0:
            break


def create_all_cast(session, tx, reader, batch_size, cache):
    for i, line in enumerate(reader):
        cast, crew, id = line
        cast_l = ast.literal_eval(cast)
        # crew_l = ast.literal_eval(crew)

        movie_title = str(id)
        if id in movies_by_id:
            movie_title = movies_by_id[id]
        else:
            print(f"No title found for id {id}")

        for person in cast_l:
            if person["id"] not in cache:
                create_cast_crew(tx, person["id"], person["name"])
                cache.add(person["id"])
            add_cast_assoc(tx, person["id"], id)

        if (i + 1) % batch_size == 0:
            break


def iterator_is_empty(it):
    try:
        first = next(it)
    except StopIteration:
        return None
    else:
        return itertools.chain([first], it)

def ingest(filename, batch_size, ingest_fn, cache):
    with open(filename) as file:
        reader = csv.reader(file)
        next(reader)
        count = 0
        while (reader:= iterator_is_empty(reader)) != None:
            with driver.session() as session:
                with session.begin_transaction() as tx:
                    t1 = time.time()
                    ingest_fn(session, tx, reader, batch_size, cache)
                    t2 = time.time()
                    tx.commit()
                    count += batch_size
                    if count >= 50000:
                        break
                    print(f"Ingested {batch_size} items in {t2-t1}s. Total items: {count}")


if __name__ == "__main__":
    print("Checking to see if we have the data")
    if not path.exists("the-movies-dataset.zip"):
        print("Downloading Kaggle dataset...")
        process = subprocess.Popen(
            "kaggle datasets download -d rounakbanik/the-movies-dataset", shell=True, stdout=subprocess.PIPE)
        process.wait()
        print("Done.")
    print("Extracting...")
    with zipfile.ZipFile("the-movies-dataset.zip", 'r') as zip_ref:
        zip_ref.extractall("./")
    print("Done.")
    print("Connecting to database...")
    while True:
        try:
            uri = "neo4j://neo4j:7687"
            driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))
            break
        except:
            print("Failed to connect, retry in 5s")
            time.sleep(5)
            tries = tries + 1
            if tries > 10:
                break
    print("Done.")
    print("Ingesting movies...")
    movies_by_id = {}
    movies_cache = set()
    ingest("./movies_metadata.csv", 5000, create_movies, movies_cache)
    del movies_cache
    print("Done.")
    print("Ingesting actors...")
    actor_names = set()
    ingest("./credits.csv", 100, create_all_cast, actor_names)
    del actor_names   
    print("Done.")
