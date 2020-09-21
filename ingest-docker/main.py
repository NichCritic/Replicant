import urllib.request
import zipfile
from neo4j import GraphDatabase
import subprocess
import csv
import ast
import itertools
from os import path


def create_movie(tx, title):
    result = tx.run("MERGE (a:Movie {title:$title}) "
                    "RETURN a.title + ', from node ' + id(a)", title=title)
    return result.single()[0]


def create_cast_crew(tx, name):
    result = tx.run("MERGE (a:Person {name:$name}) "
                    "RETURN a.name", name=name)
    return result.single()[0]


def add_cast_assoc(tx, name, title):
    result = tx.run("MATCH (p:Person {name:$name}) "
                    "MATCH (m:Movie {title:$title}) "
                    "MERGE (p)-[rel:CAST_IN]->(m) "
                    "RETURN p.name + ' joined to ' + m.title", name=name, title=title)
    ret = result.single()
    if ret == None:
        print(f"Problem joining {name} with {title}")
        return None
    return ret[0]


def create_movies(session, tx, reader, batch_size):
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
        create_movie(tx, title)
        if (i + 1) % batch_size == 0:
            break


def create_all_cast(session, tx, reader, batch_size):
    for i, line in enumerate(reader):
        cast, crew, id = line
        cast_l = ast.literal_eval(cast)
        # crew_l = ast.literal_eval(crew)

        for person in cast_l:
            create_cast_crew(tx, person["name"])
            add_cast_assoc(tx, person["name"], movies_by_id[id])

        if (i + 1) % batch_size == 0:
            break


def iterator_is_empty(it):
    try:
        first = next(it)
    except StopIteration:
        return None
    else:
        return itertools.chain([first], it)


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
    with driver.session() as session:
        movies_by_id = {}
        with open("./movies_metadata.csv") as movie_file:
            reader = csv.reader(movie_file)
            next(reader)
            count = 0
            batch_size = 5000
            while (reader:= iterator_is_empty(reader)) != None:
                with session.begin_transaction() as tx:
                    create_movies(session, tx, reader, batch_size)
                    tx.commit()
                    count += batch_size
                    if count >= 50000:
                        break
                    print(f"Ingested {count} movies")

        print("Done.")
        print("Ingesting actors...")
        with open("./credits.csv") as credits_file:
            reader = csv.reader(credits_file)
            next(reader)
            count = 0
            batch_size = 100
            while (reader:= iterator_is_empty(reader)) != None:
                with session.begin_transaction() as tx:
                    create_all_cast(session, tx, reader, batch_size)
                    tx.commit()
                    count += batch_size
                    print(f"Ingested actors from {count} movies")
                    if count >= 50000:
                        break
        print("Done.")
