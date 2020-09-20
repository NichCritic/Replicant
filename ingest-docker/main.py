import urllib.request
import zipfile
from neo4j import GraphDatabase

def create_movie(tx, title):
    result = tx.run("CREATE (a:Movie) {title:$title} "
                    "RETURN a.title + ', from node ' + id(a)", message=title)
    return result.single()[0]

def create_cast_crew(tx, name):
    result = tx.run("CREATE (a:Person) {name:$name, role:$role}", name=name)
    return result.single()[0]

def add_cast_assoc(tx, name, title):
    result = tx.run("MATCH (p:Person) {name:$name}"
                    "MATCH (m:Movie) {title:$title}"
                    "CREATE (p)-[rel:CAST_IN]->(m)", name=name, title=title)
    return result.single()[0]

if __name__ == "__main__":
    urllib.request.urlretrieve('https://www.kaggle.com/rounakbanik/the-movies-dataset/download', 'data.zip')
    with zipfile.ZipFile("data.zip", 'r') as zip_ref:
        zip_ref.extractall("./")

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

    with driver.session() as session:
        movies_by_id = {}
        with open("./movies_metadata.csv") as movie_file:
            for line in movies_file:
                adult,belongs_to_collection,budget,genres,homepage,id, \
                imdb_id,original_language,original_title,overview,popularity,\
                poster_path,production_companies,production_countries,\
                release_date,revenue,runtime,spoken_languages,status,\
                tagline,title,video,vote_average,vote_count = line.split(",")
                movies_by_id[id] = title
                session.write_transaction(create_movie, title)


        with open("./credits.csv") as credits_file:

            for line in credits_file[1:]:
                cast, crew, id = line.split(",")







