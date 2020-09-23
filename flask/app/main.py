from flask import Flask, jsonify, request, g
from neo4j import GraphDatabase
import time
import urllib.parse


# ----------Setup-------------------------------------
app = Flask(__name__)


def connect_to_database():
    if not hasattr(g, 'neo4j_driver'):
        tries = 0
        while True:
            try:
                uri = "neo4j://neo4j:7687"
                g.neo4j_driver = GraphDatabase.driver(
                    uri, auth=("neo4j", "test"))
                return g.neo4j_driver
            except:
                # Bare except is bad here, but it's unclear what errors Neo4j
                # will throw and for what reasons. Need to track those down before
                # releasing this as a finished product
                print("Failed to connect, retry in 5s")
                time.sleep(5)
                tries = tries + 1
                if tries > 50:
                    return None
    else:
        return g.neo4j_driver


@app.teardown_appcontext
def close_db(*args):
    if hasattr(g, 'neo4j_driver'):
        g.neo4j_driver.close()


with app.app_context():
    connect_to_database()

# -----------Routes------------------------


@app.route('/bacon_number')
def bacon_number():
    name = request.args.get("name")
    name = urllib.parse.unquote(name)
    if name == "Kevin Bacon":
        # Neo4j doesn't like it when the path has 0 nodes
        ret = {
            "name": name,
            "bacon_number": 0,
            "path": []
        }
        return ret
    driver = connect_to_database()
    with driver.session() as session:
        match_result = session.run("MATCH (n:Person {name: $name }) "
                                   "RETURN n", name=name)
        person = match_result.single()
        if person is None or person == []:
            ret = {"name": name, "bacon_number": "Not available", "path": []}
            return jsonify(ret)

        result = session.run(
            'MATCH p=shortestPath((bacon:Person {name:"Kevin Bacon"})-[*]-(meg:Person {name:$name})) '
            'RETURN p, length(p) as bacon_number', name=name)
        result_data = result.data()
        ret = {
            "name": name,
            "bacon_number": int(result_data[0]["bacon_number"] / 2),
            "path": result_data[0]["p"]
        }
        return jsonify(ret)
