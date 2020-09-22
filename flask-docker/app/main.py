from flask import Flask, jsonify, request
from neo4j import GraphDatabase
import time

tries = 0
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

app = Flask(__name__)


def bacon_number(tx):
    query = ''
    result = tx.run(query)
    return result


@app.route('/bacon')
def bacon():
    name = request.args.get("name")
    with driver.session() as session:
        match_result = session.run("MATCH (n:Person {name:$name}) "
                            "RETURN n")
        person = match_result.single()
        if person is None:
            abort(404)

        result = session.run(
            'MATCH p=shortestPath((bacon:Person {name:"Kevin Bacon"})-[*]-(meg:Person {name:"$name"})) RETURN p', name = name)
        return jsonify(result.data())
