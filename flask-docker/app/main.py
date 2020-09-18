from flask import Flask
from neo4j import GraphDatabase

uri = "neo4j://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))
app = Flask(__name__)

def bacon_number(tx):
	query = (
		'MATCH p=shortestPath('
			'(bacon:Person {name:"Kevin Bacon"})-[*]-(meg:Person {name:"Meg Ryan"})'
		')'
		'RETURN p'
	)
	result = tx.run(query)
	return result

@app.route('/')
def hello_world():
	with driver.session() as session:
		result = session.read_transaction(bacon_number)
		return result 