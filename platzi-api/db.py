from bson.json_util import dumps, ObjectId
from flask import current_app
from pymongo import MongoClient, DESCENDING
from werkzeug.local import LocalProxy


# Este método se encarga de configurar la conexión con la base de datos
def get_db():
    platzi_db = current_app.config['PLATZI_DB_URI']
    client = MongoClient(platzi_db)
    return client.platzi


# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)


def test_connection():
    return dumps(db.collection_names())


def collection_stats(collection_name):
    return dumps(db.command('collstats', collection_name))

# -----------------Carreras-------------------------


def crear_carrera(json):
    return str(db.carreras.insert_one(json).inserted_id)


def consultar_carreras(skip, limit):
    return dumps(db.carreras.find({}).skip(int(skip)).limit(int(limit)).sort('_id', DESCENDING))


def consultar_carrera_por_id(carrera_id):
    return dumps(db.carreras.find_one({'_id': ObjectId(carrera_id)}))


def actualizar_carrera(carrera):
    # Esta funcion solamente actualiza nombre y descripcion de la carrera
    return str(db.carreras.update_one({'_id': ObjectId(carrera['_id'])},
                           {'$set': {'nombre': carrera['nombre'], "descripcion": carrera['descripcion']}}).modified_count)


def borrar_carrera_por_id(carrera_id):
    return str(db.carreras.delete_one({'_id': ObjectId(carrera_id)}).deleted_count)
