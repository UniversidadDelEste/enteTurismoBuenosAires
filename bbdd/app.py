#!/usr/bin/env python
import psycopg2
import csv
import os


DBNAME = 'alojamientos'
DBUSER = 'postgres'


def import_csv(filename, delimiter=';'):
    """Retorna el contenido del filename dado como una lista.

    Keywords:
        filename    --  Path del archivo csv
        delimiter   --  Separador del archivo csv
    """
    lines = []

    with open(filename, 'r') as fp:
        reader = csv.reader(fp, delimiter=delimiter)
        for row in reader:
            lines.append(row)

    return lines


def write_csv(data, filename, delimiter=';'):
    """Escribe los datos recibidos a un archivo

    Keywords:
        data        -- Iterable, ej una lista de python
        filename    -- Nombre del archivo destino
        delimiter   -- Separador del archivo csv
    """

    with open(filename, 'wb') as fp:
        writer = csv.writer(fp, delimiter=delimiter)
        writer.writerows(data)


def category_list(data):
    """Retorna una lista de tuplas(nro_registro, categoria)

    Keywords:
        data        -- Iterable
    """
    lines = []

    for line in data:
        lines.append((line[0], line[1]))

    return lines


def neighborhood_list(data):
    """Retorna una lista de tuplas(nro_registro, barrio)

    Keywords:
        data        -- Iterable
    """
    lines = []

    for line in data:
        lines.append((line[0], line[6]))

    return lines


def write_category_csv():
    """Escribe un archivo csv con la lista de categorias"""
    data = import_csv('alojamientos.csv')
    categories = []

    for category in category_list(data):
        category_name = category[1]
        if category_name != 'categoria' and [category_name] not in categories:
            categories.append([category_name])

    write_csv(data=categories, filename='categories.csv')


def write_neighborhood_csv():
    """Escribe un archivo csv con la lista de barrios"""
    data = import_csv('alojamientos.csv')
    neighborhoods = []

    for neighborhood in neighborhood_list(data):
        neighborhood_name = neighborhood[1]
        if neighborhood_name != 'barrio' and [neighborhood_name] not in neighborhoods:
            neighborhoods.append([neighborhood_name])

    write_csv(data=neighborhoods, filename='neighborhoods.csv')


def write_all_csv_files():
    """Escribe todos los ficheros csv necesarios para poder hacer el COPY
    """
    write_category_csv()
    write_neighborhood_csv()
    for file_ in ['categories.csv', 'neighborhoods.csv']:
        if not os.path.isfile(file_):
            raise Exception("No pude generar {}, abortando.".format(file_))
    print("Generado categories.csv")
    print("Generado neighborhoods.csv")
    print("Ahora puede importar los datos a PostgreSQL ingresando:")
    print("COPY barrio(nombre) from 'neighborhoods.csv'")
    print("COPY categoria(nombre) from 'categories.csv'")


def get_category_id_by_name(name):
    """Retorna el id de una categoria segun el nombre"""
    connection = connect()
    cursor = connection.cursor()
    query = "SELECT id FROM categoria where nombre = %s;"
    cursor.execute(query, [name])
    id_ = cursor.fetchone()
    cursor.close()
    connection.close()
    if len(id_) == 1:
        return id_[0]


def get_neighborhood_id_by_name(name):
    """Retorna el id de un barrio segun el nombre"""
    connection = connect()
    cursor = connection.cursor()
    query = "SELECT id FROM barrio where nombre = %s;"
    cursor.execute(query, [name])
    id_ = cursor.fetchone()
    cursor.close()
    connection.close()
    if len(id_) == 1:
        return id_[0]


def insert_places():
    """Inserta los establecimientos turisticos"""
    data = import_csv('alojamientos.csv')
    data_to_insert = []
    del data[0]  # borramos los headers

    for row in data:
        nro_registro = int(row[0])
        id_categoria = get_category_id_by_name(row[1]) or None
        establecimiento = row[2]
        n_habitaciones = int(row[3]) if row[3] else None
        n_plazas = int(row[4]) if row[4] else None
        domicilio = row[5]
        id_barrio = get_neighborhood_id_by_name(row[6]) or None
        telefono = row[7]
        mail = row[8]
        longitud = float(row[9]) if row[9] else None
        latitud = float(row[10]) if row[10] else None
        data_to_insert.append((
                nro_registro,
                id_categoria,
                establecimiento,
                n_habitaciones,
                n_plazas,
                domicilio,
                id_barrio,
                telefono,
                mail,
                longitud,
                latitud,
                ))
    connection = connect()
    cursor = connection.cursor()
    for alojamiento in data_to_insert:
        query = ' '.join((
            "INSERT INTO establecimiento",
            "(nro_registro, id_categoria, nombre, n_habitaciones, n_plazas,",
            "domicilio, id_barrio, telefono, mail, longitud, latitud)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            ))
        cursor.execute(query, alojamiento)
    connection.commit()
    cursor.close()
    connection.close()


def connect(dbname=DBNAME, dbuser=DBUSER):
    """Retorna una coneccion a PostgreSQL

    Keywords:
        dbname  --      Nombre de la base a conectarse
        dbuser    --      Usuario con el cual conectarse a la DB
    """
    conn = psycopg2.connect("dbname={} user={}".format(dbname, dbuser))
    return conn
