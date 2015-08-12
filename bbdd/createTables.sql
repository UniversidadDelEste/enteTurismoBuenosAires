CREATE TABLE categoria (
    id SERIAL PRIMARY KEY,
    nombre text UNIQUE NOT NULL);
CREATE TABLE barrio (
    id SERIAL PRIMARY KEY,
    nombre text UNIQUE NOT NULL);
CREATE TABLE establecimiento (
    nro_registro integer PRIMARY KEY,
    nombre text NOT NULL,
    n_habitaciones integer,
    n_plazas integer,
    domicilio text NOT NULL,
    telefono text,
    mail text,
    longitud float,
    latitud float,
    id_barrio integer REFERENCES barrio (id),
    id_categoria integer REFERENCES categoria (id));
