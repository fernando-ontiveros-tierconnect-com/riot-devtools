CREATE TABLE thingsdemo1.things1 (
  id                 uuid,
  serial             varchar,
  name               varchar,
  parent_id          varchar,
  thingType_id       varchar,
  createdByUser_id   varchar,
  createdTime        timestamp,
  group_id           varchar,
  groupTypeFloor_id  varchar,
  active             boolean,
  isAlive            boolean,
  fields             map<varchar, varchar>,

  PRIMARY KEY (id, serial)
);

CREATE INDEX serial_idx ON things1 ( serial );


CREATE TYPE gps_type (
   latitude  float,
   longitude float,
   altitude  float
);

CREATE TABLE things1_history (
  thing_id           uuid,
  fieldname          varchar,
  at                 timestamp,
  type               varchar,
  value_int          int,
  value_float        float,
  value_text         varchar,
  value_gps          frozen<gps_type>,
  PRIMARY KEY (thing_id, fieldname, at)
);



una tabla history por cada thingtype
opcion 1, todos los values en la tabla, existiendo un campo por cada tipo de value
opcion 2, un type con todos los tipos, y un solo campo llamado value, con el valor de int

una tabla history por cada tipo de dato, y en ella estarian todos los thingtypes
opcion 3, la tabla tendria el campo value con el tipo especifico.

una unica tabla history para todos los thingtypes
opcion 1a, y 2a

guardar solo como texto
opcion 4 una sola tabla
opcion 5 una tabla por cada thingtype


