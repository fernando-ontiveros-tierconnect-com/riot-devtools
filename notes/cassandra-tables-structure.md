# keyspaces

CREATE KEYSPACE thingsdemo1 WITH REPLICATION =
{ 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };

CREATE KEYSPACE demodb WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 2};


CREATE KEYSPACE "thingsdemo1" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};




#basic tables for cassandra
CREATE TABLE users (
  user_name varchar,
  password varchar,
  gender varchar,
  session_token varchar,
  state varchar,
  birth_year bigint,
  PRIMARY KEY (user_name));



CREATE TYPE thing_template_fields (
   name varchar,
   type   varchar,
   unit   varchar,
   symbol  varchar
);

CREATE TABLE thing_template (
  id                 uuid,
  name               varchar,
  tablename          varchar,
  createdTime        timestamp,
  active             boolean,
  fields             set< frozen<thing_template_fields>>,
  PRIMARY KEY (id)
);
