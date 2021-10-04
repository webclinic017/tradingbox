CREATE USER tradingbox WITH PASSWORD 'Tbox1234!';
CREATE DATABASE datastore;
GRANT ALL PRIVILEGES ON DATABASE datastore TO tradingbox;

CREATE DATABASE datastore_worker;
GRANT ALL PRIVILEGES ON DATABASE datastore_worker TO tradingbox;