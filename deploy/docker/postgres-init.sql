-- Each service owns a separate logical database. This keeps schemas and
-- migrations independent while still using one local PostgreSQL instance.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE notification_db;
