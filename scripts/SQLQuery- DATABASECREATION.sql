-- DATABASE CREATION
USE master;

-- creating the new database
CREATE DATABASE DataWarehouse;
-- using the new database
USE DataWarehouse;

-- CREATING SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;