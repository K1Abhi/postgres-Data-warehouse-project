-- ============================================================================
-- Script: Create Database and Schemas in PostgreSQL
-- ============================================================================
-- Script Purpose:
--     This script drops the 'DataWarehouse' database if it exists, recreates it,
--     and then sets up the 'bronze', 'silver', and 'gold' schemas.
--
-- WARNING:
--     Running this script will permanently delete the existing 'DataWarehouse'
--     database and all its data. Backup your data before proceeding.
-- ============================================================================

-- Connect to default database to drop the target database
-- This part must be run in a database outside 'DataWarehouse' (e.g., 'postgres')

-- Step 1: Terminate connections and drop the database (run from 'postgres')
DO
$$
BEGIN
   IF EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse') THEN
      -- Terminate all existing connections
      PERFORM pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = 'datawarehouse';

      -- Drop the database
      DROP DATABASE datawarehouse;
   END IF;
END
$$;

-- Step 2: Create the database
CREATE DATABASE datawarehouse;

-- Step 3: Connect to the new database and create schemas
-- NOTE: The following must be executed in the 'datawarehouse' database context

-- You must now switch to the `datawarehouse` DB before running the below:

-- ─── Run This After Switching ───

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
