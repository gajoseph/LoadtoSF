-------------------------------------------------------------------------------------------------
-- Name: create_qdna_database.sql
--
-- Description: To create new QNDA databases and the associated roles in Snowflake computing
--
-- Version----Name------------------Date-----------Description-----------------------------------
-- 
-------------------------------------------------------------------------------------------------
Set V_SCRIPT_NAME = 'create_qdna_database';
Set V_SCRIPT_VER  = '1.9';

-----------------------------------------------------------
-- Parameters/Variables to be Set
-----------------------------------------------------------
Set V_WH_NAME    = 'DNA_WH';
Set V_DB_SECURE  =  TRUE;   -- TRUE / FALSE : Does the schema contain sensitive data
Set V_ENV_NAME   = 'DEV';   -- DEV, TST, or PRD 
Set V_DB_NAME    = 'COPY';
Set V_DB_COMMENT = 'mirror copy of source system schemas.';

--   insert results from  helper_create_qdna_database.sql below, otherwise delete lines below

-----------------------------------------------------------
-- Global Roles
-----------------------------------------------------------
Set V_MONITOR_ROLE = 'SF_MONITOR';


-----------------------------------------------------------
-- Calculate Object and Role names
-----------------------------------------------------------
-- Calculate prefixes
Set V_ENV_DB = $V_ENV_NAME || '_' || $V_DB_NAME;

-- Calculate Environment Role Names 
Set V_ENV_RO_ROLE      = $V_ENV_NAME || '_RO';
Set V_ENV_RO_SEC_ROLE  = $V_ENV_NAME || '_RO_SECURE';
Set V_ENV_RW_ROLE      = $V_ENV_NAME || '_RW';
Set V_ENV_ADMIN_ROLE   = $V_ENV_NAME || '_DB_ADMIN';

-- Calculate DB Role Names --
Set V_ENV_DB_RO_ROLE     = $V_ENV_DB || '_RO';  -- If secure, don't create the Read Only role
Set V_ENV_DB_RO_SEC_ROLE = $V_ENV_DB || '_RO_SECURE';
Set V_ENV_DB_RW_ROLE     = $V_ENV_DB || '_RW';
Set V_ENV_DB_ADMIN_ROLE  = $V_ENV_DB || '_ADMIN';

-- Public Schema ROLE
Set V_ENV_DB_PUBLIC_ADMIN_ROLE  = $V_ENV_DB || '_PUBLIC_ADMIN';

-- Schema comments 
Set V_ENV_DB_RO_ROLE_COMMENT     = 'Role that has Read Only access on all objects in all non-secure schemas within the database';
Set V_ENV_DB_RO_SEC_ROLE_COMMENT = 'Role that has Read Only access on all objects in all schemas within the database';
Set V_ENV_DB_RW_ROLE_COMMENT     = 'Role that has select/insert/update/delete access on all objects in all schemas within the database';
Set V_ENV_DB_ADMIN_ROLE_COMMENT  = 'Role that has full access on all objects in all schemas within the database';


-----------------------------------------------------------
---- Use SECURITYADMIN to create database roles
-----------------------------------------------------------
Use Warehouse Identifier($V_WH_NAME);
Use Role securityadmin;

-- Create DB level roles
Create Role If Not Exists Identifier($V_ENV_DB_RO_ROLE);
Create Role If Not Exists Identifier($V_ENV_DB_RO_SEC_ROLE);
Create Role If Not Exists Identifier($V_ENV_DB_RW_ROLE);
Create Role If Not Exists Identifier($V_ENV_DB_ADMIN_ROLE);

-- Add/Update DB Role comments
Alter Role Identifier($V_ENV_DB_RO_ROLE)      Set Comment = $V_ENV_DB_RO_ROLE_COMMENT;
Alter Role Identifier($V_ENV_DB_RO_SEC_ROLE)  Set Comment = $V_ENV_DB_RO_SEC_ROLE_COMMENT;
Alter Role Identifier($V_ENV_DB_RW_ROLE)      Set Comment = $V_ENV_DB_RW_ROLE_COMMENT;
Alter Role Identifier($V_ENV_DB_ADMIN_ROLE)   Set Comment = $V_ENV_DB_ADMIN_ROLE_COMMENT;

-- Grant DB level roles to Environment level roles
Grant Role Identifier($V_ENV_DB_RO_ROLE)      To Role Identifier($V_ENV_RO_ROLE);
Grant Role Identifier($V_ENV_DB_RO_SEC_ROLE)  To Role Identifier($V_ENV_RO_SEC_ROLE);
Grant Role Identifier($V_ENV_DB_RW_ROLE)      To Role Identifier($V_ENV_RW_ROLE);
Grant Role Identifier($V_ENV_DB_ADMIN_ROLE)   To Role Identifier($V_ENV_ADMIN_ROLE);

-- -- Grant DB Read Only (RO) ==> DB Read Write (RW) ==> DB Admin (ADMIN)    
-- Grant Role Identifier(iff($V_DB_SECURE, $V_ENV_DB_RO_SEC_ROLE, $V_ENV_DB_RO_ROLE))   To Role Identifier((iff($V_DB_SECURE, $V_ENV_DB_RW_ROLE, $V_ENV_DB_RO_SEC_ROLE)));
-- Grant Role Identifier($V_ENV_DB_RO_SEC_ROLE)  To Role Identifier($V_ENV_DB_RW_ROLE);
-- Grant Role Identifier($V_ENV_DB_RW_ROLE)      To Role Identifier($V_ENV_DB_ADMIN_ROLE);

Create Role If Not Exists Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE) Comment = 'Full control of PUBLIC schema';


-----------------------------------------------------------
-- Create the database using the SYSADMIN role
-----------------------------------------------------------
Use Warehouse Identifier($V_WH_NAME);
Use Role SYSADMIN;
Create Database If Not Exists Identifier($V_ENV_DB);
Alter Database Identifier($V_ENV_DB) Set Comment = $V_DB_COMMENT;


-----------------------------------------------------------
---- Grant USAGE on the database
-----------------------------------------------------------
Use Role securityadmin;

-- Grant to database 
Grant Usage On Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_RO_ROLE);
Grant Usage On Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_RO_SEC_ROLE);
Grant Usage On Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_RW_ROLE);
Grant Usage On Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_ADMIN_ROLE);


-----------------------------------------------------------
-- Change Ownership of DB and PUBLIC schema 
-----------------------------------------------------------
Use Warehouse Identifier($V_WH_NAME);
Use Role securityadmin;
Grant Ownership On Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_ADMIN_ROLE) Copy CURRENT GRANTS;


-----------------------------------------------------------
-- Set database object grants
-----------------------------------------------------------
Use Role Identifier($V_ENV_DB_ADMIN_ROLE);
Use database Identifier($V_ENV_DB);

-- Grant database access to GLOBAL roles
Grant Monitor On database Identifier($V_ENV_DB) To Role Identifier($V_MONITOR_ROLE);

-- Grant database access to DB level roles 
-- NOTE: RW ROLE will inherit DATABASE USAGE from RO role
Grant Usage On database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RO_ROLE);
Grant Usage On database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RO_SEC_ROLE);
Grant Usage On database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RW_ROLE);
Grant All   On database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_ADMIN_ROLE);

-- Schema grants 
Grant Usage On All schemas in Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RO_ROLE);
Grant Usage On All schemas in Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RO_SEC_ROLE);
Grant Usage On All schemas in Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_RW_ROLE);
Grant All   On All schemas in Database Identifier($V_ENV_DB) To Role Identifier($V_ENV_DB_ADMIN_ROLE);


-----------------------------------------------------------
-- PUBLIC Schema 
-----------------------------------------------------------
Use Role securityadmin;
Grant Usage On database Identifier($V_ENV_DB) To Role securityadmin;
Grant Usage On schema public To Role securityadmin;
Use database Identifier($V_ENV_DB);


Grant Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE) To Role Identifier($V_ENV_DB_ADMIN_ROLE);
Grant Usage On Database Identifier($V_ENV_DB)      To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);

Grant Ownership On Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE) Copy CURRENT GRANTS;

-- Grant Privildeges to PUBLIC schema
Use schema public;
Grant All On All Tables       In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On All Views        In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On All Stages       In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On All Sequences    In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On All Functions    In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On Future Tables    In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On Future Views     In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On Future Stages    In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On Future Sequences In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Grant All On Future Functions In Schema PUBLIC To Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);

-- Revoke access to Public schema from PUBLIC
Revoke All On All Tables       In Schema PUBLIC From Role PUBLIC;
Revoke All On All Views        In Schema PUBLIC From Role PUBLIC;
Revoke All On All Stages       In Schema PUBLIC From Role PUBLIC;
Revoke All On All Sequences    In Schema PUBLIC From Role PUBLIC;
Revoke All On All Functions    In Schema PUBLIC From Role PUBLIC;
Revoke All On Future Tables    In Schema PUBLIC From Role PUBLIC;
Revoke All On Future Views     In Schema PUBLIC From Role PUBLIC;
Revoke All On Future Stages    In Schema PUBLIC From Role PUBLIC;
Revoke All On Future Sequences In Schema PUBLIC From Role PUBLIC;
Revoke All On Future Functions In Schema PUBLIC From Role PUBLIC;

-- Revoke PUBLIC ACCESS
Revoke Usage On Schema PUBLIC From Role PUBLIC;
Revoke Usage On database Identifier($V_ENV_DB) From Role PUBLIC;

-- Grant PUBLIC ACCESS to DB level roles
Grant Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE)  To Role Identifier($V_ENV_DB_RO_ROLE);
Grant Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE)  To Role Identifier($V_ENV_DB_RO_SEC_ROLE);
Grant Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE)  To Role Identifier($V_ENV_DB_RW_ROLE);
Grant Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE)  To Role Identifier($V_ENV_DB_ADMIN_ROLE);

-----------------------------------------------------------
-- Create PUBLIC file formats
-----------------------------------------------------------
Use Role Identifier($V_ENV_DB_ADMIN_ROLE);
Use database Identifier($V_ENV_DB);
Use Schema Public;

DROP File Format IF EXISTS FORMAT_DB2_LINUX_GZIP;
DROP File Format IF EXISTS FORMAT_DB2_LINUX;
DROP File Format IF EXISTS format_postgres_linux;

Use Role Identifier($V_ENV_DB_PUBLIC_ADMIN_ROLE);
Use database Identifier($V_ENV_DB);
Use Schema Public;

Create Or Replace File Format FORMAT_DB2_LINUX_GZIP
  type = 'CSV'
  COMPRESSION=gzip
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  DATE_FORMAT = 'YYYYMMDD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD-HH24.MI.SS.FF'
  TIME_FORMAT = 'HH24.MI.SS'
  ENCODING = 'iso-8859-1'
  TRIM_SPACE=true;

Create Or Replace File Format FORMAT_DB2_LINUX
  type = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  DATE_FORMAT = 'YYYYMMDD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD-HH24.MI.SS.FF'
  TIME_FORMAT = 'HH24.MI.SS'
  ENCODING = 'iso-8859-1'
  TRIM_SPACE=true;

Create Or Replace File Format format_postgres_linux
  type = 'CSV'
  field_delimiter = '|'
  skip_header = 1  
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
  TRIM_SPACE=true;


-----------------------------------------------------------
-- PUBLIC Schema 
-----------------------------------------------------------
Use Role securityadmin;
REVOKE Usage On schema public FROM Role securityadmin;
REVOKE Usage On database Identifier($V_ENV_DB) FROM Role securityadmin;


-----------------------------------------------------------
-- Save execution informtion
-----------------------------------------------------------
USE ROLE SYSADMIN;
USE DATABASE asd_DBA;
USE SCHEMA SNOWFLAKE_INFO;

INSERT INTO script_execution_history  (ENVIRONMENT_NAME, DATABASE_NAME, SCHEMA_NAME, secure_ind, SCRIPT_NAME, SCRIPT_VERSION, EXECUTED_BY, EXECUTED_TIMESTAMP)
VALUES ($V_ENV_NAME, $V_ENV_DB, NULL, iff($V_DB_SECURE, 1, 0), $V_SCRIPT_NAME, $V_SCRIPT_VER, CURRENT_USER, current_timestamp);
