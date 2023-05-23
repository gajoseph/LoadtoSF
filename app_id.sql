-------------------------------------------------------------------------------------------------
-- Name: create_user_app.sql 
--
-- Description: Creates a user id for an application. 
--
-- This scripts:
-- 1) creates the app user id
-- 2) creates a role which is used by the app user.  
-- 
-- STEPS:
-- 1) Fill in values in the "Parameters/Variables to be Set" section
-- 2) Run the script
--
-- Version----Name------------------Date-----------Description-----------------------------------
-- 1.0        Steve Huber           2019-05-20     Created  
-- 1.1        Steve Huber           2019-07-10     Added Create role
-- 1.2        Steve Huber           2019-07-30     Added resets of user defaults
-- 1.3        Steve Huber           2020-05-27     Added V_ACCESS_ROLE
-------------------------------------------------------------------------------------------------

-----------------------------------------------------------
-- Parameters/Variables to be Set
-----------------------------------------------------------
Set V_ENV_NAME    = 'DEV';      -- DEV, STG, or PRD 
Set V_APP_ABBR    = 'MYAPP';   -- the app name/abbriation
Set V_APP_PWD     = 'SomePassword'; -- the password to be used by the application 

-- Has access to 
Set V_WH_NAME     = 'DNA_WH';              -- which warehouse will the app be using
Set V_ACCESS_ROLE = 'dev_copy_qtg_rw';     -- to schema.  Examples; dev_copy_qtg_ro, dev_copy_qtg_rw, dev_copy_qtg_admin
                                           -- to database.  Examples; dev_copy_ro, dev_copy_admin
                                           -- to enviroment.  Examples; dev_ro

-----------------------------------------------------------
-- Calculate Object and Role names
-----------------------------------------------------------
-- Calculate prefixes
Set V_ENV_APP_ID    = $V_ENV_NAME || '_' || $V_APP_ABBR || '_APP_ID';
Set V_ENV_APP_ROLE  = $V_ENV_NAME || '_' || $V_APP_ABBR || '_APP_ROLE';


--------------------------------------------
-- Create Role
--------------------------------------------
Use Role securityadmin;

CREATE ROLE IF NOT EXISTS Identifier($V_ENV_APP_ROLE);
GRANT ROLE Identifier($V_ACCESS_ROLE)           TO ROLE Identifier($V_ENV_APP_ROLE);
GRANT USAGE ON WAREHOUSE Identifier($V_WH_NAME) TO ROLE Identifier($V_ENV_APP_ROLE);


--------------------------------------------
-- Create User
--------------------------------------------
Use Role securityadmin;

CREATE USER IF NOT EXISTS Identifier($V_ENV_APP_ID) 
    PASSWORD=$V_APP_PWD
    LOGIN_NAME = $V_ENV_APP_ID
    DISPLAY_NAME = $V_ENV_APP_ID
    DEFAULT_ROLE = $V_ENV_APP_ROLE
    DEFAULT_WAREHOUSE = $V_WH_NAME
    MUST_CHANGE_PASSWORD = False;
	
ALTER USER Identifier($V_ENV_APP_ID) SET DEFAULT_ROLE = $V_ENV_APP_ROLE;
ALTER USER Identifier($V_ENV_APP_ID) SET DEFAULT_WAREHOUSE = $V_WH_NAME;

-- Grants
Grant Role Identifier($V_ENV_APP_ROLE) To User Identifier($V_ENV_APP_ID);

