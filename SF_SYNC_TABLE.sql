CREATE TABLE SF_SYNC_TABLE  (
		  SCHEMA VARCHAR(128) NOT NULL , 
		  TABLE_NAME VARCHAR(128) NOT NULL , 
		  TABLE_TYPE" VARCHAR(10) NOT NULL WITH DEFAULT  , 
		  SYNC_TYPE VARCHAR(20) NOT NULL WITH DEFAULT  , 
		  INCR_SYNC_FILTER VARCHAR(80 ) NOT NULL WITH DEFAULT  )
		  
