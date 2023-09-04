
from s3sf.s3tosf import  sfloader
import logging

class sfloaderRabbitMqmessages(sfloader):
    """
    Inherits Sfloader that loads data from csv to SF table
    This extneded class is for rabbit<Q messages that are JSON format


    """

    pass

    def __init__(self, psecretid):
        sfloader.__init__(self, psecretid)


    def sf_fileformat(self):

        file_format_sql = """
            CREATE OR REPLACE FILE FORMAT sfloaderRabbitMqmessage_ff type= 'CSV'
                    FIELD_DELIMITER = '|'
                    SKIP_HEADER = 1
                    DATE_FORMAT = 'YYYY-MM-DD'
                    TIME_FORMAT = 'HH24.MI.SS'
                    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
                    TRIM_SPACE = TRUE
                    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                    NULL_IF = ('\\N', 'NULL', 'NUL', '', 'null')
                    EMPTY_FIELD_AS_NULL = FALSE
                    ENCODING = 'UTF-8';
        """
        return file_format_sql

    def sf_stage(self):
        sf_stg_sql =  """
                CREATE OR REPLACE STAGE sfloaderRabbitMqmessage_stage
                  storage_integration = s3_da_etl
                  url = 's3://da-etl/'
                  file_format = sfloaderRabbitMqmessage_ff;
                """

        return sf_stg_sql


    def load_s3file_sftable(self, s3filepath, tablename, columns, first_file_name):
        """:parameter
            s3filepath : path of the data file
            tablename: table to be loaded
        """

        copy_sql = """
                COPY into 
                /* table name */ 
                {}
                /* column list*/
                ({}) 
                from (
                select $1::VARIANT, $2::VARIANT, current_timestamp()
                    from @sfloaderRabbitMqmessage_stage/{} (pattern=>'.*.csv')
                    /*where metadata$filename >= '{}' */
                    /*kafka/da-snowflake-stg/2022_03_06/tab_snow_pipe_2022_03_06_22_53_04.076550.csv*/
                )    
            """
        try:
            if not self.db_connection is None:
                copy_sql = copy_sql.format(tablename,columns, s3filepath, first_file_name)
                logging.info(copy_sql)
                self.db_connection.cursor().execute(copy_sql)

                #print("loading {} \n  {}".format(tablename, copy_sql.strip()))

                self.db_connection.cursor().execute("commit")
            else:
                logging.error("No Db connection ")


        except Exception as e:
            logging.error(e)

            self.db_connection.rollback()
            raise e
            logging.error(e)

