import logging
import snowflake.connector


import s3sf.awsSecret as awssec
class sfloader(awssec.awsSecret):
    """:parameter
        Class sfloader inherits from awssec.awsSecret

    """
    db_connection = None
    # copy into public.actiontakentypelu   from @S3_DA_ETL_GLUE/stgd566a.unigroupinc.com/qtgdb/qtg.actiontakentypelu/ALL/part-00000-1eae7743-7db7-4393-91f3-448052869b2a-c000.csv
    copy_sql= """
        COPY into 
        /* table name */ 
        {}
        /* column list*/
        {} 
        from @S3_DA_ETL_GLUE/{}/{} pattern='.*.csv'   /*@S3_DA_ETL_GLUE/prd/TSDATA/2021_03_12/shared/booking_agent_dim */
    """


    def __init__(self, psecretid):
        """
            initialize (●__●)
        """
        awssec.awsSecret.__init__(self, psecretid) # calling parent init functon to get aws password
       
        self.init_db_conn()

    def init_db_conn(self):
        try:
            self.db_connection = snowflake.connector.connect(
                account=self.secrets['sf_account'],
                user=self.secrets['sf_username'],
                # private_key=self.pkb,
                password=self.secrets['sf_password'],
                database=self.secrets['sf_db'],
                schema=self.secrets['sf_schema'],
                warehouse=self.secrets['sf_warehouse'],
                role=self.secrets['sf_role'],
            )
            ff =self.sf_fileformat()

            self.db_connection.cursor().execute("{} ;".format(ff))
            self.db_connection.cursor().execute(self.sf_stage())

            self.db_connection.cursor().execute("commit")
        except Exception as e:
            logging.error(e)
            logging.error("No Db connection made ")
            if not self.db_connection is None:
                self.db_connection.rollback
            raise e

    def sf_fileformat(self):
        file_format = """
       
                CREATE OR REPLACE FILE FORMAT pglogformat type= 'CSV'
                        FIELD_DELIMITER = ','
                        SKIP_HEADER = 1
                        DATE_FORMAT = 'YYYY-MM-DD'
                        TIME_FORMAT = 'HH24.MI.SS'
                        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
                        TRIM_SPACE = TRUE
                        FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                        NULL_IF = ('\\N', 'NULL', 'NUL', '', 'null')
                        EMPTY_FIELD_AS_NULL = FALSE
                        ENCODING = 'UTF-8'
        """
        return file_format

    def sf_stage(self):
        stg_sql = """
                    CREATE OR REPLACE STAGE S3_DA_ETL_GLUE
                      storage_integration = s3_da_etl
                      url = 's3://da-etl/'
                      file_format = pglogformat;
           """
        return stg_sql

    def load_s3file_sftable(self, s3filepath, tablename):
        """:parameter
            s3filepath : path of the data file
            tablename: table to be loaded
        """

        copy_sql = """
                COPY into 
                /* table name */ 
                {}
                /* column list*/
                {} 
                from @S3_DA_ETL_GLUE/{}/{} pattern='.*.csv'   /*@S3_DA_ETL_GLUE/prd/TSDATA/2021_03_12/shared/booking_agent_dim */
            """
        try:
            if not self.db_connection is None:
                copy_sql = copy_sql.format(tablename,"", s3filepath, tablename)
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



    def load_json_rbitmq_sftable(self, msgdt, contents, tablename):
        """:parameter
            s3filepath : path of the data file
            tablename: table to be loaded
        """

        ins_sql = """
                INSERT into 
                /* table name */ 
                {}
                /* column list*/
                {} 
                select '{}'::timestamp,PARSE_JSON('{}')
            """
        try:
            if not self.db_connection is None:
                ins_sql = ins_sql.format(tablename,"", msgdt, contents)
                logging.info(ins_sql)
                self.db_connection.cursor().execute(ins_sql)

                #print("loading {} \n  {}".format(tablename, copy_sql.strip()))

                self.db_connection.cursor().execute("commit")
            else:
                logging.error("No Db connection ")


        except Exception as e:
            logging.error(e)

            self.db_connection.rollback()
            raise e
            logging.error(e)

def main():

    obj_s32sf = sfloader("SF_dev_inform_depric")

    obj_s32sf.load_s3file_sftable(s3filepath = "stgd566a.unigroupinc.com/qtgdb/qtg.esaccptinfo", tablename= 'esaccptinfo')





if __name__ == "__main__":
    main()
