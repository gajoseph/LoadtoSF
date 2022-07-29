import logging

from sqlalchemy import create_engine
import os 

class dbLoader():
    """":parameter
        Loads data to postgres db

    """

    db_connection = None
    insert_sql = ""

    def __del__(self):
        logging.info('Destructor called, for object :{}'.format(self.__class__.__name__))
        if self.db_connection is not None:
            self.db_connection.dispose()

    def __init__(self, __prop_dict):
        """
            initialize (●__●)
        """
        logging.info('Constructor called, for object : {}'.format(self.__class__.__name__))
        self.__prop_dict = __prop_dict
        self.init_db_conn()

    def __buildDbstring__(self):
        return "postgresql://{}:{}@{}:{}/{}".format(self.__prop_dict['pg.user']
                                                    , self.__prop_dict['pg.password']
                                                    , self.__prop_dict['pg.server']
                                                    , self.__prop_dict['pg.port']
                                                    , self.__prop_dict['pg.database'])

    def init_db_conn(self):
        try:
            self.db_connection = create_engine(self.__buildDbstring__()
                                               , connect_args={
                    'options': '-csearch_path={}'.format(self.__prop_dict['pg.schema'])}

                                               )
            result_set = self.db_connection.execute(
                "select version();")

            for r in result_set:
                logging.info(r)

        except Exception as e:
            logging.error(e)
            logging.error("No Db connection made ")

            raise e

    def strip_spec_chars(self, string1):
        for rep in ['\\\"', ',', '\r', ';', '\\t', '\n']:
            string1 = string1.replace(rep, "")

        return string1

    def load_qlik_apps(self, app_name, app_tab_sec_name, app_qvd_name, qvd_name, qvd_cols):
        """
        qlik_app_qvd_flds

        :return:
        """
        ins_sql = """
            insert into qlik_app_qvd_flds values(%s,%s,%s,%s,%s)
        """

        self.db_connection.execute(ins_sql, (
        self.strip_spec_chars(app_name), self.strip_spec_chars(app_tab_sec_name), self.strip_spec_chars(app_qvd_name),
        self.strip_spec_chars(qvd_name), qvd_cols.strip()))


    def load_qlik_app_qvd_fld(self, app_name, qlik_tab_name, qlik_app_local_qvd_name, qlik_qvd_name, qlik_qvd_col, qlik_qvd_col_alias
                            , qlik_qvd_fld):
        """
        qlik_app_qvd_fld

        :return:
        """
        ins_sql = """
            insert into qlik_app_qvd_fld values(%s,%s,%s,%s,%s, %s, %s, %s)
        """

        self.db_connection.execute(ins_sql, (
        self.strip_spec_chars(app_name), self.strip_spec_chars(qlik_tab_name), self.strip_spec_chars(qlik_app_local_qvd_name),
        self.strip_spec_chars(qlik_qvd_name), qlik_qvd_col, qlik_qvd_col_alias, qlik_qvd_fld, os.getlogin()))


    def del_qlik_app_qvd_fld(self, app_name, qlik_tab_name, qlik_app_local_qvd_name, qlik_qvd_name):
        """
        qlik_app_qvd_fld

        :return:
        """
        del_sql = """
            delete from  qlik_app_qvd_fld where qlik_app_name = %s and qlik_tab_name= %s and qlik_app_local_qvd_name = %s and 
                qlik_qvd_name = %s

        """
        logging.info(" Deleting from  qlik_app_qvd_fld  for {}; {}; {}; {}".format(app_name
                                                                    , qlik_tab_name
                                                                    , qlik_app_local_qvd_name
                                                                    , qlik_qvd_name))

        dels = self.db_connection.execute(del_sql, (app_name, qlik_tab_name,  qlik_app_local_qvd_name
                                                                    , qlik_qvd_name))

        if dels.rowcount ==0: 
            logging.info(" Deleting from  qlik_app_qvd_fld  for {}; {}; {}; {} NO ROWS deleted = {} INVESTIGATE".format(app_name
                                                                    , qlik_tab_name
                                                                    , qlik_app_local_qvd_name
                                                                    , qlik_qvd_name
                                                                    , dels.rowcount))

                                                                                
        else:
            logging.info(" Deleting from  qlik_app_qvd_fld  for {}; {}; {}; {} deleted = {}".format(app_name
                                                                    , qlik_tab_name
                                                                    , qlik_app_local_qvd_name
                                                                    , qlik_qvd_name
                                                                    , dels.rowcount))





    # def load_qlik_apps(self, app_name, app_qvd_name, qvd_name, qvd_cols ):
    #     """
    #     qlik_app_qvd_flds

    #     :return:
    #     """
    #     ins_sql = """
    #         insert into qlik_app_qvd_flds values(%s,%s,%s,%s)
    #     """

    #     self.db_connection.execute(ins_sql, (app_name, app_qvd_name, qvd_name, qvd_cols ))

    def del_qvd_tab_columns_by_app_name(self, qlik_app_name):
        """
        qlik_app_qvd_flds
        :return:
        """
        del_sql = """
            delete from  QVD_tab_columns where qlik_app_name = %s

        """
        logging.info(" deletiing from  QVD_tab_columns  for {}".format(qlik_app_name))

        self.db_connection.execute(del_sql, (qlik_app_name))




    def del_qlik_app_qvd_flds_by_app_name(self, qlik_app_name):
            
        del_sql = """
            delete from  qlik_app_qvd_flds where qlik_app_name = %s

        """
        # logging.info(" insert into QVD_tab_columns \n values ({},{},{},{}, {}, {})".format(qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name,db_tab_name, db_col_name ))
        logging.info(" deletiing from  qlik_app_qvd_flds  for {}".format(qlik_app_name))

        self.db_connection.execute(del_sql, (qlik_app_name))


    def load_qvd_tab_columns(self, qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name, db_tab_name,
                             db_col_name):
        """
        qlik_app_qvd_flds
        :return:
        """
        ins_sql = """
            insert into QVD_tab_columns values(%s,%s,%s,%s,%s, %s)

        """
        # logging.info(" insert into QVD_tab_columns \n values ({},{},{},{}, {}, {})".format(qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name,db_tab_name, db_col_name ))

        self.db_connection.execute(ins_sql, (
        qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name, db_tab_name, db_col_name))

    def load_qvd_load_text(self, qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name, db_tab_name, qlik_load_text):
        ins_sql = """
            insert into qvd_load_text values(%s,%s,%s,%s,%s,%s)
        """

        self.db_connection.execute(ins_sql, (
        qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name, db_tab_name, qlik_load_text))


    def del_load_qvd_load_text(self, qlik_app_name):
            
        del_sql = """
            delete from  qvd_load_text where qlik_app_name = %s

        """
        # logging.info(" insert into QVD_tab_columns \n values ({},{},{},{}, {}, {})".format(qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name,db_tab_name, db_col_name ))

        self.db_connection.execute(del_sql, (qlik_app_name))    


    def truc_qlik_apps_info(self):
        """
        qlik_app_qvd_flds

        :return:
        """
        ins_sql = """
            truncate table qlik_app_qvd_flds
        """

        self.db_connection.execute(ins_sql)

    def truc_QVD_tab_columns(self):
        """
        qlik_app_qvd_flds

        :return:
        """
        ins_sql = """
            truncate table qvd_tab_columns
        """

        self.db_connection.execute(ins_sql)


    def  ins_qlik_app_sheet_controls(self, qlik_app_id, qlik_app_name, sheet_id, sheet_name, parent_control_id, parent_control_type
    , control_id, control_type, control_title, control_fld_label, control_fld_field  ):

                            
        """
        qlik_app_sheet_controls
        :return:
        
create table qlik_app_sheet_controls(
	qlik_app_id varchar(50),
	qlik_app_name varchar(200),
	sheet_id varchar(50),
	sheet_name varchar(200),
	parent_control_id varchar(50),
	parent_control_type varchar(50),
	control_id varchar(50),
	
	control_type varchar(150),
	control_fld_label	 varchar(150),
	control_fld_field	text,
	load_user_code  varchar(32) null,
	load_dt	timestamp without time zone not null default timezone('UTC'::text, now())
	
)
        """
        ins_sql = """
            insert into qlik_app_sheet_controls(
                qlik_app_id, qlik_app_name
                , sheet_id, sheet_name
                , parent_control_id, parent_control_type
                , control_id, control_type
                , control_title
                , control_fld_label,control_fld_field
                , load_user_code    )
            
             values(%s,%s
                    , %s, %s
                    , %s, %s
                    , %s, %s
                    , %s
                    , %s, %s
                    , %s)

        """
        # logging.info(" insert into QVD_tab_columns \n values ({},{},{},{}, {}, {})".format(qlik_app_name, qlik_tab_name, qlik_conn_name, qlik_qvd_name,db_tab_name, db_col_name ))
        
        self.db_connection.execute(ins_sql, (
         qlik_app_id, qlik_app_name
         , sheet_id, sheet_name
         , parent_control_id,parent_control_type
         , control_id, control_type
        , control_title
        , control_fld_label, control_fld_field,
        os.getlogin()))



    def  del_qlik_app_sheet_controls(self, qlik_app_id : str, sheet_id : str):    
        """
        qlik_app_sheet_controls
        :return:
        """
        del_sql = """
            Delete from qlik_app_sheet_controls values where qlik_app_id= %s and sheet_id= %s

        """
       
        self.db_connection.execute(del_sql, (qlik_app_id, sheet_id))
