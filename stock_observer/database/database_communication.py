import mysql.connector as mysql
from log_setup import get_logger
from configparser import ConfigParser
from sqlalchemy import create_engine
from pandas import DataFrame, read_sql

logger = get_logger(__name__)


class MySQL_Connection:
    def __init__(self, config: ConfigParser):
        self.config = config
        # connecting to the database using 'connect()' method
        self.server = config['MySQL']['server']
        self.username = config['MySQL']['username']
        self.password = config['MySQL']['password']
        self.database = config['MySQL']['database']
        self.connection = mysql.connect(host=self.server,
                                        user=self.username,
                                        passwd=self.password,
                                        database=self.database)
        # creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
        self.cursor = self.connection.cursor()

    def __del__(self):
        # self.cursor.close()
        self.connection.close()

    def create_table(self, table_name: str, table_type: str, derivative_features=None):
        global query
        logger.info(f"Start creating {table_type} table with {table_name} name in the database")
        if table_type == 'stage':
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        open double, 
                        high double, 
                        low double, 
                        close double, 
                        volume bigint(20),
                        CCI double);"""
        elif table_type == 'main':
            cols = " double, ".join([str(i) for i in derivative_features]) + " double"
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        open double, 
                        high double, 
                        low double, 
                        close double, 
                        volume bigint(20), 
                        {cols});"""
        elif table_type == 'analysis':
            cols = " double, ".join([str(i) for i in derivative_features]) + " double"
            query = f"""CREATE TABLE {table_name} (
                        id VARCHAR(20) NOT NULL PRIMARY KEY, 
                        ticker VARCHAR(10) NOT NULL, 
                        date date NOT NULL, 
                        {cols});"""
        elif table_type == 'fundamentals':
            query = f"""CREATE TABLE {table_name} (
                        ticker VARCHAR(20) NOT NULL,
                        date DATE NOT NULL,
                        Market_Index VARCHAR(20) NULL,
                        Market_Cap VARCHAR(20) NULL, 
                        Income VARCHAR(20) NULL, 
                        Sales VARCHAR(20) NULL, 
                        Book_on_sh FLOAT NULL, 
                        Cash_on_sh FLOAT NULL, 
                        Dividend FLOAT NULL, 
                        Dividend_p VARCHAR(20) NULL, 
                        Employees BIGINT NULL,
                        Optionable VARCHAR(20) NULL, 
                        Shortable VARCHAR(20) NULL, 
                        Recom FLOAT NULL , 
                        P_on_E FLOAT NULL, 
                        Forward_P_on_E FLOAT NULL, 
                        PEG FLOAT NULL, 
                        P_on_S FLOAT NULL, 
                        P_on_B FLOAT NULL, 
                        P_on_C FLOAT NULL, 
                        P_on_FCF FLOAT NULL,
                        Quick_Ratio FLOAT NULL, 
                        Current_Ratio FLOAT NULL, 
                        Debt_on_Eq FLOAT NULL, 
                        LT_Debt_on_Eq FLOAT NULL, 
                        SMA20 FLOAT NULL, 
                        EPS_ttm FLOAT NULL, 
                        EPS_next_Y FLOAT NULL,
                        EPS_next_Q FLOAT NULL, 
                        EPS_this_Y FLOAT NULL, 
                        EPS_next_5Y FLOAT NULL, 
                        EPS_past_5Y FLOAT NULL, 
                        Sales_past_5Y FLOAT NULL,
                        Sales_Q_Q FLOAT NULL, 
                        EPS_Q_Q FLOAT NULL, 
                        Earnings VARCHAR(20) NULL, 
                        SMA50 FLOAT NULL, 
                        Insider_Own FLOAT NULL, 
                        Insider_Trans FLOAT NULL, 
                        Inst_Own FLOAT NULL,
                        Inst_Trans FLOAT NULL, 
                        ROA FLOAT NULL, 
                        ROE FLOAT NULL, 
                        ROI FLOAT NULL, 
                        Gross_Margin FLOAT NULL, 
                        Oper_Margin FLOAT NULL, 
                        Profit_Margin FLOAT NULL,
                        Payout FLOAT NULL, 
                        SMA200 FLOAT NULL,
                        Shs_Outstand VARCHAR(20) NULL, 
                        Shs_Float VARCHAR(20) NULL, 
                        Short_Float FLOAT NULL, 
                        Short_Ratio FLOAT NULL,
                        Target_Price FLOAT NULL, 
                        _52W_Range VARCHAR(20) NULL, 
                        _52W_High FLOAT NULL, 
                        _52W_Low FLOAT NULL, 
                        RSI_14 FLOAT NULL, 
                        Rel_Volume FLOAT NULL,
                        Avg_Volume VARCHAR(20) NULL, 
                        Volume FLOAT NULL, 
                        Perf_Week FLOAT NULL, 
                        Perf_Month FLOAT NULL, 
                        Perf_Quarter FLOAT NULL, 
                        Perf_Half_Y FLOAT NULL,
                        Perf_Year FLOAT NULL, 
                        Perf_YTD FLOAT NULL, 
                        Beta FLOAT NULL, 
                        ATR FLOAT NULL, 
                        Volatility VARCHAR(20) NULL, 
                        Prev_Close FLOAT NULL, 
                        Price FLOAT NULL,
                        Change_Price FLOAT NULL);"""
        self.cursor.execute(query)

    def insert(self, table_name, data_df: DataFrame) -> None:
        """
        :param table_name:
        :param data_df: storing values in a variable
                values = ("Hafeez", "hafeez")
        :return: None
        """
        values = [tuple(x) for x in data_df.to_numpy()]
        cols = ", ".join([str(i) for i in data_df.columns.tolist()])
        try:
            query = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," * (data_df.shape[1] - 1) + "%s)"
            # executing the query with values
            self.cursor.execute(query, values)
            # to make final output we have to run the 'commit()' method of the database object
            self.connection.commit()

            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error(f"insert error: {e}")
            raise e

    def insert_many(self, table_name, data_df: DataFrame) -> None:
        """
        :param data_df: storing values in a variable
                values = [("Peter", "peter"), ("Amy", "amy"), ("Michael", "michael"), ("Hennah", "hennah")]
        :param table_name:
        :return: None
        """
        # data_df.drop(columns='Date', inplace=True)
        values_list = [tuple(x) for x in data_df.to_numpy()]
        cols = ", ".join([str(i) for i in data_df.columns.tolist()])
        try:
            query = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," * (data_df.shape[1] - 1) + "%s)"
            # executing the query with values
            self.cursor.executemany(query, values_list)
            # to make final output we have to run the 'commit()' method of the database object
            self.connection.commit()
            logger.info(f"{self.cursor.rowcount} record inserted")
        except Exception as e:
            logger.error(f"insert many error: {e}")
            raise e

    def insert_df(self, data_df: DataFrame, table_name: str, primary_key: str, if_exists: str = 'fail'):
        engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.server}/{self.database}')
        db_connection = engine.connect()
        try:
            test = self.select(f"SELECT * FROM {table_name} LIMIT 3;")
            if not test.empty:
                counter = 0
                logger.info(f"Deleting old data from data warehouse")
                key_list = data_df[primary_key]
                key_len = len(key_list)
                for key_value in key_list:
                    counter = +1
                    self.delete(f"Delete from {table_name} where {primary_key} = '{key_value}';")
                    if counter % 1000 == 0:
                        logger.info(f"{counter} of {key_len} is checked")

            logger.info(f"Inserting new data into data warehouse")
            data_df.to_sql(name=table_name, con=db_connection, if_exists=if_exists, index=False,
                           index_label=primary_key)
        except ValueError as vx:
            logger.error(vx)
            raise vx
        except Exception as ex:
            logger.error(ex)
            raise ex
        else:
            logger.info(f"Data insertion into the {table_name} is done")
        finally:
            db_connection.close()

    def select(self, query: str) -> DataFrame:
        engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.server}/{self.database}')
        db_connection = engine.connect()
        try:
            data = read_sql(query, db_connection)
            return data
        except Exception as e:
            logger.warning(f"select error: {e}")
        finally:
            db_connection.close()

    def delete(self, query) -> None:
        """
        :param query: query = "DELETE FROM users WHERE id = 5"
        :return: None
        """
        try:
            # executing the query
            self.cursor.execute(query)
            # final step to tell the database that we have changed the table data
            self.connection.commit()
        except Exception as e:
            logger.error(f"delete error: {e}")
