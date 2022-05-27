import numpy as np
import pandas as pd


def df_to_mysql(df, connection, target_table):

    """insert data from df to mysql, 

    Args:
        df (pandas dataframe): pandas dataframe
        connection (mysqlclient Connection Object): connection object from mysqlclient package
        target_table (str): target table name

    Raises:
        Exception: Exception from mysqlclient package
    """
    columns = df.columns
    columns_str = ', '.join(columns)
    placements_str = ', '.join(['%s']*len(columns))
    query = """INSERT INTO {} ({})
    VALUES ({});
    """.format(target_table, columns_str, placements_str)
    print('query: {}'.format(query))
    
    #prepare df as params
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.replace(np.nan, None)
    df = [tuple(row) for row in df.values] 
    try: 
        cursor = connection.cursor() 
        cursor.executemany(query, df)
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise Exception(e)
    finally:
        cursor.close()

def df_upsert_mysql(df, connection, target_table):
    
    """upsert data from df to mysql, 

    Args:
        df (pandas dataframe): pandas dataframe
        connection (mysqlclient Connection Object): connection object from mysqlclient package
        target_table (str): target table name

    Raises:
        Exception: Exception from mysqlclient package
    """

    columns = df.columns
    columns_str = ', '.join(columns)
    placements_str = ', '.join(['%s']*len(columns))
    update_list = ['{}=VALUES({})'.format(column, column) for column in columns]
    update_str = ', '.join(update_list)
    query = """INSERT INTO {} ({}) 
    VALUES ({}) 
    ON DUPLICATE KEY UPDATE {};"""\
        .format(target_table, columns_str, placements_str, update_str)
    print('query: {}'.format(query))

    #prepare df as params
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.replace(np.nan, None)
    df = [tuple(row) for row in df.values] 
    try: 
        cursor = connection.cursor() 
        cursor.executemany(query, df)
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise Exception(e)
    finally:
        cursor.close()
