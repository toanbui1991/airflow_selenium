def generate_upsert_statement(target_table, data_point):
    """generate msyql upsert statement with unique constrant is the checking condition 

    Args:
        target_table (str): table name
        data_point (dict): data point as dictionary (a row in mysql)

    Returns:
        str: mysql upsert statement
    """
    columns = list(data_point.keys())
    columns_str = ', '.join(columns)
    placements_str = ', '.join(['%s']*len(columns))
    update_list = ['{}=VALUES({})'.format(column, column) for column in columns]
    update_str = ', '.join(update_list)
    query = """INSERT INTO {} ({}) 
    VALUES ({}) 
    ON DUPLICATE KEY UPDATE {};"""\
        .format(target_table, columns_str, placements_str, update_str)
    return query