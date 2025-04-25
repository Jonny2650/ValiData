"""This module provides functions to connect to a database using Polars and to read in data from csv files."""

import polars as pl

def create_db_uri(conn_engine:str, username:str, password:str, host:str, port:str, database:str)->str:
    """
    Create a database URI for polars connections.
    
    Args:
        conn_engine (str): The connection engine (e.g., 'postgresql', 'mysql', etc.).
        username (str): The username for the database.
        password (str): The password for the database.
        host (str): The host of the database.
        port (str): The port of the database.
        database (str): The name of the database.

    Returns:
        str: The formatted database URI.
    """
    
    uri = f"{conn_engine}://{username}:{password}@{host}:{port}/{database}"
    
    return uri


def create_simple_query(col_list:list, table_name:str, and_filters:dict = None, or_filters:dict = None)->str:
    """
    Create a SQL query string to select specific columns from a table.
    
    Args:
        col_list (list): List of column names to select.
        table_name (str): The name of the table.#
        and_filters (dict): Dictionary of and filters to apply to the query. Defaults to None.
        or_filters (dict): Dictionary of or filters to apply to the query. Defaults to None.

    Returns:
        str: The formatted SQL query string.
    """
    
    cols = ", ".join(col_list)
    
    if and_filters is not None:
        and_filters = "".join([f"AND {key} = {value}\n" for key, value in and_filters.items()])
    else:
        and_filters = ""
        
    if or_filters is not None:
        or_filters = "".join([f"OR {key} = {value}\n" for key, value in or_filters.items()])
    else:
        or_filters = ""
    
    query = f"""\
        SELECT {cols}\n\
        FROM {table_name}\n\
        WHERE 1=1\n\
        {and_filters}\n\
        {or_filters}"""
    
    return query


def create_complex_query(query:str)->str:
    """
    Collect a user inputted  SQL query.
    
    Args:
        query (str): The user defined SQL query string.

    Returns:
        str: The formatted complex SQL query string.
    """
    
    complex_query = f"{query}"
    
    return complex_query


def read_from_db(uri:str, query:str, partition_on:str = None, partition_num:int = None)->pl.LazyFrame:
    """
    Read data from a database using a SQL query.
    
    Args:
        uri (str): The database URI.
        query (str): The SQL query string.  
        partition_on (str, optional): Column name to partition the data on. Defaults to None.
        partition_num (int, optional): Number of partitions. Defaults to None.

    Returns:
        pl.LazyFrame: A Polars LazyFrame containing the result of the query.
    """
    
    df = pl.read_database_uri(query=query, uri=uri)
    
    return df.lazy()


def read_from_csv(file_path:str, schema:dict, has_header:bool = True, separator:str = ",", skip_rows:int = 0, with_column_names:list = None)->pl.LazyFrame:
    """
    Read data from a CSV file.
    
    Args:
        file_path (str): The path to the CSV file.
        schema (dict): Dictionary defining the schema of the CSV file.
        has_header (bool, optional): Whether the CSV file has a header row. Defaults to True.
        separator (str, optional): The separator used in the CSV file. Defaults to None.
        skip_rows (int, optional): Number of rows to skip at the start of the file. Defaults to None.
        with_column_names (list, optional): List of column names to use. Defaults to None.

    Returns:
        pl.LazyFrame: A Polars LazyFrame containing the data from the CSV file.
    """
    
    df = pl.scan_csv(
        source=file_path,
        schema=schema,
        has_header=has_header,
        separator=separator,
        skip_rows=skip_rows,
        with_column_names=with_column_names
    )
    
    return df
