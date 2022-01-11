"""
Helps work with educational tables
"""

import pandas as pd
import sqlalchemy.engine

__engine__: sqlalchemy.engine.Engine = None


def setEngine(engine: sqlalchemy.engine.Engine):
    global __engine__
    __engine__ = engine


def fromOther(other: 'Table', newPath) -> 'Table':
    """
    Copies data to new file from other tableHelper.Table

    :param other: exists instance of Table
    :param newPath: write data into this file (.CSV)
    :return: new Table instance
    """
    other.save(newPath)
    return Table(other.__name__, newPath)


def CSVfromDB(tableName: str, newPath) -> 'Table':
    """
    Creates CSV file from exist DB's table

    :param tableName: exist DB's table name
    :param newPath: write table into this file (.CSV)
    :return: new Table instance
    """
    newTable = Table(tableName, newPath)
    newTable.save()
    return newTable


def __getTableData__(tableName):
    data = pd.read_sql_table(tableName, __engine__)
    return data


def __extractSchemaParameters__(schema_path):
    schema = pd.read_csv(schema_path)
    parameters = []
    for index, row in schema.iterrows():
        end = ','
        if index == len(schema) - 1:
            end = ''
        param = row['name'] + ' ' + row['params'] + end
        parameters.append(param)

    parametersQuery = '\n'.join(parameters)
    return parametersQuery


class Table:
    """
    Helps work with educational tables
    """

    def __init__(self, db_name: str, path: str, schema_path: str = None, engine: sqlalchemy.engine.Engine = None):
        """
        Creates object that will provide easy work with resettable table
        Note: Table will dropped if schema_path will been passed

        :param db_name: desired DB's table name
        :param path: path to CSV file, from which table will filled
        :param schema_path: path to CSV file, from which table schema will created
        :param engine: instance of sqlalchemy.engine
        """
        global __engine__

        if engine:
            __engine__ = engine

        if __engine__ is None:
            raise ValueError('Engine is not assigned. Pass the engine= argument')

        self.__name__ = db_name
        self.__path__ = path

        if schema_path:
            self.createTable(schema_path)
            self.reset()

    def createTable(self, schema_path):
        """
        Creates table in DB based on passed schema
        
        :param schema_path: path to CSV that contains schema parameters
        """
        schema = __extractSchemaParameters__(schema_path)
        dropStatement = f'DROP TABLE IF EXISTS {self.__name__}'
        createStatement = f'CREATE TABLE IF NOT EXISTS {self.__name__} ({schema});'
        
        __engine__.execute(dropStatement)
        __engine__.execute(createStatement)

    def clear(self):
        """
        Erase all data from DB_table
        """
        __engine__.execute(f'DELETE FROM {self.__name__}')

    def reset(self):
        """
        Reset data in DB_table to values from CSV file
        """
        self.clear()
        data = self.__readCSV__()
        self.add(data)

    def save(self, path: str = None):
        """
        Save table data into CSV file

        :param path: path to CSV file. Override current if None
        """
        if not path:
            path = self.__path__

        data = __getTableData__(self.__name__)
        data.to_csv(path, index=False)

    def add(self, values, override=False):
        """
        INSERT *values* into DB_table

        :param values: Format: {'column': 'value'}
                {'column': ['value1', 'value2']}
        :param override:
        """

        data = None
        if isinstance(values, pd.DataFrame):
            data = values

        else:
            params = {}
            v = tuple(values.values())[0]
            if not isinstance(v, (list, tuple)):
                params['index'] = [0]
            data = pd.DataFrame(values, **params)

        data.to_sql(self.__name__, con=__engine__, index=False, if_exists='append', method='multi')

    def __readCSV__(self):
        return pd.read_csv(self.__path__)

    def __repr__(self):
        result = ''
        for attribute, value in self.__dict__.items():
            result += f'{attribute} = {value}\n'

        data = __getTableData__(self.__name__)
        result += data.to_string(index=False)
        return result
