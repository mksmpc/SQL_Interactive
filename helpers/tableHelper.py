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
    return Table(other.__name__, newPath, other.__schema_path__)


def fromDB(tableName: str, newPath) -> 'Table':
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


class Table:
    """
    Helps work with educational tables
    """

    def __init__(self, db_name: str, path: str, schema_path: str = None, engine: sqlalchemy.engine.Engine = None):
        """
        Create object will provide easy work with resettable table
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
            self.__schema_path__ = schema_path
            self.__initTable__()

    def clear(self):
        """
        Erase all data from DB_table
        """
        __engine__.execute(f'DELETE FROM {self.__name__}')
        __engine__.execute('VACUUM')

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

    def __initTable__(self):
        self.__schema__ = self.__extractSchemaParameters__()
        self.__createTable__()
        data = self.__readCSV__()
        self.add(data)

    def __extractSchemaParameters__(self):
        schema = pd.read_csv(self.__schema_path__)
        parameters = []
        for index, row in schema.iterrows():
            end = ','
            if index == len(schema) - 1:
                end = ''
            param = row['name'] + ' ' + row['params'] + end
            parameters.append(param)

        parametersQuery = '\n'.join(parameters)
        return parametersQuery

    def __createTable__(self):
        dropStatement = f'DROP TABLE IF EXISTS {self.__name__}'
        createStatement = f'CREATE TABLE IF NOT EXISTS {self.__name__} ({self.__schema__});'
        
        __engine__.execute(dropStatement)
        __engine__.execute(createStatement)

    def __readCSV__(self):
        return pd.read_csv(self.__path__)

    def __repr__(self):
        result = ''
        for attribute, value in self.__dict__.items():
            result += f'{attribute} = {value}\n'

        data = __getTableData__(self.__name__)
        result += data.to_string(index=False)
        return result
