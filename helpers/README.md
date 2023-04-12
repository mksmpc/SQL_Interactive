# tableHelper.py module

## Проблема:
В процессе обучения и работы с запросами изменения данных возникла потребность быстрого сброса таблицы к её исходному состоянию, а также временной модификации данных для некоторых заданий.

Было решено написать вспомогательный модуль, который оптимизирует рутинные процессы очистки и наполнения таблицы предустановленными данными.

## Возможности:
- создание таблиц из CSV файлов
- сброс значений таблицы к её изначальному состоянию
- создание CSV файлов из существующих таблиц
- добавление данных в таблицу

## Применение:
### 1. Передать объект соединения с БД  
>   `tableHelper.setEngine(engine: sqlalchemy.engine.base.Engine)`  

### 2. Создать экземпляр таблицы:  

>     tableHelper.Table(db_name: str, path: str, schema_path: str = None, engine: sqlalchemy.engine.base.Engine = None)
>        Created object will provide easy work with resettable table
>        Note: Table will dropped if schema_path will been passed
>          
>        :param db_name: desired DB's table name
>        :param path: path to CSV file, from which table will filled
>        :param schema_path: path to CSV file, from which table schema will created
>        :param engine: instance of sqlalchemy.engine
>
>     fromDB(tableName: str, newPath) -> 'Table'   
>        Creates CSV file from exist DB's table   
>        
>        :param tableName: exist DB's table name  
>        :param newPath: write table into this file (.CSV)  
>        :return: new Table instance  
>    
>     fromOther(other: 'Table', newPath) -> 'Table'  
>        Copies data to new file from other tableHelper.Table  
>        
>        :param other: exists instance of Table  
>        :param newPath: write data into this file (.CSV)  
>        :return: new Table instance`
	
### 3. Использовать:

>     add(self, values, override=False)  
>        INSERT *values* into DB_table  
>        
>        :param values: Format: {'column': 'value'}  
>                {'column': ['value1', 'value2']}  
>        :param override:  
>    
>     clear(self)  
>        Erase all data from DB_table  
>    
>     reset(self)  
>        Reset data in DB_table to values from CSV file  
>    
>     save(self, path: str = None)  
>        Save table data into CSV file  
>        
>        :param path: path to CSV file. Override current if None  
