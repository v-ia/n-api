from datetime import date, timedelta
from dotenv import load_dotenv
from psycopg2.extensions import connection
import pandas as pd
import psycopg2
import requests
import os


# Получить список астероидов за последние 3 дня
def get_earth_objects() -> dict:
    try:
        resp = requests.get(f'https://api.nasa.gov/neo/rest/v1/feed?'
                        f'start_date={date.today() - timedelta(days=2)}&'
                        f'end_date={date.today()}&api_key=DEMO_KEY')
        resp_dict = resp.json()
        near_earth_objects = resp_dict["near_earth_objects"]
        with open("near_earth_objects.txt", "w") as f:
            f.write(str(near_earth_objects))
        return near_earth_objects
    except Exception as error:
        print(f"Ошибка доступа к api.nasa.gov (возможно, превышено количество соединений): {error}")


# На основе полученного ответа API преобразовать содержимое тега near_earth_objects в DataFrame
def get_dataframe(data_dict: dict) -> pd.DataFrame:
    data_list = list()
    for key in data_dict.keys():
        for ob in data_dict[key]:
            data_list.append([int(ob['id']), ob['name'], ob['is_potentially_hazardous_asteroid'],
                              float(ob['estimated_diameter']['kilometers']['estimated_diameter_min']),
                              float(ob['estimated_diameter']['kilometers']['estimated_diameter_max']),
                              float(ob['close_approach_data'][0]['relative_velocity']['kilometers_per_second']),
                              float(ob['close_approach_data'][0]['miss_distance']['kilometers']), date.today()])

    df = pd.DataFrame(data_list, columns=['id', 'name', 'is_potentially_hazardous_asteroid',
                                          'estimated_diameter_min_km', 'estimated_diameter_max_km',
                                          'relative_velocity_km_sec', 'miss_distance_km', 'searching_date'])
    df['searching_date'] = df['searching_date'].astype('datetime64')
    df.to_csv("near_earth_objects.csv", index=False)
    return df


# Из полученного выше DataFrame сформируйте словарь с ключами из списка ниже и соответствующими ключам значениями
def get_stat(df: pd.DataFrame) -> dict:
    collision_hours = df['miss_distance_km'] / df['relative_velocity_km_sec'].multiply(3600)
    return {'potentially_hazardous_count': len(df[df['is_potentially_hazardous_asteroid']]),
            'name_with_max_estimated_diam': df.iloc[df['estimated_diameter_max_km'].idxmax()]['name'],
            'min_collision_hours': collision_hours.min()
            }


# Напишите функцию, которая открывает соединение с базой данных
def get_connection() -> connection:
    load_dotenv()
    connect = psycopg2.connect(host=os.getenv('POSTGRES_HOST'),
                               user=os.getenv('POSTGRES_USER'),
                               password=os.getenv('POSTGRES_PASSWORD'),
                               database=os.getenv('POSTGRES_DB'))
    return connect


# Вспомогательная функция для автоматической генерации sql запроса создания таблицы на основе структуры DataFrame
def _get_sql_query_for_create_table(table_name: str, df: pd.DataFrame) -> str:
    set_primary_key = 0
    type_mapping = {"int64": "bigint", "object": "character varying",
                    "bool": "boolean", "float64": "real", "datetime64[ns]": "date"}
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for i, col_name in enumerate(df.columns):
        sql += f"{col_name} {type_mapping[str(df.dtypes[i])]}"
        if not set_primary_key:
            set_primary_key = 1
            sql += " PRIMARY KEY"
        sql += " NOT NULL, "
    sql = sql[:len(sql) - 2]
    sql += ")"
    return sql


# Инициализация таблицы asteroids, если ее еще нет в базе
def create_table_asteroids(connect: connection, df: pd.DataFrame):
    with connect.cursor() as cur:
        cur.execute(_get_sql_query_for_create_table("public.asteroids", df))
        connect.commit()


# Вспомогательная функция для автоматической генерации sql запроса загрузки данных на основе DataFrame в базу данных
def _get_sql_query_for_insert(table_name: str, df: pd.DataFrame) -> str:
    sql = f"INSERT INTO {table_name} ("
    values = str()
    for i, col_name in enumerate(df.columns):
        sql += f"{col_name}, "
        values += "%s, "
    sql, values = sql[:len(sql) - 2], values[:len(values) - 2]
    sql += f") VALUES ({values}) ON CONFLICT DO NOTHING"
    return sql


# Вставка в таблицу данных из DataFrame
def data_insert_asteroids(connect: connection, df: pd.DataFrame):
    sql = _get_sql_query_for_insert("public.asteroids", df)
    with connect.cursor() as cur:
        for index, row in df.iterrows():
            cur.execute(sql, [x for x in row])
        connect.commit()


# Получение из таблицы списка всех имен астероидов, удовлетворяющих условиям по полям searching_date и miss_distance_km
def get_asteroid_names(connect, searching_date: date, miss_distance_km: float, condition: str) -> list:
    with connect.cursor() as cur:
        cur.execute(f'''SELECT name FROM public.asteroids WHERE searching_date {condition} %s AND 
                    miss_distance_km {condition} %s''', (searching_date, miss_distance_km))
        records = cur.fetchall()
    result = list()
    for record in records:
        result.append(*record)
    return result


if __name__ == '__main__':
    print('Получение списка астероидов за последние 3 дня..')
    near_objects = get_earth_objects()
    print('Done', 'Преобразование содержимого тега near_earth_objects в DataFrame..', sep='\n')
    df_obj = get_dataframe(near_objects)
    print('Первые 5 записей из DataFrame:', df_obj.head(), sep='\n')
    print('Done', 'Из полученного выше DataFrame формирование словаря с 3 ключами..', sep='\n')
    stat = get_stat(df_obj)
    print('Done', stat, sep='\n')

    conn = None
    try:
        print('Установление соединения с базой данных..')
        conn = get_connection()
        print('Done', 'Инициализация таблицы asteroids в БД..', sep='\n')
        create_table_asteroids(conn, df_obj)
        print('Done', 'Вставка в таблицу данных из DataFrame..', sep='\n')
        data_insert_asteroids(conn, df_obj)
        print('Done', 'Получение из таблицы списка всех имен астероидов, удовлетворяющих условиям..', sep='\n')
        print(get_asteroid_names(conn, date.today(), 2522035, '>='), 'Done', sep='\n')
    except Exception as error:
        print(f"Ошибка во время работы с базой данных: {error}")
    finally:
        if conn:
            conn.close()
