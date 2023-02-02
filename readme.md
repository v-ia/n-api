**Пояснения к реализации:**

1) Файл .env_sample нужно переименовать в .env и заполнить в нем данные окружения для базы данных
2) После запуска docker-compose и завершения инициализации автоматически запускается файл main.py, и выполняются все необходимые по заданию действия
3) Порой удобнее взаимодействовать между базой данных и Pandas при помощи SQLAlchemy ORM, но решил реализовать напрямую через psycopg2
4) Для создания таблицы в базе данных написал вспомогательную функцию "_get_sql_query_for_create_table", которая автоматически генерирует SQL-запрос  на основе структуры переданного ей в качестве параметра DataFrame
5) Аналогично написал вспомогательную функцию "_get_sql_query_for_insert" для автоматический генерации SQL-запросов при загрузке данных на основе данных из DataFrame
6) Для получения из таблицы списка всех имен астероидов, удовлетворяющих условиям по полям searching_date и miss_distance_km, в функции "get_asteroid_names" дополнительно добавлен параметр "condition", позволяющий задавать тип условия (<, >= и так далее)
7) Для долгосрочного хранения данных в yaml файле создан volume

**Пример результата запуска main.py:**

<img width="1055" alt="Screenshot 2023-02-02 at 10 02 09 PM" src="https://user-images.githubusercontent.com/102062747/216433301-a7169a6a-a448-43c6-9a23-7bc759976b05.png">


**Пример результата загрузки данных из DataFrame в базу данных:**

<img width="1217" alt="Screenshot 2023-02-02 at 9 42 08 PM" src="https://user-images.githubusercontent.com/102062747/216433366-6efee816-9df9-445a-ac7d-2cc646fddb59.png">

