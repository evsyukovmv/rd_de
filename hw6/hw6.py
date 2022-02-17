from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

pg_url = "jdbc:postgresql://localhost:5432/postgres"
pg_properties = {"user": "gpuser", "password": "secret"}

spark = SparkSession.builder \
    .config('spark.driver.extraClassPath', '/home/user/shared_folder/postgresql-42.2.20.jar') \
    .master('local') \
    .appName("lesson") \
    .getOrCreate()

category_df = spark.read.jdbc(pg_url, table="category", properties=pg_properties)
film_category_df = spark.read.jdbc(pg_url, table="film_category", properties=pg_properties)
actor_df = spark.read.jdbc(pg_url, table="actor", properties=pg_properties)
film_actor_df = spark.read.jdbc(pg_url, table="film_actor", properties=pg_properties)
film_df = spark.read.jdbc(pg_url, table="film", properties=pg_properties)
payment_df = spark.read.jdbc(pg_url, table="payment", properties=pg_properties)
rental_df = spark.read.jdbc(pg_url, table="rental", properties=pg_properties)
inventory_df = spark.read.jdbc(pg_url, table="inventory", properties=pg_properties)
city_df = spark.read.jdbc(pg_url, table="city", properties=pg_properties)
address_df = spark.read.jdbc(pg_url, table="address", properties=pg_properties)
customer_df = spark.read.jdbc(pg_url, table="customer", properties=pg_properties)

# 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
category_df.join(film_category_df, 'category_id').groupBy('category_id', 'name').count().orderBy(
    F.desc('count')).show()

# 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
actor_df.join(film_actor_df, 'actor_id').join(film_df, 'film_id'). \
    groupBy('actor_id', 'first_name', 'last_name').\
    agg(F.sum(F.col('rental_duration')).alias('films_rental_duration')).\
    orderBy(F.desc('films_rental_duration')).limit(10).show()


# 3. вывести категорию фильмов, на которую потратили больше всего денег.
payment_df.join(rental_df, 'rental_id').join(inventory_df, 'inventory_id').join(film_category_df, 'film_id')\
    .join(category_df, 'category_id').groupBy('name').agg(F.sum('amount').alias('sum_payment'))\
    .orderBy(F.desc('sum_payment')).limit(1).show()

# 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
film_df.join(inventory_df, 'film_id', how='leftanti').select('title').show()

# 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
# Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
actor_df.join(film_actor_df, 'actor_id').join(film_category_df, 'film_id').join(category_df, 'category_id')\
    .where("name = 'Children'").groupBy('first_name', 'last_name')\
    .agg(F.count('film_id').alias('films_count'))\
    .withColumn("rank", F.rank().over(Window.partitionBy().orderBy("films_count"))).where('rank <= 3')\
    .select('first_name', 'last_name').show()

# 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
# Отсортировать по количеству неактивных клиентов по убыванию.
city_df.join(address_df, 'city_id').join(customer_df, 'address_id').groupBy('city')\
    .agg(
        F.sum(F.when(F.col('active') == 1, F.lit(1)).otherwise(F.lit(0))).alias('active_count'),
        F.sum(F.when(F.col('active') == 0, F.lit(1)).otherwise(F.lit(0))).alias('non_active_count'))\
    .orderBy(F.desc('non_active_count'))\
    .show()


# 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
# (customer.address_id в этом city), и которые начинаются на букву “a”.
# То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

(category_df
 .join(film_category_df, 'category_id', 'left')
 .join(inventory_df, 'film_id', 'left')
 .join(rental_df, 'inventory_id', 'left')
 .join(customer_df, 'customer_id', 'left')
 .join(address_df, 'address_id', 'left')
 .join(city_df, 'city_id', 'inner')
 .where('city like "a%"')
 .groupBy('category_id')
 .agg(F.sum(F.col('return_date') - F.col('rental_date')).alias('rent_duration'))
 .orderBy(F.desc('rent_duration'))
 .limit(1)
 ).unionByName(
    category_df
        .join(film_category_df, 'category_id', 'left')
        .join(inventory_df, 'film_id', 'left')
        .join(rental_df, 'inventory_id', 'left')
        .join(customer_df, 'customer_id', 'left')
        .join(address_df, 'address_id', 'left')
        .join(city_df, 'city_id', 'inner')
        .where('city like "%-%"')
        .groupBy('category_id')
        .agg(F.sum(F.col('return_date') - F.col('rental_date')).alias('rent_duration'))
        .orderBy(F.desc('rent_duration'))
        .limit(1)
).show()
