from pyspark.sql import SparkSession
from pyspark.sql import functions as f


# pass your path to PostgreSQL JDBC Driver to "driver_path" variable 
driver_path = "postgresql-42.3.4.jar"
# pass your username in PostgreSQL to "db_username" variable
db_username = "your_db_username"
# pass your password of PostgreSQL server to "db_password" variable
db_password = "your_password"

spark = SparkSession.builder.config("spark.jars", driver_path).master("local").\
    appName("PySpark_Postgres").getOrCreate()

# set your db_url if needed
db_url = "jdbc:postgresql://localhost:5432/pagila"
db_properties = {"driver": "org.postgresql.Driver", "user": db_username, "password": db_password}

actor = spark.read.jdbc(url=db_url, table='actor', properties=db_properties)
address = spark.read.jdbc(url=db_url, table='address', properties=db_properties)
category = spark.read.jdbc(url=db_url, table='category', properties=db_properties)
city = spark.read.jdbc(url=db_url, table='city', properties=db_properties)
country = spark.read.jdbc(url=db_url, table='country', properties=db_properties)
customer = spark.read.jdbc(url=db_url, table='customer', properties=db_properties)
film = spark.read.jdbc(url=db_url, table='film', properties=db_properties)
film_actor = spark.read.jdbc(url=db_url, table='film_actor', properties=db_properties)
film_category = spark.read.jdbc(url=db_url, table='film_category', properties=db_properties)
inventory = spark.read.jdbc(url=db_url, table='inventory', properties=db_properties)
language = spark.read.jdbc(url=db_url, table='language', properties=db_properties)
payment = spark.read.jdbc(url=db_url, table='payment', properties=db_properties)
rental = spark.read.jdbc(url=db_url, table='rental', properties=db_properties)
staff = spark.read.jdbc(url=db_url, table='staff', properties=db_properties)
store = spark.read.jdbc(url=db_url, table='store', properties=db_properties)

# pipeline № 1
print("number of films by category:")
(
    film_category.groupBy("category_id").count().alias("df1")
    .join(category.alias("df2"), f.col("df1.category_id") == f.col("df2.category_id"), "inner")
    .select(f.col("df2.name").alias("category_name"), f.col("df1.count").alias("mov_in_category"))
    .orderBy(f.col("mov_in_category").desc()).show()
)

# pipeline № 2 (by factual rental duration)
print("10 actors whose films were rented the most:")
(
    rental.alias("df1")
    .join(inventory.alias("df2"), f.col("df1.inventory_id") == f.col("df2.inventory_id"), "inner")
    .join(film_actor.alias("df3"), f.col("df2.film_id") == f.col("df3.film_id"), "inner")
    .groupBy(f.col("df3.actor_id")).agg(f.sum(f.col("df1.return_date")-f.col("df1.rental_date"))
                                        .alias("sum_rental_duration"))
    .join(actor.alias("df4"), f.col("df3.actor_id") == f.col("df4.actor_id"), "inner")
    .select(f.col("df4.actor_id"), f.col("df4.first_name"), f.col("df4.last_name"), f.col("sum_rental_duration"))
    .orderBy(f.col("sum_rental_duration").desc()).limit(10).show(truncate=False)
)

# pipeline № 3
print("the category of films that they spent the most money on:")
(
    payment.alias("df1").join(rental.alias("df2"), f.col("df1.rental_id") == f.col("df2.rental_id"), "inner")
    .join(inventory.alias("df3"), f.col("df2.inventory_id") == f.col("df3.inventory_id"), "inner")
    .join(film_category.alias("df4"), f.col("df3.film_id") == f.col("df4.film_id"), "inner")
    .groupBy(f.col("df4.category_id")).agg(f.sum(f.col("df1.amount")).alias("money_on_category"))
    .join(category.alias("df5"), f.col("df4.category_id") == f.col("df5.category_id"), "inner")
    .select(f.col("df5.name"))
    .orderBy(f.col("money_on_category").desc()).limit(1).show()
)

# pipeline № 3 (version without considering rows when the same rental_id is set for several customer_id)
print("the category of films that they spent the most money on. Version #2:")
payment_2 = payment.filter(~((f.col("rental_id") == 4591) & (f.col("customer_id").isin([577, 16, 259, 401, 546]))))
print(
    payment_2.alias("df1").join(rental.alias("df2"), f.col("df1.rental_id") == f.col("df2.rental_id"), "inner")
    .join(inventory.alias("df3"), f.col("df2.inventory_id") == f.col("df3.inventory_id"), "inner")
    .join(film_category.alias("df4"), f.col("df3.film_id") == f.col("df4.film_id"), "inner")
    .groupBy(f.col("df4.category_id")).agg(f.sum(f.col("df1.amount")).alias("money_on_category"))
    .join(category.alias("df5"), f.col("df4.category_id") == f.col("df5.category_id"), "inner")
    .select(f.col("df5.name"))
    .orderBy(f.col("money_on_category").desc()).collect()[0][0]
    )

# pipeline № 4
print("film titles that are not in the inventory:")
mov_not_in_inventory = (
    film.alias("df1").join(inventory.alias("df2"), f.col("df1.film_id") == f.col("df2.film_id"), "leftanti")
    .select(f.col("df1.title"))
                       )
mov_not_in_inventory.show(mov_not_in_inventory.count())

# pipeline № 5
print("top 3 actors who most appeared in films in the 'Children' category:")
pre_result_top_3 = (
    film_category.alias("df1")
    .join(category.alias("df2"), f.col("df1.category_id") == f.col("df2.category_id"), "inner")
    .filter(f.col("df2.name") == "Children")
    .join(film_actor.alias("df3"), f.col("df1.film_id") == f.col("df3.film_id"), "inner")
    .groupBy(f.col("df3.actor_id")).count()
    .join(actor.alias("df4"), f.col("df3.actor_id") == f.col("df4.actor_id"), "inner")
    .select(f.col("df4.first_name"), f.col("df4.last_name"), f.col("count").alias("movies_in_children_category"))
    .orderBy(f.col("movies_in_children_category").desc())
                    )

num_list = [row[0] for row in pre_result_top_3.select(f.col("movies_in_children_category")).distinct()
            .orderBy(f.col("movies_in_children_category").desc()).limit(3).collect()]

pre_result_top_3.filter(f.col("movies_in_children_category").isin(num_list)).show()

# pipeline № 6 (the table is too long, let's show the first 20 rows)
print("cities with the number of active and inactive customers:")
(
    customer.withColumn("non-active", f.when(f.col("active") == 1, 0).otherwise(1)).alias("df1")
    .join(address.alias("df2"), f.col("df1.address_id") == f.col("df2.address_id"), "inner")
    .join(city.alias("df3"), f.col("df2.city_id") == f.col("df3.city_id"), "inner")
    .groupBy(f.col("df3.city"))
    .agg(f.sum(f.col("df1.active")).alias("total_active"), f.sum(f.col("df1.non-active")).alias("total_non-active"))
    .select(f.col("df3.city"), f.col("total_active"), f.col("total_non-active"))
    .orderBy(f.col("total_non-active").desc()).show()
)

# pipeline № 7.1 (cities started with the “a” letter - case insensitive)
print("the movie category with the highest total rental hours in cities and that starts with the 'a' letter:")
print(
    rental.alias("df1")
    .join(inventory.alias("df2"), f.col("df1.inventory_id") == f.col("df2.inventory_id"), "inner")
    .join(film_category.alias("df3"), f.col("df2.film_id") == f.col("df3.film_id"), "inner")
    .join(category.alias("df4"), f.col("df3.category_id") == f.col("df4.category_id"), "inner")
    .join(customer.alias("df5"), f.col("df1.customer_id") == f.col("df5.customer_id"), "inner")
    .join(address.alias("df6"), f.col("df5.address_id") == f.col("df6.address_id"), "inner")
    .join(city.alias("df7"), f.col("df6.city_id") == f.col("df7.city_id"), "inner")
    .filter(f.lower(f.col("df7.city")).startswith("a"))
    .groupBy(f.col("df4.name")).agg(f.sum(f.col("df1.return_date")-f.col("df1.rental_date")).alias("target_rental"))
    .orderBy(f.col("target_rental").desc())
    .select(f.col("df4.name"))
    .collect()[0][0]
    )

# pipeline № 7.2 (cities which contain “-” symbol)
print("the movie category with the highest total rental hours in cities and that contains the '-' symbol:")
print(
    rental.alias("df1")
    .join(inventory.alias("df2"), f.col("df1.inventory_id") == f.col("df2.inventory_id"), "inner")
    .join(film_category.alias("df3"), f.col("df2.film_id") == f.col("df3.film_id"), "inner")
    .join(category.alias("df4"), f.col("df3.category_id") == f.col("df4.category_id"), "inner")
    .join(customer.alias("df5"), f.col("df1.customer_id") == f.col("df5.customer_id"), "inner")
    .join(address.alias("df6"), f.col("df5.address_id") == f.col("df6.address_id"), "inner")
    .join(city.alias("df7"), f.col("df6.city_id") == f.col("df7.city_id"), "inner")
    .filter(f.col("df7.city").contains("-"))
    .groupBy(f.col("df4.name")).agg(f.sum(f.col("df1.return_date")-f.col("df1.rental_date")).alias("target_rental"))
    .orderBy(f.col("target_rental").desc())
    .select(f.col("df4.name"))
    .collect()[0][0]
)
