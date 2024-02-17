Homework:

1. Use a generator
Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

Answer the following questions:

Question 1: What is the sum of the outputs of the generator for limit = 5?
Question 2: What is the 13th number yielded


def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# question 1
limit = 5
print(sum(num for num in square_root_generator(limit)))
# Answer = 8.382332347441762


# question 2
limit = 13
generator = square_root_generator(limit)
for ix, sqrt_value in enumerate(generator):
    if ix == limit-1:
        print(sqrt_value)

#
# question 2 = 3.605551275463989



# 2. Append a generator to a table with existing data


Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data

1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
2. Append the second generator to the same table as the first.
3. **After correctly appending the data, calculate the sum of all ages of people.**


import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}


for person in people_1():
    print(person)

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_1')

# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
							   table_name="people",
							   write_disposition="replace")
print(info)

info = generators_pipeline.run(people_2(),
							   table_name="people",
							   write_disposition="append")
print(info)

# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

display(conn.sql("describe people"))

# and the data

print("\n\n\n people table below:")

people = conn.sql("SELECT * FROM people").df()
display(people)

total_age = conn.sql("SELECT sum(age) FROM people").df()
display(total_age)

# Answer is 353


3. Merge a generator
Re-use the generators from Exercise 2.

A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

After loading, you should have a total of 8 records, and ID 3 should have age 33.

Question: Calculate the sum of ages of all the people loaded as described above.

generators_pipeline2 = dlt.pipeline(destination='duckdb', dataset_name='people_pk')

# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline2.run(people_1(),
							   table_name="people3",
							   write_disposition="replace",
                               primary_key = "id")
print(info)

info = generators_pipeline2.run(people_2(),
							   table_name="people3",
							   write_disposition="merge",
                               primary_key = "id")
print(info)

# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline2.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline2.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

display(conn.sql("describe people3"))

# and the data

print("\n\n\n people table below:")

people = conn.sql("SELECT * FROM people3").df()
display(people)

total_age = conn.sql("SELECT sum(age) FROM people3").df()
display(total_age)

# Answer is 266




