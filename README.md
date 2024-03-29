All queries are in the file 'metadata.sql'

Question 1

Union All

Question 2 

c1 = N; 0 < c2 <= N; 0 <= c3 <= N

Question 3

Table T3(mi, n+j)

Question 4

T1 [INNER] JOIN T2 on a.id = b.id

Question 5

Since, we have live data events we can use Hybryd approach  where real-time data is initially captured in a high-performance NoSQL database or an in-memory datastore and then, either in real-time or in batches, moved to a relational database where complex relationships and historical data analysis can be more easily managed.
Firstly, we save the information inside a NoSQL database like Redis and then push data into a relational database (here, I use SQL (as an example)). Alternatively, we can use a more efficient columnar database like Snowflake/Clickhouse/Greenplum or a Lakehouse solution like Databricks.

About data modeling, for analytical tasks, we can implement here Data Vault 2.0 or Anchor model, but I think that for this task  this is unnecessary for this assignment I guess (but I try to put a sql scriprt with modeling data with using anchor model) 
Since the assignment is not completely clear about what we 
need to design (an analytical dwh or a 
database to store transactional information), 
I will create normalized tables (up to third normal form).

You can write the script inside the file 'metadata.sql'

Question 6

This approach could lead to a table with a large number of columns, especially if the number of settings is high or is expected to grow.

We can use here JSON object, which allows you to store all settings in a structured format within a single column. This provides flexibility and can accommodate any changes in the settings without altering the table schema.

P.S. Depending on the technology you use, other practices can be applied.
Let's say in Databricks using Spark, we can save information in the form of a struct or Map. In the form of a Athena engine (AWS), we can also save it as a Map.

You can find a query inside the file 'metadata.sql'

Question 7

We'll start by ranking the battles for each player according to the battle_id.
Then we'll flag each row where the result changes from 'win' to 'loss' or 'loss' to 'win'.
Following that, we'll carry out a running sum of these flags, which effectively groups consecutive wins or losses together.
Finally, we'll count the number of wins in each group and determine the maximum count for each player.

Question 8 

Just check the query
