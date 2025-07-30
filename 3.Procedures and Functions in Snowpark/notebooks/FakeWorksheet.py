# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from faker import Faker

def main(session: snowpark.Session): 
    f = Faker()
    output =  [[f.name(),f.address(),f.city(),f.state(),f.email()]
    for _ in range(1000)]
    df= session.create_dataframe(output,schema=["name","address","city","state","email"])
    return df