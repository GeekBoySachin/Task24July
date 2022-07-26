import pymongo
import logging
import mysql.connector as conn
import pandas as pd
import json

logging.basicConfig(filename="task.log", format='%(asctime)s : %(message)s', filemode="w", level=logging.DEBUG)
logger = logging.getLogger()


class Task28July:

    def __init__(self, user, pwd):
        """Class constructor"""
        self.user = user
        self.pwd = pwd

    def connect_mysql(self):
        """Method to connect mysql database"""
        try:
            connect = conn.connect(host="localhost", user=self.user, password=self.pwd)
            logger.info("Connected to SQL database")
            return connect
        except Exception as e:
            logger.error("Some error occurred while connecting to sql: " + str(e))

    def connect_mongodb(self):
        """method to connect mongodb database"""
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            logger.info("Connected to Mongodb database")
            return client
        except Exception as e:
            logger.error("Some error occurred while connecting to Mongo DB: " + str(e))

    def create_sql_tables(self,cursor):
        """Answer 1: method to create database and tables in mysql"""
        cursor.execute("create database if not exists task28july")
        logging.info("Database created successfully")
        cursor.execute("use task28july")
        attribute_table_structure ="Dress_ID int,Style varchar(20),Price varchar(10),Rating FLOAT(2),Size varchar(10),Season varchar(20),NeckLine varchar(20),SleeveLength varchar(20),waiseline varchar(20),Material varchar(20),FabricType varchar(10),Decoration varchar(10),`Pattern Type` varchar(10),Recommendation int"
        cursor.execute("create table if not exists attribute("+attribute_table_structure+")")
        logging.info("Table Attribute created successfully")
        dress_sales_structure = "Dress_ID int,`29/8/2013` int,`31/8/2013` int,`2/9/2013` int,`4/9/2013` int,`6/9/2013` int,`8/9/2013` int,`10/9/2013` int,`12/9/2013` int,`14/9/2013` int,`16/9/2013` int,`18/9/2013` int,`20/9/2013` int,`22/9/2013` int,`24/9/2013` int,`26/9/2013` int,`28/9/2013` int,`30/9/2013` int,`2/10/2013` int,`4/10/2013` int,`6/10/2013` int,`8/10/2010` int,`10/10/2013` int,`12/10/2013` int"
        cursor.execute("create table if not exists sales(" + dress_sales_structure + ")")
        logging.info("Table dress sales created successfully")

    def load_data(self,cursor):
        """Answer 2:method to load data inside the sql tables"""
        attribute_df = pd.read_excel(r".\datasets\Attribute DataSet.xlsx",header=None)
        attribute_df = attribute_df.fillna("none")
        dress_sales_df = pd.read_excel(r".\datasets\Dress Sales.xlsx",header=None)
        dress_sales_df = dress_sales_df.fillna(0)
        for i in range(1,attribute_df.shape[0]):
            try:
                cursor.execute("insert into attribute values"+str(tuple(attribute_df.loc[i])))
            except Exception as e:
                logging.error("Failed to insert in attribute table: "+str(e))

        for i in range(1,dress_sales_df.shape[0]):
            try:
                cursor.execute("insert into sales values"+str(tuple(dress_sales_df.loc[i])))
            except Exception as e:
                logging.error("Failed to insert in attribute table: "+str(e))

    def read_data_pandas(self,conn):
        """Answer 3:method to read data from mysql using pandas"""
        df_attribute = pd.read_sql("select * from task28july.attribute",conn)
        print(df_attribute)
        df_sales = pd.read_sql("select * from task28july.sales",conn)
        print(df_sales)
        logging.info("Data fetched successfully")

    def upload_to_mongo(self,client):
        """Answer 4 and 5:method to convert data in json and upload converted data in mongo db"""
        attribute_df = pd.read_excel(r".\datasets\Attribute DataSet.xlsx")
        # attribute_df.to_json(r".\datasets\attribute.json")
        data = attribute_df.to_json(orient="records",default_handler=dict)
        logging.info("Data converted in to json")
        data = json.loads(data)
        print(data)
        client_db = client["task28july"]
        client_collection = client_db["attribute"]
        try:
            client_collection.insert_many(data)
            logging.info("Data saved successfully")
        except Exception as e:
            logging.error("Some errro occured while inserting data in mongodb: "+str(e))

    def left_join(self,cursor):
        """Answer 6:method to perform left join between attribute and sales table"""
        cursor.execute("select * from attribute left join sales on attribute.dress_id = sales.dress_id")
        logging.info("Data fetched after left join operation")
        for row in cursor.fetchall():
            print(row)

    def unique_dress(self,cursor):
        """Answer 7:method to search unique dress"""
        cursor.execute("select distinct dress_id from sales")
        logging.info("Unique dress id's fetched")
        for row in cursor.fetchall():
            print(row)

    def zero_recommendation(self,cursor):
        """Answer 8:method to search dress with zero recommendation"""
        cursor.execute("select count(dress_id) from attribute where recommendation =0")
        logging.info("No of dress having recommendation 0 detail fetched")
        print(cursor.fetchone()[0])

    def total_sales(self,cursor):
        """Answer 9:method to fetch total sales of individual dresses"""
        cursor.execute("select dress_id,sum(`29/8/2013`  + `31/8/2013`  + `2/9/2013`  + `4/9/2013`  + `6/9/2013`  + `8/9/2013`  + `10/9/2013`  + `12/9/2013`  + `14/9/2013`  + `16/9/2013`  + `18/9/2013`  + `20/9/2013`  + `22/9/2013`  + `24/9/2013`  + `26/9/2013`  + `28/9/2013`  + `30/9/2013`  + `2/10/2013`  + `4/10/2013`  + `6/10/2013`  + `8/10/2010`  + `10/10/2013`  + `12/10/2013`) as `Total Sales` from sales group by dress_id")
        for row in cursor.fetchall():
            print("Dress ID: ",row[0],"Sales: ",str(row[1]))

    def third_highest(self,cursor):
        """Answer 10:method to find third highest most selling dress id """
        cursor.execute("create view salesview as select dress_id,sum(`29/8/2013`  + `31/8/2013`  + `2/9/2013`  + `4/9/2013`  + `6/9/2013`  + `8/9/2013`  + `10/9/2013`  + `12/9/2013`  + `14/9/2013`  + `16/9/2013`  + `18/9/2013`  + `20/9/2013`  + `22/9/2013`  + `24/9/2013`  + `26/9/2013`  + `28/9/2013`  + `30/9/2013`  + `2/10/2013`  + `4/10/2013`  + `6/10/2013`  + `8/10/2010`  + `10/10/2013`  + `12/10/2013`) as `Total Sales` from sales group by dress_id")
        cursor.execute("select dress_id, MIN(`Total sales`) from(select * from(select * from salesview order by `Total Sales` Desc) salesview limit 3) as a")
        result = cursor.fetchone()
        print("Thirst highest most selling dress id: ",result[0],"Sales amount: ",result[1])
        cursor.execute("drop view salesview")
        logging.info("Third highest saled dress details fetched")





if __name__=="__main__":
    obj = Task28July("root","root")
    conn = obj.connect_mysql()
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute("use task28july")
        # obj.create_sql_tables(cursor)
        # obj.load_data(cursor)
        # conn.commit()
        # obj.read_data_pandas(conn)
        # obj.left_join(cursor)
        # obj.unique_dress(cursor)
        # obj.zero_recommendation(cursor)
        # obj.total_sales(cursor)
        obj.third_highest(cursor)
    # client = obj.connect_mongodb()
    # obj.upload_to_mongo(client)

    conn.close()
    # client.close()
