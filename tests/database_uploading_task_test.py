import subprocess
import unittest

from pyspark import SQLContext, SparkContext, Row
from services.datavase_uploading_service import DatabaseUploadingService


class DatabaseUploadingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_name = "test_links"
        cls.db_username = "linker"
        cls.db_password = "linkerpassword"
        cls.table_name = "links"
        cls.input_path = "hdfs://localhost:9000/tmp/test/test.parquet"
        cls.service = DatabaseUploadingService(input_path=cls.input_path,
                                               db_name=cls.db_name,
                                               db_username=cls.db_username,
                                               db_password=cls.db_password,
                                               table_name=cls.table_name)
        sc = SparkContext(master="local", appName="Service")
        cls.sql_context = SQLContext(sc)
        links = ["link1",
                 "link2",
                 "link3",
                 "link4",
                 "link5"]
        cls.test_df = cls.sql_context \
            .createDataFrame(list(map(lambda x: Row(test_text=x), links)))
        cls.test_df.write.parquet(cls.input_path)

    def test_read_from_hdfs(self):
        self.assertListEqual(self.test_df.collect(), self.service.read_from_hdfs(self.sql_context).collect())

    def test_write_to_db(self):
        self.service.write_to_db(self.test_df)
        self.assertListEqual(self.sql_context.read.jdbc(url=f"jdbc:mysql://localhost:3306/{self.db_name}?useSSL=false",
                                                        table=self.table_name,
                                                        properties={"user": self.db_username,
                                                                    "password": self.db_password,
                                                                    "driver": "com.mysql.cj.jdbc.Driver"},
                                                        ).collect(),
                             [Row(test_text='link1'),
                              Row(test_text='link2'),
                              Row(test_text='link3'),
                              Row(test_text='link4'),
                              Row(test_text='link5')]
                             )

    @classmethod
    def tearDownClass(cls):
        cmd = f"hdfs dfs -rm -r {cls.input_path}".split()
        subprocess.run(cmd)
