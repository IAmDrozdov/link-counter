import subprocess
import unittest

from pyspark import SparkContext, SQLContext, Row
from services.saving_service import SavingService


class SavingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.urls = ["test_url1", "test_url2"]
        cls.input_path = "hdfs://localhost:9000/tmp/test/test.parquet"
        cls.output_path = "hdfs://localhost:9000/tmp/test/62ab4bc868c0902a190edc7b5da5f290e47a9113.parquet"
        cls.service = SavingService(input_path=[cls.input_path],
                                    urls=cls.urls,
                                    output_prefix="hdfs://localhost:9000/tmp/test/")

        sc = SparkContext(master="local", appName="Service")
        cls.sql_context = SQLContext(sc)
        links = ["link1",
                 "link2",
                 "link1",
                 "link2",
                 "link3"]
        cls.test_df = cls.sql_context \
            .createDataFrame(list(map(lambda x: Row(url=x), links)))
        cls.test_df.write.parquet(cls.input_path)

    def test_get_output_name(self):
        self.assertEqual("62ab4bc868c0902a190edc7b5da5f290e47a9113", self.service.get_output_name())

    def test_read_from_hdfs(self):
        self.assertListEqual(self.test_df.collect(), self.service.read_from_hdfs(self.sql_context).collect())

    def test_count_occurrence(self):
        counted_df = self.service.count_occurrence(self.test_df)
        self.assertListEqual([Row(url='link2'), Row(url='link1'), Row(url='link3')],
                             counted_df.select("url").collect())
        self.assertListEqual([Row(count=2), Row(count=2), Row(count=1)],
                             counted_df.select("count").collect())
        # Can not compare
        # self.assertListEqual([Row(url='link2', count=2), Row(url='link1', count=2), Row(url='link3', count=1)],
        #                      counted_df.collect())

    def test_write_to_hdfs(self):
        self.service.write_to_hdfs(self.test_df)
        cmd = f"hdfs dfs -stat {self.output_path}".split()
        self.assertIsNotNone(subprocess.check_output(cmd))

    @classmethod
    def tearDownClass(cls):
        cmd = f"hdfs dfs -rm -r {cls.input_path}".split()
        subprocess.run(cmd)
        cmd = f"hdfs dfs -rm -r {cls.output_path}".split()
        subprocess.run(cmd)
