import hashlib

import luigi
from extracting_task import ExtractingTask
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
from pyspark import SQLContext


class SavingTask(PySparkTask):
    """
    Task for counting the number of occurrences of each link
    and saving it in HDFS.
    :arg urls: list of URLs for extracting
    """
    urls = luigi.ListParameter()

    def requires(self):
        """
        Create own ExtractingTask for each URL.
        """
        return [ExtractingTask(url=url) for url in self.urls]

    def read_from_hdfs(self, sql_context):
        """
        Read all parquets from all ExtractionTask outputs
        """
        parquets = [parquet.path for parquet in self.input()]
        return sql_context.read.parquet(*parquets)

    def get_output_name(self):
        """
        Generate unique directory id from URL pool
        :return: unique name for output
        """
        hasher = hashlib.sha1()
        for url in sorted(self.urls):
            hasher.update(url.encode())
        return hasher.hexdigest()

    @staticmethod
    def count_occurrence(df):
        """
        Count number of occurrence of each link
        :param df: dataframe
        :return: dataframe with counted links
        """
        return df.groupBy("url").count()

    def write_to_hdfs(self, df):
        """
        Write data to HDFS
        :param df: dataframe
        """
        df.write.parquet(self.output().path)

    def main(self, sc, *args):
        sql_context = SQLContext(sc)
        df = self.read_from_hdfs(sql_context)
        df = self.count_occurrence(df)
        self.write_to_hdfs(df)

    def output(self):
        """
        :return: HDFS target for next task
        """
        dir_id = self.get_output_name()
        return HdfsTarget(f"hdfs://localhost:9000/tmp/saved/{dir_id}.parquet")
