import hashlib

import luigi
from extraction_task import ExtractingTask
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

    def main(self, sc, *args):
        """
        Get all files from ExtractionTask output count the number
        of occurrences of each link and save in back to HDFS.
        """
        sql_context = SQLContext(sc)
        parquets = ["/tmp/extracted/" + parquet.path for parquet in self.input()]
        df = sql_context.read.parquet(*parquets)

        df \
            .groupBy("url") \
            .count() \
            .write.parquet(self.output().path)

    def output(self):
        """
        Generate unique directory id from URL pool
        :return: HDFS target for next task
        """
        hasher = hashlib.sha1()
        for url in sorted(self.urls):
            hasher.update(url.encode())
        dir_id = hasher.hexdigest()
        return HdfsTarget(f"hdfs://localhost:9000/tmp/saved/{dir_id}.parquet")
