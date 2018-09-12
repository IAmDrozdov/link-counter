import luigi
from extraction_task import ExtractingTask
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
from pyspark import SQLContext


class SavingTask(PySparkTask):
    urls = luigi.ListParameter()

    def requires(self):
        return [ExtractingTask(url=url) for url in self.urls]

    def main(self, sc, *args):
        sql_context = SQLContext(sc)

        df = sql_context.read.parquet("/tmp/extracted/*.parquet")

        df\
            .groupBy("url")\
            .count()\
            .write\
            .parquet(self.output().path)

    def output(self):
        return HdfsTarget("/tmp/saved/counted-links.parquet")
