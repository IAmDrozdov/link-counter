import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SQLContext

from saving_task import SavingTask


class DatabaseUploadingTask(PySparkTask):
    urls = luigi.ListParameter()

    db_host = luigi.Parameter()

    def requires(self):
        return SavingTask(urls=self.urls)

    def main(self, sc, *args):
        sql_context = SQLContext(sc)
        df = sql_context.read.parquet(self.input().path)
        df.write.jdbc(url=f"jdbc:mysql://{self.db_host}:3306/links",
                      table="links",
                      properties={"user": "mysqluser",
                                  "password": "mysqlpassword",
                                  "driver": ""},
                      mode="append")
