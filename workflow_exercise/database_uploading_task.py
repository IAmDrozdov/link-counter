import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SQLContext

from saving_task import SavingTask


class DatabaseUploadingTask(PySparkTask):
    """
    Task for passing data from HDFS to MySQL database
    """
    urls = luigi.ListParameter()
    db_host = luigi.Parameter()

    def requires(self):
        return SavingTask(urls=self.urls)

    def read_from_hdfs(self, sql_context):
        """
        Read data from HDFS
        :return: dataframe
        """
        return sql_context.read.parquet(self.input().path)

    def write_to_db(self, df):
        """
        Write data to MySQL database
        :param df: dataframe
        """
        df.write.jdbc(url=f"jdbc:mysql://{self.db_host}:3306/links?useSSL=false",
                      table="links",
                      properties={"user": "linker",
                                  "password": "linkerpassword",
                                  "driver": "com.mysql.cj.jdbc.Driver"},
                      mode="append")

    def main(self, sc, *args):
        sql_context = SQLContext(sc)
        df = self.read_from_hdfs(sql_context)
        self.write_to_db(df)
