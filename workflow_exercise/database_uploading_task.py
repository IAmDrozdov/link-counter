import luigi
import pymysql
from saving_task import SavingTask


class DatabaseUploadingTask(luigi.Task):
    urls = luigi.ListParameter()
    db_host = luigi.Parameter()

    def requires(self):
        return SavingTask(self.urls)

    def run(self):
        with pymysql.connect(host=self.db_host,
                             port=3306,
                             user="mysqluser",
                             password="mysqlpassword",
                             db="links") as cursor, self.input().open("r") as f:
            for line in f:
                url, count = line.split()
                sql = f"INSERT INTO links (url, entries) values ('{url}',{count})"
                try:
                    cursor.execute(sql)
                except pymysql.err.DataError as e:
                    print(e)
