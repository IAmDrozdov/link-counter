import luigi
import pymysql
from saving_task import SavingTask


class DatabaseUploadingTask(luigi.Task):
    urls = luigi.ListParameter()

    def requires(self):
        return SavingTask(self.urls)

    def run(self):
        with pymysql.connect(host='localhost',
                             user='root',
                             password='1111',
                             db='links') as cursor, self.input().open("r") as f:
            for line in f:
                url, count = line.split()
                sql = f"INSERT INTO links (url, entries) values ('{url}',{count})"
                try:
                    cursor.execute(sql)
                except pymysql.err.DataError as e:
                    print(e)


if __name__ == "__main__":
    luigi.run()
