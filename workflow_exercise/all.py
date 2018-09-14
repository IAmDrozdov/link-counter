import argparse
import hashlib
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import urlopen, Request

import bs4
import luigi
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
from pyspark import SQLContext
from pyspark.sql.types import Row


class ExtractingTask(PySparkTask):
    url = luigi.Parameter()

    def _get_domain(self):
        parsed_url = urlparse(self.url)
        return "://".join([parsed_url.scheme, parsed_url.netloc])

    @staticmethod
    def _extract_links(html):
        soup = bs4.BeautifulSoup(html, features="html5lib")
        return list([link["href"].strip() for link in soup.findAll("a") if link.get("href")])

    def main(self, sc, *args):
        try:
            request = Request(self.url)
            html = urlopen(request)
        except (ValueError, HTTPError):
            print(f"Broken url {self.url}")
            with self.output().open("w") as f:
                pass
        else:
            raw_links = self._extract_links(html)
            domain = self._get_domain()
            sql_context = SQLContext(sc)
            df = sql_context \
                .createDataFrame(list(map(lambda x: Row(url=f"{domain}{x}" if r"://" not in x else x), raw_links)))
            df.write.parquet(self.output().path)

    def output(self):
        return HdfsTarget(f"hdfs://localhost:9000/tmp/extracted/{self.url.replace('://', '-')}.parquet")


class SavingTask(PySparkTask):
    urls = luigi.ListParameter()

    def requires(self):
        return [ExtractingTask(url=url) for url in self.urls]

    def main(self, sc, *args):
        sql_context = SQLContext(sc)

        parquets = [parquet.path for parquet in self.input()]
        df = sql_context.read.parquet(*parquets)

        df \
            .groupBy("url") \
            .count() \
            .write.parquet(self.output().path)

    def output(self):
        hash = hashlib.sha1()
        for url in sorted(self.urls):
            hash.update(url.encode())
        dir_id = hash.hexdigest()
        return HdfsTarget(f"hdfs://localhost:9000/tmp/saved/{dir_id}.parquet")


class DatabaseUploadingTask(PySparkTask):
    urls = luigi.ListParameter()
    db_host = luigi.Parameter()

    def requires(self):
        return SavingTask(urls=self.urls)

    def main(self, sc, *args):
        sql_context = SQLContext(sc)
        df = sql_context.read.parquet(self.input().path)
        df.write.jdbc(url=f"jdbc:mysql://{self.db_host}:3306/links?useSSL=false",
                      table="links",
                      properties={"user": "linker",
                                  "password": "linkerpassword",
                                  "driver": "com.mysql.cj.jdbc.Driver"},
                      mode="append")


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("urls",
                        type=str,
                        nargs="+",
                        help="URLs for scan"
                        )
    parser.add_argument("--host",
                        choices=["localhost",
                                 "compose"
                                 ],
                        type=str,
                        default="localhost",
                        help='''localhost - run without docker-compose,
                        scheduler - run with docker-compose'''
                        )
    parser.add_argument("--scheduler",
                        choices=["local",
                                 "centralized"
                                 ],
                        type=str,
                        default="local",
                        help='set scheduler'
                        )
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    luigi.build(
        [DatabaseUploadingTask(urls=args.urls, db_host="db" if args.host == "compose" else "localhost")],
        workers=len(args.urls),
        local_scheduler=args.scheduler == "local",
        scheduler_host=args.host if args.host == "localhost" else "scheduler"
    )
