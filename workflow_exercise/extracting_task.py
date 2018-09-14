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
    """
    Task for scraping links from URL and saving them to HDFS.
    :arg url : URL for current task.
    """
    url = luigi.Parameter()

    def _get_domain(self):
        """
        Get domain address from URL
        :return: pure domain address
        """
        parsed_url = urlparse(self.url)
        return "://".join([parsed_url.scheme, parsed_url.netloc])

    @staticmethod
    def _extract_links(html):
        """
        Scrap links from html. Check each a-tag and get only with href-attribute
        :param html: response from GET-request
        :return: List of links
        """
        soup = bs4.BeautifulSoup(html, features="html5lib")
        return list([link["href"].strip() for link in soup.findAll("a") if link.get("href")])

    def main(self, sc, *args):
        """
        Get page via URL, extract links from it and save it as parquet file in HDFS.
        """
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
        """
        Save each result  as file named with URL.
        :return: HDFS target for new task.
        """
        return HdfsTarget(f"/tmp/extracted/{self.url.replace('://', '-')}.parquet")

