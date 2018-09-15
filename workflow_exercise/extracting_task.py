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

    def get_domain(self):
        """
        Get domain address from URL
        :return: pure domain address
        """
        parsed_url = urlparse(self.url)
        return "://".join([parsed_url.scheme, parsed_url.netloc])

    @staticmethod
    def extract_links(html):
        """
        Scrap links from html. Check each a-tag and get only with href-attribute
        :param html: response from GET-request
        :return: List of links
        """
        soup = bs4.BeautifulSoup(html, features="html5lib")
        return list([link["href"].strip() for link in soup.findAll("a") if link.get("href")])

    def _get_html(self):
        """
        Get html from URL
        :return: html from URL page
        """
        request = Request(self.url)
        return urlopen(request)

    def write_to_hdfs(self, raw_links, sql_context):
        """
        Write list of links to HDFS
        """
        domain = self.get_domain()
        df = sql_context \
            .createDataFrame(list(map(lambda x: Row(url=f"{domain}{x}" if r"://" not in x else x), raw_links)))
        df.write.parquet(self.output().path)

    def main(self, sc, *args):
        """
        Get page via URL, extract links from it and save it as parquet file in HDFS.
        """
        try:
            sql_context = SQLContext(sc)
            html = self._get_html()
        except (ValueError, HTTPError):
            print(f"Broken url {self.url}")
            with self.output().open("w") as f:
                pass
        else:
            raw_links = self.extract_links(html)
            self.write_to_hdfs(raw_links, sql_context)

    def output(self):
        """
        Save each result  as file named with URL.
        :return: HDFS target for new task.
        """
        return HdfsTarget(f"hdfs://localhost:9000/tmp/extracted/{self.url.replace('://', '-')}.parquet")
