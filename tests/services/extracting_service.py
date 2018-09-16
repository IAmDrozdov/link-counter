from urllib.parse import urlparse
from urllib.request import Request, urlopen

import bs4
from pyspark import Row, SparkContext
from pyspark.sql import SQLContext


class ExtractingService:
    def __init__(self, url):
        self.url = url
        self.output_path = f"hdfs://localhost:9000/tmp/test/{self.url.replace('://', '-')}.parquet"

    def get_domain(self):
        parsed_url = urlparse(self.url)
        return "://".join([parsed_url.scheme, parsed_url.netloc])

    def get_response(self):
        request = Request(self.url)
        return urlopen(request)

    @staticmethod
    def extract_links(response):
        soup = bs4.BeautifulSoup(response, features="html5lib")
        return list([link["href"].strip() for link in soup.findAll("a") if link.get("href")])

    def write_to_hdfs(self, raw_links):
        sc = SparkContext(master="local", appName="Service")
        sql_context = SQLContext(sc)
        domain = self.get_domain()
        df = sql_context \
            .createDataFrame(list(map(lambda x: Row(url=f"{domain}{x}" if r"://" not in x else x), raw_links)))
        df.write.mode("overwrite").parquet(self.output_path)
