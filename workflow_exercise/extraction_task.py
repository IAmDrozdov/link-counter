from urllib.parse import urlparse
from urllib.request import urlopen, Request
from luigi.format import UTF8
import bs4
import luigi


class ExtractionTask(luigi.Task):
    url = luigi.Parameter()

    def _get_domain(self):
        parsed_url = urlparse(self.url)
        return "://".join([parsed_url.scheme, parsed_url.netloc])

    def run(self):
        try:
            domain = self._get_domain()
            request = Request(self.url)
            html = urlopen(request)
        except ValueError:
            print(f"Broken url {self.url}")
            with self.output().open("w") as f:
                pass
        else:
            soup = bs4.BeautifulSoup(html)
            links = [link["href"] for link in soup.findAll("a") if link.get("href")]
            with self.output().open("w") as f:
                for link in links:
                    f.write(f"{link}\n") if "://" or "#" in link else f.write(f"{domain}{link}\n")

    def output(self):
        return luigi.LocalTarget(f"output/first/{str(self.url).replace('/', '-')}.txt" , format=UTF8)
