from collections import Counter

import luigi
from extraction_task import ExtractionTask


class SavingTask(luigi.Task):
    urls = luigi.ListParameter()

    def requires(self):
        return [ExtractionTask(url) for url in self.urls]

    def run(self):
        links = []
        for _input in self.input():
            with _input.open("r") as f:
                for line in f:
                    links.append(line.strip())

        counted_links = Counter(links)

        with self.output().open("w") as f:
            for k, v in counted_links.items():
                f.write(f"{k} {v}\n")

    def output(self):
        return luigi.LocalTarget("tmp/second/counted-links.txt")
