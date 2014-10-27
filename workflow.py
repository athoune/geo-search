import zipfile
from random import random

import luigi
import requests


class FetchGeonamesZIP(luigi.Task):
    """\
Download country archive from geonames website.
    """

    country = luigi.Parameter(default="FR")

    def output(self):
        return luigi.LocalTarget("data/%s.zip" % self.country)

    def run(self):
        r = requests.get('http://download.geonames.org/export/dump/%s.zip' % self.country)
        assert r.status_code == 200
        with self.output().open('w') as f:
            f.write(r.content)


class Geonames(luigi.Task):
    """\
Extract a country archive
    """

    country = luigi.Parameter(default="FR")

    def requires(self):
        return FetchGeonamesZIP(self.country)

    def output(self):
        return luigi.LocalTarget("data/%s.txt" % self.country)

    def run(self):
        # Just for simulating crash
        if random() < 0.1:
            raise Exception("Just for the sport")
        with zipfile.ZipFile(self.requires().output().open('r'), 'r') as z:
            with self.output().open('w') as f:
                f.write(z.read('%s.txt' % self.country))

class RunAll(luigi.Task):
    """\
Dummy task that trigger all the work to be done
    """

    def requires(self):
        # some countries . US data is huge and slow
        for i in ["FR", "US", "GB", "BE", "RU", "CA", "JP", "PT"]:
            yield Geonames(i)

    def complete(self):
        # Nothing to do, just need requierements.
        return False

if __name__ == '__main__':
    luigi.run(main_task_cls=RunAll, cmdline_args=['--workers', '4'])
