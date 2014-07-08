import zipfile

import luigi
import requests

#http://download.geonames.org/export/dump/
class FetchGeonamesZIP(luigi.Task):

    country = luigi.Parameter(default="FR")

    def output(self):
        return luigi.LocalTarget("data/%s.zip" % self.country)

    def run(self):
        r = requests.get('http://download.geonames.org/export/dump/%s.zip' % self.country)
        assert r.status_code == 200
        with self.output().open('w') as f:
            f.write(r.content)


class Geonames(luigi.Task):

    country = luigi.Parameter(default="FR")

    def requires(self):
        return FetchGeonamesZIP(self.country)

    def output(self):
        return luigi.LocalTarget("data/%s.txt" % self.country)

    def run(self):
        with zipfile.ZipFile(self.requires().output().open('r'), 'r') as z:
            z.extractall('data/')

class RunAll(luigi.Task):

    def requires(self):
        for i in ["FR", "US", "GB", "BE", "RU", "CA", "JP", "PT"]:
            yield Geonames(i)

    def complete(self):
        return False

if __name__ == '__main__':
    luigi.run(main_task_cls=RunAll)
