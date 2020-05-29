import os
import requests


class PrimesList:
    path = "./Primes/"
    local = []

    def __init__(self, prime_range="", path="./Primes/"):
        """
        :param prime_range: Whether to download all 50 million primes
                            "all" - download all primes to local storage (~470 Mb)
                            "none", None, "" - don't store any primes locally
                            (a, b) - download primes between a and b (inclusive)
                            [c, d] - download the c-th prime through the d-th prime (inclusive)
        :param path: Path to keep fetched primes locally. False, None, or "" to discard values on completion
        """
        PrimesList._init_PrimesList(prime_range, path)
        self.curr_prime = 2
        self.range = prime_range

    @classmethod
    def _init_PrimesList(cls, prime_range, path):
        cls.path = path if type(path) is str and path != "" else "tmp_primes"
        cls.path = "./" + cls.path.strip(". / \\") + "/"

        if type(prime_range) is str:
            if (prl := prime_range.lower()) == "all":
                reload_range = (1, 500)
            elif prl == "none" or prl == "" or prl is None:
                return
            else:
                err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
                raise ValueError(err)
        elif type(prime_range) is tuple:
            reload_range = PrimesList.classify_prime(prime_range)
        elif type(prime_range) is list:
            reload_range = [int(i / (100 * 1000 + 1)) + 1 for i in prime_range]
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        cls.prime_ranges = PrimesList.download_primes(rng=PrimesList._get_reloads(rng=reload_range), path=path)
        cls.MAX_PRIME = cls.prime_ranges[len(cls.prime_ranges)][1]

    @classmethod
    def _get_reloads(cls, min_size=0, rng=range(1, 50)):
        reloads = []
        for i in rng:
            filename = cls.path + str(i) + ".csv"
            if not os.path.isfile(filename) or os.stat(filename).st_size <= min_size:
                reloads.append(i)
        return reloads

    @classmethod
    def download_primes(cls, rng, path):
        print("Downloading:", rng, "into", path)
        ranges = {}
        for i in rng:
            url = r"https://raw.githubusercontent.com/ZacharyPH/PrimeNumbers/master/HundredThousands/" + str(i) + ".csv"
            try:
                r = requests.get(url, stream=True)
            except requests.exceptions.MissingSchema:
                print("The supplied URL is invalid. Please update and run again.")
                raise Exception("InvalidURL")
            content = r.content.strip("\n")
            ranges[i] = (content[0], content[-1])
            cls._write_primes(content, path)
        return ranges

    @classmethod
    def _write_primes(cls, content, path):
        with open(path, "r") as out_file:
            print(*content, sep=",", end="", file=out_file)

    @classmethod
    def classify_prime(cls, primes):
        cats = []
        if type(primes) is str:
            try:
                primes = int(primes)
            except ValueError:
                err = str(primes) + " cannot be classified as it cannot be converted to an integer"
                raise ValueError(err)
        if type(primes) is int:
            primes = [primes]
        for prime in primes:
            for index, rng in cls.prime_ranges.items():
                if prime <= rng[1]:
                    cats.append(index)
                    break
        return cats

    def __del__(self):
        if PrimesList.path == "" or PrimesList.path is False or PrimesList.path is None:
            os.rmdir("./tmp_primes")

# TODO: Write another function for local prime retrieval. Maybe track which ones are downloaded.
# TODO: Cleanup code structure and define class plan
