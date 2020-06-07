import os
import requests


class PrimesList:
    path = ""
    # cls.path = path if type(path) is str and path != "" else "tmp_primes"
    # cls.path = "./" + cls.path.strip(". / \\") + "/"
    local = dict(zip(list(range(1, 500 + 1)), [None] * 500))
    MAX_PRIME = None
    with open("PrimeRanges.csv", "r") as prime_rngs:
        prime_ranges = {i + 1: (int(line.split(",")[0]), int(line.split(",")[1].strip("\n")))
                        for i, line in enumerate(prime_rngs.readlines())}

    def __init__(self, prime_range="", path=""):
        """
        :param prime_range: Whether to download all 50 million primes
                            "all" - download all primes to local storage (~470 Mb)
                            "none", None, "" - don't store any primes locally
                            (a, b) - download primes between a and b (inclusive)
                            [c, d] - download the c-th prime through the d-th prime (inclusive)
        :param path: Path to keep fetched primes locally. False, None, or "" to discard values on completion
        """
        if path == "":
            if PrimesList.path == "":
                PrimesList.path = "./Primes/"
        elif PrimesList.path != "":
            raise ValueError("Changing path between PrimeList instances will cause duplicate files "
                             "and wasted space.")
        else:
            PrimesList.path = "./" + path.strip("./ ") + "/"
        self.prime_range = prime_range
        PrimesList._init_prime_list(prime_range, PrimesList.path)
        for file in os.listdir(PrimesList.path):
            if (k := int(file.split(".")[0])) in PrimesList.local.keys():
                PrimesList.local[k] = file
        upper_limits = [p[1] for i, p in PrimesList.prime_ranges.items() if PrimesList.local[i] is not None]
        PrimesList.MAX_PRIME = max(upper_limits) if len(upper_limits) >= 1 else None
        self.curr_prime = 2
        self.curr_index = 0
        self.range = prime_range

    @classmethod
    def _init_prime_list(cls, prime_range, path):
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
        PrimesList.download_primes(rng=PrimesList._get_reloads(rng=reload_range), path=path)

    @classmethod
    def _get_reloads(cls, min_size=0, rng=(1, 50)):
        reloads = []
        for i in range(rng[0], rng[1] + 1):
            filename = cls.path + str(i) + ".csv"
            if not os.path.isfile(filename) or os.stat(filename).st_size <= min_size:
                reloads.append(i)
        return reloads

    @classmethod
    def download_primes(cls, rng, path):
        if not os.path.exists(path):
            os.makedirs(path)
        for i in rng:
            url = r"https://raw.githubusercontent.com/ZacharyPH/PrimeNumbers/master/HundredThousands/" + str(i) + ".csv"
            try:
                r = requests.get(url, stream=True)
            except requests.exceptions.MissingSchema:
                print("The supplied URL is invalid. Please update and run again.")
                raise Exception("InvalidURL")
            content = str(r.content, "utf-8").strip("\n")
            cls._write_primes(i, content, path)
        cls.local.update({i: str(i) + ".csv" for i in rng if cls.local[i] is None})

    @classmethod
    def _write_primes(cls, name, content, path):
        with open(path + str(name) + ".csv", "w") as out_file:
            print(content, end="", file=out_file)

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

    def __next__(self):
        self.curr_index += 1
        yield PrimesList.local[self.curr_index - 1]

    def __del__(self):
        if PrimesList.path == "" or PrimesList.path is False or PrimesList.path is None:
            os.rmdir("./tmp_primes")

# TODO: Write another function for local prime retrieval. Maybe track which ones are downloaded.
# TODO: Cleanup code structure and define class plan
