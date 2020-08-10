import os
import h5py
import requests


class PrimesList:
    """
    local: dictionary of integers, values of None, string (path), or file object to primes
    """
    path = ""
    local = dict(zip(list(range(1, 500 + 1)), [None] * 500))
    MAX_PRIME = None
    # Breakdown of starting and ending prime for each of the 500 chunks
    with open("PrimeRanges500.csv", "r") as prime_rngs:
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
        elif prime_range is None:
            reload_range = []
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        PrimesList.download_primes(rng=PrimesList._get_reloads(rng=reload_range), path=path)

    @classmethod
    def _get_reloads(cls, min_size=0, rng=(1, 50)):
        reloads = []
        if not rng:
            return reloads
        for i in range(rng[0], rng[1] + 1):
            filename = cls.path + str(i) + ".csv"
            if not os.path.isfile(filename) or os.stat(filename).st_size <= min_size:
                reloads.append(i)
        return reloads

    @classmethod
    def download_primes(cls, rng, path):
        if not os.path.exists(path) and rng != []:
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
        """
        Determine which prime package a prime is a part of
        :param primes: List of primes, string, or int
        :return: list of prime package categorizations
        """
        cats = []
        if type(primes) is str:
            try:
                primes = int(primes)
            except ValueError:
                err = str(primes) + " cannot be classified as it cannot be converted to an integer"
                raise ValueError(err)
        if type(primes) is list:
            try:
                primes = (int(prime) for prime in primes)
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

    @classmethod
    def _open_local(cls, index):
        result = []
        if type(index) is int:
            index = [index]
        for i in index:
            if type(cls.local[i]) is str:
                cls.local[i] = [int(p) for p in open(cls.path + cls.local[i], "r").readline().split(",")]
            result.append(cls.local[i])
        return result

    @classmethod    # TODO: Current - this function (and the following) was (were) just added. Test!
    def _init_virtual_dataset(cls):
        hdf_path = "./HDF/"
        layout = h5py.VirtualLayout(shape=(50, 1000 * 1000), dtype="u4")
        for n in range(50):
            with h5py.File(hdf_path + f"{n}.h5", "r") as f:
                layout[n] = h5py.VirtualSource(f["Primes"])

        with h5py.File(hdf_path + "Primes.h5vd", "w") as f:
            f.create_virtual_dataset("Primes", layout)

    @classmethod
    def open_hdf(cls):
        return h5py.File("./HDF/" + "Primes" + ".h5vd", "r")

    def __iter__(self):
        # self.curr_index
        return self

    def __next__(self):
        self.curr_index += 1
        next = PrimesList.classify_prime(self.curr_index)[0]
        list = PrimesList._open_local(next)[0]
        self.curr_prime = list[self.curr_index % 100000]
        return self.curr_prime

    @classmethod
    def __del__(cls):
        if PrimesList.path == "" or PrimesList.path is False or PrimesList.path is None:
            os.rmdir("./tmp_primes")
        for local in cls.local.values():
            del local

# TODO: Cleanup code structure and define class plan
# TODO: Comments :)
# TODO: Maybe this class should be divided for clarity
# TODO: Continuation - not sure whether the primes instances should inherit from the parent class. Inheritance is cool!
# TODO: Current - Piece in the virtual datasets with non-downloaded data
# TODO: Cleanup main code to allow simple access, rather than currently defined iterators on a per-instance basis
