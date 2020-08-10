import os
import h5py
import requests


class PrimesList:
    """
    # TODO: Fill out
    """

    path = ""
    prime_ranges = None

    @classmethod
    def __init__(cls, prime_range="", path=""):
        """
        :param prime_range: Whether to download all 50 million primes
                            "all" - download all primes to local storage (~200 Mb)
                            "none", None - don't download any primes initially (default)
                            (a, b) - download primes between a and b (inclusive)
                            [c, d] - download the c-th prime through the d-th prime (inclusive)
        :param path: Path to keep fetched primes locally. False or None to discard values on completion
        """
        if path == "":  # Determining where to store the primes
            if PrimesList.path == "":
                PrimesList.path = "./Primes/"
        elif PrimesList.path != "":
            raise ValueError("Changing path between PrimeList instances will cause duplicate files "
                             "and wasted space.")
        else:
            PrimesList.path = "./" + path.strip("./ ") + "/"

        # Breakdown of starting and ending prime for each of the 500 chunks and whether they have been downloaded
        with open("PrimeRanges50.csv", "r") as prime_rngs:
            cls.prime_ranges = {i: [int(line.split(",")[0]), int(line.split(",")[1].strip("\n")), False]
                                for i, line in enumerate(prime_rngs.readlines())}

        for i in range(50):
            if os.path.getsize(PrimesList.path + f"{i}.h5") > 1000 * 1000:
                cls.prime_ranges[i][2] = True

        # Initialization of the virtual dataset (no numbers, just placeholders - all 0)
        cls._init_virtual_dataset(PrimesList.path)

        # Downloading any appropriate primes
        PrimesList._init_prime_list(prime_range, PrimesList.path)

    @classmethod
    def _init_prime_list(cls, prime_range, path) -> None:
        # Determine which chunks need downloading
        if type(prime_range) is str:
            if (prl := prime_range.lower()) == "all":
                reload_range = range(50)
            elif prl == "none" or prl == "" or prl is None:
                return
            else:
                err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
                raise ValueError(err)
        elif len(prime_range) >= 3:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        elif type(prime_range) is tuple:
            bounds = PrimesList.classify_primes(prime_range)
            reload_range = range(bounds[0], bounds[1] + 1)
        elif type(prime_range) is list:
            bounds = [int(i / (1000 * 1000)) for i in prime_range]
            reload_range = range(bounds[0], bounds[1] + 1)
        elif prime_range is None:
            return
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        reload = [r for r in reload_range if cls.prime_ranges[r][2] is False]
        PrimesList.download_primes(rng=reload, path=path)

    @classmethod
    def download_primes(cls, rng, path) -> None:
        if not os.path.exists(path) and rng != []:
            os.makedirs(path)
        for i in rng:
            url = r"https://github.com/ZacharyPH/PrimeNumbers/blob/master/HDF/" + str(i) + ".h5?raw=true"
            try:
                r = requests.get(url)
            except requests.exceptions.MissingSchema:
                print("The supplied URL is invalid. Please update and run again.")
                raise Exception("InvalidURL")
            with open(path + str(i) + ".h5", "wb") as out_file:
                out_file.write(r.content)
                print("Successfully written:", path + str(i) + ".h5")
            cls.prime_ranges[i][2] = True
            print(f"Downloaded {i} million primes")

    @classmethod
    def classify_primes(cls, primes) -> list:
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
    def _init_virtual_dataset(cls, hdf_path):
        layout = h5py.VirtualLayout(shape=(50, 1000 * 1000), dtype="u4")
        if not os.path.exists(hdf_path):
            os.mkdir(hdf_path)
        for n in range(50):
            path = hdf_path + f"{n}.h5"
            if not os.path.exists(path):
                f = h5py.File(hdf_path + f"{n}.h5", "w")
                f.create_dataset("Primes", shape=[1, 1000 * 1000])
            else:
                f = h5py.File(hdf_path + f"{n}.h5", "r")
            layout[n] = h5py.VirtualSource(f["Primes"])

        with h5py.File(hdf_path + "Primes.h5vd", "w") as f:
            f.create_virtual_dataset("Primes", layout)

    @classmethod
    def open_hdf(cls) -> h5py.File:
        return h5py.File("./HDF/" + "Primes" + ".h5vd", "r")

    def __iter__(self):
        # self.curr_index
        return self

    # def __next__(self):
    #     self.curr_index += 1
    #     next = PrimesList.classify_primes(self.curr_index)[0]
    #     list = PrimesList._open_local(next)[0]
    #     self.curr_prime = list[self.curr_index % 100000]
    #     return self.curr_prime

    @classmethod
    def __del__(cls):
        if PrimesList.path == "" or PrimesList.path is False or PrimesList.path is None:
            os.rmdir("./tmp_primes")
        # for local in cls.local.values():
        #     del local

# TODO: PLAN
#       1a. __init__() sets the storage path, stores the ranges (and False on each to indicate they're not local)
#       1b. __init__() then creates Primes.h5vd, the virtual dataset, and downloads any necessasry primes
#       2. Define __getitem__() to allow arbitrary slicing and iteration
