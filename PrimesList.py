import os
import h5py
import requests


class PrimesList:
    """
    Returns a sliceable, iterable numpy array of primes

    Class functions:
    All class functions should be accessed using standard syntax for slicing and iterator.
    For debugging purposes, PrimesList exposes the classify_primes() function,
        which determines which HDF dataset contains

    Variables:
    path: Download location
    prime_ranges: primes in each dataset and whether they are stored locally [first, last, local]
    primes: actual virtual dataset
    """

    path = ""
    prime_ranges = None
    primes = None
    initialized = False

    def __init__(self, prime_range="", path=""):
        if not PrimesList.initialized:
            PrimesList._init_PrimesList_(prime_range, path)
            PrimesList.initialized = True
        # Copy classification from _init_PrimesList_ so each instance knows it's limits :)
        self.lower = 0  # TODO: These should be initialized to None, once __init__ can handle [c, d]
        self.upper = 15
        self.curr = None


    @classmethod
    def _init_PrimesList_(cls, prime_range="", path=""):
        """
        :param prime_range: Whether to download all 50 million primes
                            "all" - download all primes to local storage (~200 Mb)
                            "none", None - don't download any primes initially (default)
                            (a, b) - download primes between a and b (inclusive)
                            [c, d] - download the c-th prime through the d-th prime (inclusive)
        :param path: Path to keep fetched primes locally. False or None to discard values on completion
        """
        if path == "":  # Determining where to store the primes
            if cls.path == "":
                cls.path = "./Primes/"
        elif cls.path != "":
            raise ValueError("Changing path between PrimeList instances will cause duplicate files "
                             "and wasted space.")
        else:
            cls.path = "./" + path.strip("./ ") + "/"

        # Breakdown of starting and ending prime for each of the 500 chunks and whether they have been downloaded
        with open("PrimeRanges50.csv", "r") as prime_rngs:
            cls.prime_ranges = {i: [int(line.split(",")[0]), int(line.split(",")[1].strip("\n")), False]
                                for i, line in enumerate(prime_rngs.readlines())}

        # Update which sets of primes are already downlaoded (size > 1 Mb)
        for i in range(50):
            if os.path.getsize(cls.path + f"{i}.h5") > 1000 * 1000:
                cls.prime_ranges[i][2] = True

        # Initialization of the virtual dataset (no numbers, just placeholder 0's if not downloaded)
        cls._init_virtual_dataset(cls.path)

        # Determine primes to download
        prime_sets = cls._parse_range(prime_range)

        # Downloading any appropriate primes
        cls._download_primes(prime_sets, cls.path)

        # Open the HDF Virtual Dataset
        cls.primes = cls._open_hdf()["Primes"][0]

    @classmethod
    def _parse_range(cls, prime_range) -> list:
        """
        Determine which chunks need downloading
        :param prime_range: str, list, tuple, None, according to constructor definition
        :return: list of primes sets which need to be downloaded
        """
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
        elif type(prime_range) is tuple:    # (a, b) -> download primes between a and b (inclusive)
            bounds = cls.classify_primes(prime_range)
            reload_range = range(bounds[0], bounds[1] + 1)
        elif type(prime_range) is list:     # [a, b] -> download the c-th prime through the d-th prime (inclusive)
            bounds = [int(i / (1000 * 1000)) for i in prime_range]
            reload_range = range(bounds[0], bounds[1] + 1)
        elif prime_range is None:
            reload_range = []
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        # Only download sets that aren't already downloaded
        return [r for r in reload_range if cls.prime_ranges[r][2] is False]

    @classmethod
    def _download_primes(cls, rng, path) -> None:
        if not os.path.exists(path) and rng != []:
            os.makedirs(path)
        for i in rng:
            url = r"https://github.com/ZacharyPH/PrimeNumbers/blob/master/HDF/" + str(i) + ".h5?raw=true"
            try:
                r = requests.get(url)
            except requests.exceptions.MissingSchema:
                print("The supplied URL is invalid. Please Report this in GitHub Issues.")
                raise Exception("InvalidURL")
            with open(path + str(i) + ".h5", "wb") as out_file:
                out_file.write(r.content)
                # print("Successfully written:", path + str(i) + ".h5")
            cls.prime_ranges[i][2] = True
            # print(f"Downloaded {i} million primes")

    @classmethod
    def classify_primes(cls, primes) -> list:
        """
        Determine which prime package a prime is a part of
        :param primes: List of primes, string, or int
        :return: list of prime package categorizations
        """
        #   Input classification, string
        if type(primes) is str:
            try:
                primes = int(primes)
            except ValueError:
                err = str(primes) + " cannot be classified as it cannot be converted to an integer"
                raise ValueError(err)
        #   Input classification, list
        if type(primes) is list:
            try:
                primes = (int(prime) for prime in primes)
            except ValueError:
                err = str(primes) + " cannot be classified as it cannot be converted to an integer"
                raise ValueError(err)
        #   Input classification, integer
        if type(primes) is int:
            primes = [primes]

        cats = []  # Prime classifications
        for prime in primes:
            for index, rng in cls.prime_ranges.items():
                if prime <= rng[1]:
                    cats.append(index)
                    break
        return cats

    @classmethod
    def _init_virtual_dataset(cls, hdf_path):
        layout = h5py.VirtualLayout(shape=(1, 50 * 1000 * 1000), dtype="u4")
        if not os.path.exists(hdf_path):
            os.mkdir(hdf_path)
        for n in range(50):
            path = hdf_path + f"{n}.h5"
            if not os.path.exists(path):
                f = h5py.File(hdf_path + f"{n}.h5", "w")
                f.create_dataset("Primes", shape=[1, 1000 * 1000])
            else:
                f = h5py.File(hdf_path + f"{n}.h5", "r")
            layout[0, n * 1000 * 1000:(n + 1) * 1000 * 1000] = h5py.VirtualSource(f["Primes"])

        with h5py.File(hdf_path + "Primes.h5vd", "w") as f:
            f.create_virtual_dataset("Primes", layout)

    @classmethod
    def _open_hdf(cls) -> h5py.File:
        return h5py.File(cls.path + "Primes.h5vd", "r")

    def __iter__(self, s):
        print("__iter__")
        if self.curr is None:
            if s.start is None:
                self.curr = self.lower - 1
            else:
                self.curr = s.start - 1
        if s.stop is None:
            stop = self.upper
        else:
            stop = s.stop
        if s.step is None:
            step = 1
        else:
            step = s.step
        return self

    def __next__(self):
        print("__next__")
        self.curr += 1
        if self.curr <= self.upper:
            return PrimesList.primes[self.curr // (1000 * 1000)][self.curr % (1000 * 1000)]
        else:
            return StopIteration

    def __getitem__(self, s):
        print("__getitem__")
        if s.start is None:
            start = self.lower
        else:
            start = s.start
        if s.stop is None:
            stop = self.upper
        else:
            stop = s.stop
        if s.step is None:
            step = 1
        else:
            step = s.step
        # Add a check for whether the slice falls within the accepted (initialized) range for the instance
        # What probably needs to happen is that each new instance gets bounds
        # When a new instance is created, it defaults to having no numbers and not being iterable
        # If it is given a range, that range is re-initialized to make sure everything is downloaded
        # Q: Do I want to iterate by primes or prime number? Probably number, so I can use the below return...
        # A: Yep, that makes sense because the slice will be in terms of number, rather than prime.
        return PrimesList.primes[start:stop:step]

    # TODO: Figure out conditions requiring clearing out the downloads
    @classmethod
    def __del__(cls):
        pass

# TODO: PLAN
#       1a. _init_PrimesList_() sets the storage path, stores the ranges (and False on each to indicate they're not local)
#       1b. _init_PrimesList_() then creates Primes.h5vd, the virtual dataset, and downloads any necessasry primes
#       2. Define __getitem__() to allow arbitrary slicing and iteration
