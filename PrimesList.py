import os
import h5py
import requests
from numpy import array2string


class PrimesList:
    """
    Returns a sliceable, iterable numpy array of primes

    Class functions:
    All class functions should be accessed using standard syntax for slicing and iterator.
    For debugging purposes, PrimesList exposes the classify_primes() function,
        which determines which HDF dataset contains
    find_next_prime(n) returns (index, prime) of the smallest prime larger than n

    Variables:
    path: Download location
    prime_ranges: primes in each dataset and whether they are stored locally [first, last, local]
    primes: actual virtual dataset
    """

    path = ""
    prime_ranges = None
    primes = None
    initialized = False

    def __init__(self, prime_range="", path="") -> None:
        """
        Instance constructor, sets accessible range for the object. Initializes the class once
        :param prime_range: Prime numbers which should be accessible
        :param path: Where to download primes. Default = 'PrimesList/Primes/...'
        """
        if not PrimesList.initialized:
            PrimesList._init_PrimesList_(prime_range, path)
            PrimesList.initialized = True

        if type(prime_range) is list:
            self.lower, self.upper = prime_range
        elif type(prime_range) is tuple:
            self.lower = PrimesList.find_next_prime(prime_range[0])[0]
            self.upper = PrimesList.find_next_prime(prime_range[1])[0]
        elif type(prime_range) is str:
            if prime_range == "all":
                self.lower, self.upper = (0, 50 * 1000 * 1000)
            if prime_range == "none" or prime_range == "" or prime_range is None:
                # Is it odd behavior to have 'none' give access to the first set(s) of downloaded primes?
                # Seems strange, but this object would have no use for access otherwise, so...
                pass
        self.curr = None    # Updated once the object has been used in iteration
        PrimesList._download_primes(PrimesList._parse_range([self.lower, self.upper]))

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

        # Initialization of the virtual dataset (no numbers, just placeholder 0's if not downloaded)
        cls._init_virtual_dataset(cls.path)

        # Update which sets of primes are already downlaoded (size > 1 Mb)
        for i in range(50):
            if os.path.getsize(cls.path + f"{i}.h5") > 1000 * 1000:
                cls.prime_ranges[i][2] = True

        # Determine primes to download
        prime_sets = cls._parse_range(prime_range)

        # Downloading any appropriate primes
        cls._download_primes(prime_sets, cls.path)

        # Open the HDF Virtual Dataset
        cls.primes = cls._open_primes_dataset()

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
                reload_range = []
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
            reload_range = range(bounds[0], bounds[1] + 1 if bounds[1] <= 49 else 50)
        elif prime_range is None:
            reload_range = []
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        # Only download sets that aren't already local
        return [r for r in reload_range if cls.prime_ranges[r][2] is False]

    @classmethod
    def _download_primes(cls, rng, path="") -> None:
        """
        Fetches each prime set in rng from GitHub and saves it locally
        :param rng: Sets to download
        :param path: Save location, defaults to cls.path
        :return: None (files downloaded)
        """
        if path == "":
            path = cls.path
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
    def find_next_prime(cls, n) -> tuple:
        """
        Finds the smallest prime larger than n
        :param n: number to find prime close to
        :return: (index, prime)
        """
        prime_set = cls.classify_primes(n)
        if cls.prime_ranges[prime_set[0]][2] is False:
            cls._download_primes(prime_set)
        curr = prime_set[0] * 1000 * 1000
        p = 0
        while (p := cls.primes[curr]) < n:
            curr += 1
        return curr, p

    @classmethod
    def _init_virtual_dataset(cls, hdf_path) -> None:
        """
        Construct the virtual dataset from local and non-local files
        :param hdf_path: Prime dataset location
        :return: None (creates file)
        """
        layout = h5py.VirtualLayout(shape=(1, 50 * 1000 * 1000), dtype="u4")
        if not os.path.exists(hdf_path):
            os.mkdir(hdf_path)
        for n in range(50):
            path = hdf_path + f"{n}.h5"
            if not os.path.exists(path):
                f = h5py.File(hdf_path + f"{n}.h5", "w")
                f.create_dataset("Primes", shape=[1, 1000 * 1000], dtype="u4")
            else:
                f = h5py.File(hdf_path + f"{n}.h5", "r")
            layout[0, n * 1000 * 1000:(n + 1) * 1000 * 1000] = h5py.VirtualSource(f["Primes"])

        with h5py.File(hdf_path + "Primes.h5vd", "w") as f:
            f.create_virtual_dataset("Primes", layout)

    @classmethod
    def _open_primes_dataset(cls) -> h5py.File:
        """
        Opens the 'Primes' virtual dataset
        :return: numpy array of all primes, including placeholder 0's for non-downloaded sets
        """
        return h5py.File(cls.path + "Primes.h5vd", "r")["Primes"][0]

    def __iter__(self):
        """
        Initializes iteration
        :return: self
        """
        self.curr = self.lower - 1
        return self

    def __next__(self):
        """
        Defines iteration
        :return: next prime in range
        """
        self.curr += 1
        if self.curr < self.upper:
            return PrimesList.primes[self.curr]
        else:
            raise StopIteration

    def __getitem__(self, s):  # -> np.array
        """
        p[start:stop:step]; __getitem__ supports all normal slicing syntax. Returns a slice of the prime numbers
        p[stop] -> array from self.lower to stop
        p[start, stop[, step]] -> array from start to stop, optionally in increments of step
        :param s: slice, in the form slice([start = self.lower], [stop = self.upper], [step = 1])
        :return: numpy array of primes
        """
        if s.start is None:
            start = self.lower
        elif s.start < self.lower:
            msg = f"Start {s.start} is out of bounds for {self}, which has bounds ({self.lower}, {self.upper})"
            raise ValueError(msg)
        else:
            start = s.start
        if s.stop is None:
            stop = self.upper
        elif s.stop > self.upper:
            msg = f"Stop {s.stop} is out of bounds for {self}, which has bounds ({self.lower}, {self.upper})"
            raise ValueError(msg)
        else:
            stop = s.stop
        if s.step is None:
            step = 1
        else:
            step = s.step
        return PrimesList.primes[start:stop:step]

    def __str__(self):
        return array2string(self[(slice(None))], max_line_width=1_000_000, separator=", ")

    def __repr__(self):
        head = array2string(self[slice(self.lower + 4)], separator=", ")[:-1]
        tail = array2string(self[slice(self.upper - 4, self.upper)], separator=", ")[1:]
        return f"PrimesList object containing prime number {self.lower} to {self.upper}: {head}, ... {tail}"
