import os


class PrimesList:
    MAX_PRIME = 982451653
    prime_ranges = {1: (2, 15485863),
                    2: (15485867, 32452843),
                    3: (32452867, 49979687),
                    4: (49979693, 67867967),
                    5: (67867979, 86028121),
                    6: (86028157, 104395301),
                    7: (104395303, 122949823),
                    8: (122949829, 141650939),
                    9: (141650963, 160481183),
                    10: (160481219, 179424673),
                    11: (179424691, 198491317),
                    12: (198491329, 217645177),
                    13: (217645199, 236887691),
                    14: (236887699, 256203161),
                    15: (256203221, 275604541),
                    16: (275604547, 295075147),
                    17: (295075153, 314606869),
                    18: (314606891, 334214459),
                    19: (334214467, 353868013),
                    20: (353868019, 373587883),
                    21: (373587911, 393342739),
                    22: (393342743, 413158511),
                    23: (413158523, 433024223),
                    24: (433024253, 452930459),
                    25: (452930477, 472882027),
                    26: (472882049, 492876847),
                    27: (492876863, 512927357),
                    28: (512927377, 533000389),
                    29: (533000401, 553105243),
                    30: (553105253, 573259391),
                    31: (573259433, 593441843),
                    32: (593441861, 613651349),
                    33: (613651369, 633910099),
                    34: (633910111, 654188383),
                    35: (654188429, 674506081),
                    36: (674506111, 694847533),
                    37: (694847539, 715225739),
                    38: (715225741, 735632791),
                    39: (735632797, 756065159),
                    40: (756065179, 776531401),
                    41: (776531419, 797003413),
                    42: (797003437, 817504243),
                    43: (817504253, 838041641),
                    44: (838041647, 858599503),
                    45: (858599509, 879190747),
                    46: (879190841, 899809343),
                    47: (899809363, 920419813),
                    48: (920419823, 941083981),
                    49: (941083987, 961748927),
                    50: (961748941, 982451653)}
    path = "./Primes/"

    def __init__(self, prime_range="", path="./Primes/"):
        """
        :param prime_range: Whether to download all 50 million primes (~500mb)
                            "all" - download all primes
                            "none", None, "" - don't download any primes
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
                reload_range = (1, 50)
            elif prl == "none" or prl == "" or prl is None:
                return
            else:
                err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
                raise ValueError(err)
        elif type(prime_range) is tuple:
            reload_range = PrimesList.classify_prime(prime_range)
        elif type(prime_range) is list:
            reload_range = [int(i / (1 * 1000 * 1000 + 1)) + 1 for i in prime_range]
        else:
            err = str(prime_range) + " is not a valid prime_range. ['all', 'none', None, '', (a, b), [c, d]]"
            raise ValueError(err)
        PrimesList.download_primes(rng=PrimesList._get_reloads(rng=reload_range), path=path)

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
        pass

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
