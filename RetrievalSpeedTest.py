import csv
import json
import h5py
import pstats
import pickle
import cProfile
from pstats import SortKey


def init_objs(data):
    global path
    name = "1mil"
    # Pickling
    with open(path + name + ".p", "wb") as P:
        pickle.dump(data, P)
    # JSON
    with open(path + name + ".json", "w") as JSON:
        json.dump(data, JSON)
    # h5py
    with h5py.File(path + name + ".hdf5", "w") as hdf:
        hdf.create_dataset("Primes", (100000, ), dtype="i", data=data)


def unlist():
    global path
    with open(path + "1.csv", "r") as f:
        return [int(prime) for prime in f.readline().split(",")]


def unpickle():
    global path
    with open(path + "1.p", "rb") as p:
        return pickle.load(p)


def unjson():
    global path
    with open(path + "1.json", "r") as JSON:
        return json.load(JSON)


def uncsv():
    global path
    with open(path + "1.csv", "r") as CSV:
        reader = csv.reader(CSV)
        return [int(p) for p in list(reader)[0]]


def unhdf():
    global path
    with h5py.File(path + "1.hdf5", "r") as hdf:
        return hdf["Primes"][()]


def run_all():
    global path
    # print("List:\t")
    unlist()

    # print("Pickle:\t")
    unpickle()

    # print("JSON:\t")
    unjson()

    # print("CSV:\t")
    uncsv()

    # print("HDF:\t")
    unhdf()


if __name__ == "__main__":
    path = "./Speed Tests/"
    init_objs(unlist())

    cProfile.run("run_all()", "runtime")
    p = pstats.Stats('runtime').strip_dirs()
    p.sort_stats(SortKey.CUMULATIVE).print_stats(100)  # Results: HDF, Pickle, list, csv, json
    # p.sort_stats(SortKey.TIME).print_stats(100) # Results: HDF, Pickle, json, list, csv
    # Note: If I'm reading this correctly, HDF is about twice as fast as pickling,
    # which is at least three time faster than everything past that.

    # Given these numbers, I will construct a virtual dataset of the larger files for use with HDF!
