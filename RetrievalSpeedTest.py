import pickle, json, csv


def init_objs(data):
    global path
    # Pickling
    with open(path + "1.p", "wb") as P:
        pickle.dump(data, P)
    # JSON
    with open(path + "1.json", "w") as JSON:
        json.dump(data, JSON)


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


if __name__ == "__main__":
    path = "./Speed Tests/"
    d = unlist()
    init_objs(d)
    print("List:\t", unlist()[:10])
    print("Pickle:\t", unpickle()[:10])
    print("JSON:\t", unjson()[:10])
    print("CSV:\t", uncsv()[:10])

