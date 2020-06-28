import pickle, json


def init_objs(data):
    global path
    # Pickling
    with open(path + "1.p", "wb") as P:
        pickle.dump(data, P)
    # JSON
    with open(path + "1.json", "w") as JSON:
        json.dump(data, JSON)


def csv():
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


if __name__ == "__main__":
    path = "./Speed Tests/"
    d = csv()
    init_objs(d)
    print("CSV:\t", csv()[:10])
    print("Pickle:\t", unpickle()[:10])
    print("JSON:\t", unjson()[1])

