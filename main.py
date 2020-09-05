from PrimesList import PrimesList

# p now stores the all primes between 0 and 100
print("Primes between 0 and 100:")
p = PrimesList((0, 100))
print(p, "\n")  # > [ 2,  3,  5,  7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]

# p now stores the first through the fifth primes
print("First through the fifth primes:")
p = PrimesList([1, 6])
for prime in p:
    print(prime, end=", ")  # > 3, 5, 7, 11, 13,
print("\n")

# p now stores all fifty million primes. NOTE: This will take over a minute to initially download
# all 50 million primes, but will be much faster on all subsequent runs
print("Every hundredth prime between the one thousandth prime and the two thousandth prime (forwards and backwards):")
p = PrimesList("all")
print(p[1000:2000:100])  # > [ 7927  8837  9739 10663 11677 12569 13513 14533 15413 16411]
print(p[2000:1000:-100], "\n")  # > [17393 16411 15413 14533 13513 12569 11677 10663  9739  8837]

# Preview a PrimesList object
print("PrimesList object preview:")
print(repr(p), "\n")
# > PrimesList object containing prime number 0 to 50000000: [2, 3, 5, 7, ... 982451581, 982451609, 982451629, 982451653]

# Equivalently, just get the head and tail
print("PrimesList contents preview:")
print(p)  # > [        2,         3,         5, ..., 982451609, 982451629, 982451653]
