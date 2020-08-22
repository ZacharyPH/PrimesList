from PrimesList import PrimesList

# p now stores the all primes between 0 and 100
p = PrimesList((0, 100))
print(p)  # > 2  3  5  7 11 13 17 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97

# p now stores the first through the fifth primes
p = PrimesList([1, 6])
for prime in p:
    print(p, end=", ")  # > 3, 5, 7, 11, 13,

# p now stores all fifty million primes. NOTE: This will take several seconds to download
# all 50 million primes, but will be much faster on all subsequent runs
p = PrimesList("all")
print(p[1000:2000:100])  # > [ 7927  8837  9739 10663 11677 12569 13513 14533 15413 16411]
