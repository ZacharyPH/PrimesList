from PrimesList import PrimesList
# from PrimesListHDF import PrimesListHDF

p = PrimesList([8345679, 11561238])
for prime in p[0:200:15]:
    print(prime, end=", ")
