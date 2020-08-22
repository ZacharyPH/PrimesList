# PythonPrimes
A Python module to provide extremely fast access to pre-computed primes at https://github.com/ZacharyPH/PrimeNumbers

### HDF
This module employs the HDF file format, which you can learn about here: https://www.hdfgroup.org/solutions/hdf5/

The HDF group provides the h5py module for ease of use  at https://github.com/h5py/h5py

### Implementation
The HDF format was chosen for two primary reasons: speed and size.

* Size: the storage size of all 50 million primes is only 200Mb, a mere 40% of the CSV equivalent. *

* Speed: the HDF format sports retrieval speeds nearly double that of pickling, which is itself faster than the built-in csv.reader, JSON, and manual file manipulation

\* Additionally, the module downloads primes in chunks of 1,000,000, so the actual space usage is about 

<img src="https://render.githubusercontent.com/render/math?math=4Mb \times \frac{Prime\ Reference\ Range}{1000000}">

### Use
Import the package as follows: ```from PrimesList import PrimesList```

From there, you can initialize objects and print them:
```
p = PrimesList((0, 100))  # p now stores the all primes between 0 and 100
print(p)  # > 2  3  5  7 11 13 17 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97
```
PrimesList also supports iteration:
```
p = PrimesList([1, 6])  # p now stores the first through the fifth primes
for prime in p:
    print(p, end=", ")  # > 3, 5, 7, 11, 13, 
```
And slicing:
```
p = PrimesList("all")  # p now stores all fifty million primes
print(p[1000:2000:100])  # > [ 7927  8837  9739 10663 11677 12569 13513 14533 15413 16411]
```

### License
PrimesList employs the same MIT license as [UTM Primes](https://primes.utm.edu),
from where these primes are taken. You can view the license [here](https://github.com/ZacharyPH/PrimesList/blob/master/LICENSE)
and the GitHub Source [Here](https://github.com/ZacharyPH/PrimeNumbers).

You are welcome to use this module commercially or personally with attribution.