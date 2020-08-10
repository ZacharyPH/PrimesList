# PythonPrimes
A Python module to provide extremely fast access to pre-computed primes at https://github.com/ZacharyPH/PrimeNumbers

### HDF
This module employs the HDF file format, which you can learn about here: https://www.hdfgroup.org/solutions/hdf5/

The HDF group provides the h5py module for ease of use  at https://github.com/h5py/h5py

### Implementation
The HDF format was chose for two primary reasons: speed and size.

* Size: the storage size of all 50 million primes is only 200Mb, a mere 40% of the CSV equivalent. *

* Speed: the HDF format sports retrieval speeds nearly double that of pickling, which is itself faster than the built-in csv.reader, JSON, and manual file manipulation

\* Additionally, the module downloads primes in chunks of 1,000,000, so the actual space usage is about 

<img src="https://render.githubusercontent.com/render/math?math=4Mb \times \frac{Prime\ Reference\ Range}{1000000}">