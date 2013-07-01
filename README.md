Cabinet is an simple,light-weight key-value storage with high performance.



=== What Is Cabinet Used For? ===


Cabinet is an simple, light-weight key-value storage service, it store all binary value data on disk, and keep all keys in a in-memory hash map. So a "get(key)" operation only require one disk access. So reading data from cabinet more effient than other dbs.
If you only have limited memory but need to store relative huge value data, and you want good reading performance, cabinet fits.


=== How To Build And Install? ===


Cabinet use SCons(http://www.scons.org) as building tool.

Change into the repository directory, to build:

  scons
  
To install cabinetd in /usr/local:

  sudo scons install
  
Run as daemon job:

  cabinetd --daemon
