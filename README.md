[![Build Status](https://travis-ci.org/lmdbjava/lmdbjava.svg?branch=master)](https://travis-ci.org/lmdbjava/lmdbjava)
[![Coverage Status](https://coveralls.io/repos/github/lmdbjava/lmdbjava/badge.svg?branch=master)](https://coveralls.io/github/lmdbjava/lmdbjava?branch=master)
[![Dependency Status](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4)
[![License](https://img.shields.io/hexpm/l/plug.svg?maxAge=2592000)](http://www.apache.org/licenses/LICENSE-2.0.txt)
![Size](https://reposs.herokuapp.com/?path=lmdbjava/lmdbjava)

# LmdbJava

LmdbJava provides an extremely low latency
[JNR-FFI](https://github.com/jnr/jnr-ffi)-based binding to the
[LMDB](http://symas.com/mdb/) native library. LMDB is an ultra-fast,
ultra-compact, b-tree ordered, embedded, key-value store developed by Symas for
the OpenLDAP Project.

LMDB uses memory-mapped files, so it has the read performance of a pure in-memory
database while still offering the persistence of standard disk-based databases.
It is transactional with full ACID semantics and crash-proof by design.
No journal files. No corruption. No startup time. No dependencies. No config
tuning. LMDB is the perfect foundation for large, read-centric, single node
workloads that require strong latency and operational robustness outcomes.

## Usage

1. Install `liblmdb` for your platform (eg Arch Linux: `pacman -S lmdb`)
2. Clone this repository and `mvn clean install` (Maven Central coming soon)
3. Add the `lmdbjava` artifact to your project POM
4. Browse the [LMDB Documentation](http://lmdb.tech/doc/) (especially the
   [Getting Started](http://lmdb.tech/doc/starting.html) page)
5. Take a look at our 
   [tests](https://github.com/lmdbjava/lmdbjava/tree/master/src/test/java/org/lmdbjava)
   (the LmdbJava class names and contracts closely match the LMDB C API)

## Support

We're happy to help you use LmdbJava. Simply
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) if you have
any questions.

## Contributing

Contributions are welcome! Simply submit a pull request that's consistent with
the existing coding style and includes an appropriate test. 

For larger changes, please
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) so we can
discuss what you have in mind.

## History

For years Java users have been able to access LMDB via
[LMDBJNI](https://github.com/deephacks/lmdbjni). Its public API is mature and
widely used, but this makes it challenging to implement any substantial changes.

LmdbJava was created to provide a new LMDB abstraction without the backward
compatibility consideration. A separate project also offered a convenient
opportunity to implement many internal changes to reduce latency and long-term
maintenance costs. For example, we moved from HawtJNI to JNR-FFI (for its active
community, lower latency, Java 9 roadmap and much simpler build requirements). We
also significantly reduced and isolated `Unsafe` use, with only a single method
now requiring it (and there is an automatic reflective fallback if `Unsafe` isn't
available). Overall these changes make LmdbJava the optimal choice for projects
targeting server-class JVMs, and it will be easy to support Java 9 when released.

## License

This project is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

You must separately install the LMDB library. LMDB is currently licensed under
[The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).