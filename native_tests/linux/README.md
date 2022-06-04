This directory contains some tests from the original [redis](https://github.com/redis/redis)

These tests cover a significant part of the functionality of redis

To run these tests, first you need to make sure that the following conditions are met:

* `tcl-tls`installed
* The necessary certificates have been generated

Certificates can be generated using:

    % ./gen-test-certs.sh
    
When all the conditions are met, run the tests with the following command:

    % ./runtest --host <host> --port <port>
    
For more flexible testing, see [original redis tests](https://github.com/redis/redis/tree/7.0/tests)