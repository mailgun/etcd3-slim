# etcd3-slim

[![Build Status](https://travis-ci.org/mailgun/etcd3-slim.svg?branch=master)](https://travis-ci.org/mailgun/etcd3-slim)
[![Coverage Status](https://coveralls.io/repos/mailgun/etcd3-slim/badge.svg?branch=master&service=github)](https://coveralls.io/github/mailgun/etcd3-slim?branch=master)

Thin wrapper around Etcd3 gRPC stubs


##### gRPC Limitations
Due to a limitation in the C binding which python uses to make gRPC calls you
may not interact with etcd3-slim after forking. This includes any use of the
`multiprocess` module for preforming asynchronous CPU intensive work. To use
etcd3-slim in an multiprocess environment correctly `fork()` must be called
*before* etcd3-slim is initialized and gRPC connections have been established.

If you do interact with etcd3-slim after forking you might see the following error
```
E0820 22:02:35.823169413   14446 ssl_transport_security.cc:470] Corruption detected.
E0820 22:02:35.823242229   14446 ssl_transport_security.cc:446] error:1e000065:Cipher functions:OPENSSL_internal:BAD_DECRYPT
E0820 22:02:35.823256466   14446 ssl_transport_security.cc:446] error:1000008b:SSL routines:OPENSSL_internal:DECRYPTION_FAILED_OR_BAD_RECORD_MAC
E0820 22:02:35.823261537   14446 secure_endpoint.cc:181]     Decryption error: TSI_DATA_CORRUPTED
E0820 22:02:35.823302859   14446 ssl_transport_security.cc:497] SSL_write failed with error SSL_ERROR_SSL.
```

See https://github.com/grpc/grpc/issues/15334 to follow updates to fork()
support in gRPC.

##### Dockerized Server For Local Development

To build a Docker image with etcd server for local development:

    $ ./build-docker.sh

The image will be named `etcd3-slim-dev`.