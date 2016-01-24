# erlang_htas
A High Throughput Atomic Store
==============================

Erlang implementation erlang_htas is based on a paper "A High Throughput Atomic Storage Algorithm"
by Rachid Guerraoui, Dejan Kostic, Ron R. Levy and Vivien Quema.

It is an atomic, high throughput, resilient distributed store. It will always be consistent and 
available despite of server failures (CA).

It assumes that a point-to-point communication is always available between all the servers
(partition intolerant). A failure detection mechanism is setup to detect any server failure
and system will continue to work as long as at least one server is available.

Clients can read and write concurrently using any server thus making it a high throughput store.
It also prevents concurrent update on same object to maintain consistency.

It solves the read-inversion problem and prevents any read from returning an old value after
an earlier read returned a new value.

Please refer to accompanying document erlang_replication.pdf for more details.
   
