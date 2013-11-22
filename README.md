rmq_consume
===========

This is a simple RabbitMQ consumer tool written in Erlang.

Build
-----
You need Erlang and [rebar](https://github.com/basho/rebar).

    $ cd ../rmq-consume
    $ rebar get-deps compile escriptize
    [...]

This should fetch dependencies, compile everything, and create executable escripts.

You can also use `make` to drive the build:

    $ make

Usage
-----

Consuming:

    $ ./rmq_consume -u amqp://guest:guest@192.168.1.1:5672/%2f -q myqueue -d ~/output_dir/

You can also specify a timeout. If no messages are received after it, `rmq_consume` will exit:

    $ ./rmq_consume -u amqp://guest:guest@192.168.1.1:5672/%2f -q myqueue -d ~/output_dir/ -t 60

It is also possible to consume messages but not save them to disk with the --nosave option. Ex:

    $ ./rmq_consume -u amqp://guest:guest@192.168.1.1:5672/%2f -q myqueue -n true

For more information see `-h`/`--help`.
