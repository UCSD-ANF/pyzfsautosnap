pyzfsautosnap
=============

ZFS Automatic Snapshots for Linux and Solaris

This is a Python implementation of ZFS autosnap which was written for
OpenSolaris.

TODO
----
* Add snapshot syncronization as client/server
** use mbuffer for the sync stuff, SSH wrapper or something for setup of stream

mbuffer
-------

http://everycity.co.uk/alasdair/2010/07/using-mbuffer-to-speed-up-slow-zfs-send-zfs-receive/

The receiver does a giant read per TXG. Between these reads, the standard TCP
network buffers fill and flow control off. When the disks and the network are
similar bandwidths, you will see alternate network, then disk activity. By
having a buffer on the receive side which can hold 5 seconds worth of data at
the rate the disk and network can stream, you will then see the dataflow stream
continuously. I didn’t see any significant benefit of a buffer at the sender,
but a small one can’t do any harm.

If you are using mbuffer on a local replication it’s both tedious, complicated
and unnecessary to use two copies running on the same machine. Try this syntax:

    zfs send ... | mbuffer -s 128k -m 2G -o - | zfs receive ...

the “-o -” puts mbuffer into file output mode, but then sends to stdout
instead. Fancy!

paramiko
--------

http://hackerific.net/2009/02/06/paramiko-scripting-ssh-with-python/
http://www.paramiko.org/
