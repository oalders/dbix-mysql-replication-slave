
DBIx::MySQL::Replication::Slave issues a "SHOW SLAVE STATUS" query and
returns the results to you as a HASHREF. It also includes the slave_ok()
method, which is a handy shortcut to see whether your slave server
requires any special attention. It doesn't do anything you can't already
do for yourself, but it makes it just a little bit quicker to check on
the health of your slaves.

Dist::Zilla is used to build the distribution.  So, if you'd like to
build your own, you'll need to install Dist::Zilla and then issue the
following command from the top directory:

dzil build
