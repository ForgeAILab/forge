#!/bin/sh
# Shared Unix script writer for Rust and Python fixtures. The caller owns a
# private staging directory, then fsyncs, chmods and atomically publishes the
# file after this process exits. Never open the executable inode in the parent
# for writing: another thread's fork could inherit it and cause Linux ETXTBSY.
umask 077
exec /bin/cat > "$1"
