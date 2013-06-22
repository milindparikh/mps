#!/bin/sh 
# Retrieve dependenices
set +e
make rel 2>&1 || true
chmod -R 777 deps
set -e
sed -e 's/p3/p5/g' < deps/webmachine/rebar.config > deps/webmachine/rebar.config.tmp
mv deps/webmachine/rebar.config.tmp deps/webmachine/rebar.config
rm -r deps/mochiweb
make rel
cp -r apps/mps/src/priv rel/mps
#mkdir rel/mps/lib/mps-1
cp -r apps/mps/src/priv rel/mps/lib/mps-1
#./rel/mps/bin/mps console
