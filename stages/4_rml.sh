#!/usr/bin/env bash

mkdir -p rdf
clojure -M:rdf rdf/tox21.nt
rdf2hdt rdf/tox21.nt rdf/tox21.hdt
rm -f rdf/tox21.nt
