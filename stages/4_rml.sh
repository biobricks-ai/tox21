#!/usr/bin/env bash

morph-kgc morph-kgc.ini
mkdir -p rdf
rdf2hdt rdf/tox21.nt rdf/tox21.hdt
rm -f rdf/tox21.nt
