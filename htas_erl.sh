#!/bin/bash

cd /Users/gyanendraaggarwal/erlang/code/erlang_htas

erl -sname $1 -pa ./ebin -config ./sys
