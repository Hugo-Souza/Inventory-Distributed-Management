#!/usr/bin/env bash

exec python fabrica.py -n 1 -p 1 2 3 4 5&
exec python centro_distribuicao.py &
exec python loja.py -n 1 