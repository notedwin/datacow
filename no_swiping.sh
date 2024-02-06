#!/bin/bash
cut -d= -f2 .env | tr -d '"' > sample
rg -f sample -g '!sample'
rm sample