#!/bin/sh

if [ -z "${SAG}" ]; then
   return
fi

. ${SAG}/bin/sagenv.new

for i in 23 24; do
   adaopr db=$i disp=uq
   adaopr db=$i disp=com
   adaopr db=$i reset=com
   adarep db=$i cont
#   adarep db=$i lay
#   adaopr db=$i disp=cq
#   adaopr db=$i disp=hq
done
