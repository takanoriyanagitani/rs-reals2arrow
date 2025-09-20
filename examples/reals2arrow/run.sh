#!/bin/sh

printf '0000 803f' | xxd -r -ps > sample1.f32.le.dat
printf '0000 0040' | xxd -r -ps > sample2.f32.le.dat

ENV_INPUT_RAW_REALS_32_LE=./sample1.f32.le.dat ./reals2arrow
ENV_INPUT_RAW_REALS_32_LE=./sample2.f32.le.dat ./reals2arrow
