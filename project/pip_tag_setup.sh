#!/bin/bash

# git tag -a v0.0.1 -m "v0.0.1, first release to prod"

# pip install git+https://github.com/dmerz75/spark2_dfanalysis#egg=dfanalysis
pip install --index-url https://github.com/dmerz75/spark2_dfanalysis \
            -r requirements.txt
            -t ./dfanalysis/ \
            --no-deps --upgrade
