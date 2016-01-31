#!/bin/bash

../spark-1.2.0/bin/spark-submit --class "IBM1" --driver-memory 25G WA.jar data/hansards.f data/hansards.e 5 alignment.txt
