#!/bin/bash

../spark-1.2.0/bin/spark-submit --class "IBM1" --driver-memory 25G WA.jar hans2000.f hans2000.e 5 alignment.txt
