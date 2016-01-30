#!/bin/bash

../spark-1.2.0/bin/spark-submit --class "SimpleApp" WA.jar data/hansards.f data/hansards.e 20 alignment.txt
