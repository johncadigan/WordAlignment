#!/bin/bash

  sbt -mem 7000 "run-main main.scala.run_dual data/hansards.f data/hansards.e 20 alignment.txt"
