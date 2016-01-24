#!/bin/bash
 sbt clean
 sbt compile
 sbt package
 mv target/scala-2.10/*.jar WA.jar
