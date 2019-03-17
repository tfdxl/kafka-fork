#!/bin/sh
echo "start building ..."
echo "start cleaning ..."
./gradlew clean
echo "end cleaning ..."
./gradlew releaseTarGz
echo "end building ..."
