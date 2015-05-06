#!/bin/bash

# Join arguments in a string. But not quite like you'd expect... see next line.
printf -v var "'%s', " "$@"

# Remove trailing ", "
var=${var%??}

./gradlew run -PappArgs="[$var]"
