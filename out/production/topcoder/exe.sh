#!/bin/sh

source /etc/profile

. JDK jdk1.8.0_144

#echo "status:running" >> coder_info

java -Xmx4G MainFrame $1 $2 $3 $4 > exe_result 2>&1
