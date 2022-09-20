#!/bin/bash
echo "Hello World"
read -p "What is your Name? " name
echo "Hello $name"
echo "Script PID $$"
array=(first second third fourth fifth sixth seventh)
echo "Whats 9x9"
read answer
if [ $answer = 81 ]
then
  echo "correct 9x9=81"
else
  echo "wrong 9x9=81"
fi
echo "<h1>Hello from Host</h1>" > ./target/index.html
docker run -it --rm --name nginx -p 8080:80 -v "$(pwd)"/target:/usr/share/nginx/html nginx
echo ${array[@]:2:$#array[@]}