sudo docker run -it --sysctl net.ipv6.conf.all.disable_ipv6=0 --privileged -p 5678:5678 -p 8000:8000 -v $(pwd)/data:/data -e USERID=$UID otklik-dev
