iptables -A OUTPUT -o eth0 -m owner --uid-owner 500 -j DROP
iptables -A OUTPUT -o tun0 -m owner --uid-owner 500 -j DROP
iptables -A OUTPUT -o lo -j ACCEPT
iptables -A INPUT -i lo -j ACCEPT
