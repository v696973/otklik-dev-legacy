## Run

```sh
./build.sh
./otklik.sh
```
## Add peer

1. Run application and find your addresses in output
````
2018-11-17 09:43:28,264 INFO: INIT | Tor: IPv6 Addres is: fd87:d87e:eb43:cd00:d388:5681:efb1:9977
2018-11-17 09:43:28,332 INFO: INIT | IPFS: Peer ID: QmS6ZdhgUoUcenprcyYwTk5i5m4CNEuTH6QTCh61xa5QBm
````

2. Give values to your boys

3. Stop application

3. Edit `data/config/otklik_config.json`

4. Add new entry in list of peers with format:

`"/ip6/{{ IPv6 }}/tcp/4001/ipfs/{{ Peer ID }}"`

like

`"/ip6/fd87:d87e:eb43:10c0:f0e0:1060:faf7:a836/tcp/4001/ipfs/Q1a78eHYSvEmTNs1cRN81hkd9tTk8DvJXF179Ae2a2DGwt"`

5. Run application

---

Source code of Otklik-dev, a decentralized port scanner prototype, developed by BlackNode team in 2018.

**WARNING! IN ITS CURRENT STATE, THIS SOFTWARE SHOULDN'T BE USED AS IS AND IS PROVIDED FOR REFERENCE USE ONLY!**

Licensed under CC-BY-NC-SA 4.0
