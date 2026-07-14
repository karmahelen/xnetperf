# xnetperf

xnetperf is a network performance test utility built on the Hearth framework (https://github.com/karmahelen/hearth). Past runs are stored in a SQLite database (xnetperf.db) locally.

Check out the "App Pics" below to see what it looks like and get a sense of what it can do.

I have built many test utilities over the course of my career and performed 1000s of hours of characterization of all major WiFi chipsets and various networking products (routers/switches/gateways/bluetooth devices/IoT devices/smartphones). Leveraging what I have learned, I wanted to build a network performance test utility that specifically leveraged the hearth framework. This app is meant to be run in serve mode and then connecting from the browser of one or more devices/clients that are on the same network. I, in particular, use it to verify/check the performance of what my server can provide.

## Features
* Throughput test allows you to quickly verify the bulk download and/or upload TCP performance to a single device/client
* Stream test allows you to to send a constant data rate TCP download stream with an optional buffer and/or bulk download/upload contention to verify throughput and latency
* Multi Client test allows you to verify multiple streams to multiple devices/clients
* Iperf test allows you to easily utilize iperf3
* All test parameters are configurable

I would definitely like to add more features based on feedback.

## Install
Run the following to install/update:

    curl -fsSL https://raw.githubusercontent.com/karmahelen/hearth/main/hearth-install.sh | bash

NOTE: The script will also install the necessary Hearth framework, but if you point it to a directory where it already exists then it will give you the option to just install/update xnetperf.

## Uninstall
Everything is self-contained to the folder you install to so if you don't like it you can just delete the folder to remove/uninstall.

## App Pics
[![View App Pics](https://img.shields.io/badge/App-Pics-blue)](https://karmahelen.github.io/xnetperf/AppPics.html)

## Background
I started development of this project for my own personal purposes on my Linux Mint. As I started building it up, I thought that this might be worthwhile to share. As a solo developer, I have currently only been able to fully test it out on Linux Mint 22.2 Cinnamon. I believe it should work with current releases of Ubuntu and potentially other similar Linux distros. If I can strike up interest, I would love to continue developing this for a broader audience but I need feedback. You can reach out to me at:

xnetperf.puppet866@passinbox.com
(I am using an email alias for filtering purposes and this is what I was able to create)

I am still working on better documentation to describe the functionality and features but am waiting to see if there is any real interest before spending too much effort.

Thanks for taking the time to look at this and hopefully you found something of interest!

## License
GNU GPLv3

## Support My Work
This project is open-source and free to use. If it has brought you value please consider throwing a tip in my jar.

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-ffdd00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/karmahelen)
