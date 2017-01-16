// Copyright 2017 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

// +build linux darwin

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var (
	inMemoryXattr bool
	latency       time.Duration

	latencyStr string
)

func init() {
	flag.BoolVar(&inMemoryXattr, "in-memory-xattr", false,
		"use an in-memory implementation for xattr. Otherwise,\n"+
			"fall back to the ._ file approach provided by osxfuse.")
	flag.StringVar(&latencyStr, "latency", "0s",
		"add an artificial latency to every fuse handler on every call")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s ROOT MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	var err error
	if latency, err = time.ParseDuration(latencyStr); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		usage()
		os.Exit(2)
	}

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(1)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("loopback"),
		fuse.Subtype("loopback-fs"),
		fuse.VolumeName("goLoopback"),
		fuse.AllowRoot(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}

	log.Println("mounted!")

	err = fs.Serve(c, newFS(flag.Arg(0)))
	if err != nil {
		log.Fatal(err)
	}

}
