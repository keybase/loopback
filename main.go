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
	"strconv"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var (
	inMemoryXattr bool
	latency       time.Duration
)

func init() {
	flag.BoolVar(&inMemoryXattr, "in-memory-xattr", false,
		"use an in-memory implementation for xattr. Otherwise,\n"+
			"fall back to the ._ file approach provided by osxfuse.")
	flag.DurationVar(&latency, "latency", 0,
		"add an artificial latency to every fuse handler on every call")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s ROOT MOUNTPOINT [[ROOT MOUNTPOINT] ...]\n",
		os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() < 2 || flag.NArg()%2 != 0 {
		usage()
		os.Exit(2)
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < flag.NArg(); i += 2 {
		mountpoint := flag.Arg(i + 1)

		c, err := fuse.Mount(
			mountpoint,
			fuse.FSName("loopback"),
			fuse.Subtype("loopback-fs"),
			fuse.VolumeName("goLoopback"+strconv.Itoa(i/2)),
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

		wg.Add(1)
		go func(c *fuse.Conn, i int) {
			err = fs.Serve(c, newFS(flag.Arg(i)))
			if err != nil {
				log.Fatalf("fs.Serve error: %v\n", err)
			}
			wg.Done()
		}(c, i)
	}

	wg.Wait()
}
