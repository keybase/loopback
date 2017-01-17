package loog

import (
	"fmt"
	"io"
	"os"

	"bazil.org/fuse"
)

var sink io.WriteCloser

func SetSink(file string) {
	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	sink = f
}

func LogAttr(p string, a *fuse.Attr, err error) {
	fmt.Fprintf(sink, "Attr valid=%s Inode=%x size=%d blocks=%d mode=%o nlink=%d uid=%d gid=%d rdev=%x flags=%o blocksize=%d error=%v path=%s\n",
		a.Valid,
		a.Inode,
		a.Size,
		a.Blocks,
		//  a.Atime
		//  a.Mtime
		//  a.Ctime
		//  a.Crtime
		a.Mode,
		a.Nlink,
		a.Uid,
		a.Gid,
		a.Rdev,
		a.Flags,
		a.BlockSize,
		err, p)
}
