// Copyright 2017 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

// +build linux darwin

package main

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

const (
	attrValidDuration = time.Second
)

func translateError(err error) error {
	switch {
	case os.IsNotExist(err):
		return fuse.ENOENT
	case os.IsExist(err):
		return fuse.EEXIST
	case os.IsPermission(err):
		return fuse.EPERM
	default:
		return err
	}
}

// FS is the filesystem root
type FS struct {
	rootPath string

	xlock  sync.RWMutex
	xattrs map[string]map[string][]byte

	nlock sync.Mutex
	nodes map[string][]*Node // realPath -> nodes
}

func newFS(rootPath string) *FS {
	return &FS{
		rootPath: rootPath,
		xattrs:   make(map[string]map[string][]byte),
		nodes:    make(map[string][]*Node),
	}
}

func (f *FS) newNode(n *Node) {
	rp := n.getRealPath()

	f.nlock.Lock()
	defer f.nlock.Unlock()
	f.nodes[rp] = append(f.nodes[rp], n)
}

func (f *FS) nodeRenamed(oldPath string, newPath string) {
	f.nlock.Lock()
	defer f.nlock.Unlock()
	f.nodes[newPath] = append(f.nodes[newPath], f.nodes[oldPath]...)
	delete(f.nodes, oldPath)
	for _, n := range f.nodes[newPath] {
		n.updateRealPath(newPath)
	}
}

func (f *FS) forgetNode(n *Node) {
	f.nlock.Lock()
	defer f.nlock.Unlock()
	nodes, ok := f.nodes[n.realPath]
	if !ok {
		return
	}

	found := -1
	for i, node := range nodes {
		if node == n {
			found = i
			break
		}
	}

	if found > -1 {
		nodes = append(nodes[:found], nodes[found+1:]...)
	}
	if len(nodes) == 0 {
		delete(f.nodes, n.realPath)
	} else {
		f.nodes[n.realPath] = nodes
	}
}

// Root implements fs.FS interface for *FS
func (f *FS) Root() (n fs.Node, err error) {
	time.Sleep(latency)
	defer func() { log.Printf("FS.Root(): %#+v error=%v", n, err) }()
	nn := &Node{realPath: f.rootPath, isDir: true, fs: f}
	f.newNode(nn)
	return nn, nil
}

var _ fs.FSStatfser = (*FS)(nil)

// Statfs implements fs.FSStatfser interface for *FS
func (f *FS) Statfs(ctx context.Context,
	req *fuse.StatfsRequest, resp *fuse.StatfsResponse) (err error) {
	time.Sleep(latency)
	defer func() { log.Printf("FS.Statfs(): error=%v", err) }()
	var stat syscall.Statfs_t
	if err := syscall.Statfs(f.rootPath, &stat); err != nil {
		return translateError(err)
	}
	resp.Blocks = stat.Blocks
	resp.Bfree = stat.Bfree
	resp.Bavail = stat.Bavail
	resp.Files = 0 // TODO
	resp.Ffree = stat.Ffree
	resp.Bsize = uint32(stat.Bsize)
	resp.Namelen = 255 // TODO
	resp.Frsize = 8    // TODO

	return nil
}

// if to is empty, all xattrs on the node is removed
func (f *FS) moveAllxattrs(ctx context.Context, from string, to string) {
	f.xlock.Lock()
	defer f.xlock.Unlock()
	if f.xattrs[from] != nil {
		if to != "" {
			f.xattrs[to] = f.xattrs[from]
		}
		f.xattrs[from] = nil
	}
}

// Node is the node for both directories and files
type Node struct {
	fs *FS

	rpLock   sync.RWMutex
	realPath string

	isDir bool

	lock sync.RWMutex
}

func (n *Node) getRealPath() string {
	n.rpLock.RLock()
	defer n.rpLock.RUnlock()
	return n.realPath
}

func (n *Node) updateRealPath(realPath string) {
	n.rpLock.Lock()
	defer n.rpLock.Unlock()
	n.realPath = realPath
}

var _ fs.HandleReadDirAller = (*Node)(nil)

// ReadDirAll implements fs.HandleReadDirAller interface for *Node
func (n *Node) ReadDirAll(ctx context.Context) (
	dirs []fuse.Dirent, err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Node(%s).ReadDirAll(): %#+v error=%v",
			n.getRealPath(), dirs, err)
	}()
	f, err := os.Open(n.getRealPath())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return nil, translateError(err)
	}
	return getDirentsWithFileInfos(fis), nil
}

var _ fs.HandleReader = (*Node)(nil)

// Read implements fs.HandleReader interface for *Node
func (n *Node) Read(ctx context.Context,
	req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Node(%s).Read(): error=%v",
			n.getRealPath(), err)
	}()

	f, err := os.Open(n.getRealPath())
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Seek(req.Offset, 0); err != nil {
		return translateError(err)
	}
	resp.Data = make([]byte, req.Size)
	nRead, err := f.Read(resp.Data)
	resp.Data = resp.Data[:nRead]
	return translateError(err)
}

var _ fs.HandleWriter = (*Node)(nil)

// Write implements fs.HandleWriter interface for *Node
func (n *Node) Write(ctx context.Context,
	req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Node(%s).Write(): error=%v",
			n.getRealPath(), err)
	}()

	f, err := os.OpenFile(n.getRealPath(), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Seek(req.Offset, 0); err != nil {
		return translateError(err)
	}
	nRead, err := f.Write(req.Data)
	resp.Size = nRead
	return translateError(err)
}

var _ fs.NodeAccesser = (*Node)(nil)

// Access implements fs.NodeAccesser interface for *Node
func (n *Node) Access(ctx context.Context, a *fuse.AccessRequest) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("%s.Access(%o): error=%v", n.getRealPath(), a.Mask, err)
	}()
	fi, err := os.Stat(n.getRealPath())
	if err != nil {
		return translateError(err)
	}
	if a.Mask&uint32(fi.Mode()>>6) != a.Mask {
		return fuse.EPERM
	}
	return nil
}

// Attr implements fs.Node interface for *Dir
func (n *Node) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	time.Sleep(latency)
	defer func() { log.Printf("%s.Attr(): %#+v error=%v", n.getRealPath(), a, err) }()
	fi, err := os.Stat(n.getRealPath())
	if err != nil {
		return translateError(err)
	}

	fillAttrWithFileInfo(a, fi)

	return nil
}

// Lookup implements fs.NodeRequestLookuper interface for *Node
func (n *Node) Lookup(ctx context.Context,
	name string) (ret fs.Node, err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("%s.Lookup(%s): %#+v error=%v",
			n.getRealPath(), name, ret, err)
	}()

	if !n.isDir {
		return nil, fuse.ENOTSUP
	}

	p := filepath.Join(n.getRealPath(), name)
	fi, err := os.Stat(p)

	err = translateError(err)
	if err != nil {
		return nil, translateError(err)
	}

	var nn *Node
	if fi.IsDir() {
		nn = &Node{realPath: p, isDir: true, fs: n.fs}
	} else {
		nn = &Node{realPath: p, isDir: false, fs: n.fs}
	}

	n.fs.newNode(nn)
	return nn, nil
}

func getDirentsWithFileInfos(fis []os.FileInfo) (dirs []fuse.Dirent) {
	for _, fi := range fis {
		stat := fi.Sys().(*syscall.Stat_t)
		var tp fuse.DirentType

		switch {
		case fi.IsDir():
			tp = fuse.DT_Dir
		case fi.Mode().IsRegular():
			tp = fuse.DT_File
		default:
			panic("unsupported dirent type")
		}

		dirs = append(dirs, fuse.Dirent{
			Inode: stat.Ino,
			Name:  fi.Name(),
			Type:  tp,
		})
	}

	return dirs
}

func fuseOpenFlagsToOSFlagsAndPerms(
	f fuse.OpenFlags) (flag int, perm os.FileMode) {
	flag = int(f & fuse.OpenAccessModeMask)
	if f&fuse.OpenAppend != 0 {
		perm |= os.ModeAppend
	}
	if f&fuse.OpenCreate != 0 {
		flag |= os.O_CREATE
	}
	if f&fuse.OpenDirectory != 0 {
		perm |= os.ModeDir
	}
	if f&fuse.OpenExclusive != 0 {
		perm |= os.ModeExclusive
	}
	if f&fuse.OpenNonblock != 0 {
		log.Printf("fuse.OpenNonblock is set in OpenFlags but ignored")
	}
	if f&fuse.OpenSync != 0 {
		flag |= os.O_SYNC
	}
	if f&fuse.OpenTruncate != 0 {
		flag |= os.O_TRUNC
	}

	return flag, perm
}

var _ fs.NodeCreater = (*Node)(nil)

// Create implements fs.NodeCreater interface for *Node
func (n *Node) Create(
	ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (
	fsn fs.Node, fsh fs.Handle, err error) {
	time.Sleep(latency)
	flags, _ := fuseOpenFlagsToOSFlagsAndPerms(req.Flags)
	name := filepath.Join(n.getRealPath(), req.Name)
	defer func() {
		log.Printf("%s.Create(%s): %o %o error=%v",
			n.getRealPath(), name, flags, req.Mode, err)
	}()

	node := &Node{
		realPath: filepath.Join(n.getRealPath(), req.Name),
		isDir:    req.Mode.IsDir(),
		fs:       n.fs,
	}

	f, err := os.Create(node.realPath)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	n.fs.newNode(node)
	return node, node, nil
}

var _ fs.NodeMkdirer = (*Node)(nil)

// Mkdir implements fs.NodeMkdirer interface for *Node
func (n *Node) Mkdir(ctx context.Context,
	req *fuse.MkdirRequest) (created fs.Node, err error) {
	time.Sleep(latency)
	defer func() { log.Printf("%s.Mkdir(%s): error=%v", n.getRealPath(), req.Name, err) }()
	name := filepath.Join(n.getRealPath(), req.Name)
	if err = os.Mkdir(name, req.Mode); err != nil {
		return nil, translateError(err)
	}
	nn := &Node{realPath: name, isDir: true, fs: n.fs}
	n.fs.newNode(nn)
	return nn, nil
}

var _ fs.NodeRemover = (*Node)(nil)

// Remove implements fs.NodeRemover interface for *Node
func (n *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	time.Sleep(latency)
	name := filepath.Join(n.getRealPath(), req.Name)
	defer func() { log.Printf("%s.Remove(%s): error=%v", n.getRealPath(), name, err) }()
	defer func() {
		if err == nil {
			n.fs.moveAllxattrs(ctx, name, "")
		}
	}()
	return os.Remove(name)
}

var _ fs.NodeFsyncer = (*Node)(nil)

// Fsync implements fs.NodeFsyncer interface for *Node
func (n *Node) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	time.Sleep(latency)
	defer func() { log.Printf("%s.Fsync(): error=%v", n.getRealPath(), err) }()
	return nil
}

var _ fs.NodeSetattrer = (*Node)(nil)

// Setattr implements fs.NodeSetattrer interface for *Node
func (n *Node) Setattr(ctx context.Context,
	req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("%s.Setattr(valid=%x): error=%v", n.getRealPath(), req.Valid, err)
	}()
	if req.Valid.Size() {
		if err = syscall.Truncate(n.getRealPath(), int64(req.Size)); err != nil {
			return translateError(err)
		}
	}

	if req.Valid.Mtime() {
		var tvs [2]syscall.Timeval
		if !req.Valid.Atime() {
			tvs[0] = tToTv(time.Now())
		} else {
			tvs[0] = tToTv(req.Atime)
		}
		tvs[1] = tToTv(req.Mtime)
	}

	if req.Valid.Handle() {
		log.Printf("%s.Setattr(): unhandled request: req.Valid.Handle() == true",
			n.getRealPath())
	}

	if req.Valid.Mode() {
		if err = os.Chmod(n.getRealPath(), req.Mode); err != nil {
			return translateError(err)
		}
	}

	if req.Valid.Uid() || req.Valid.Gid() {
		if req.Valid.Uid() && req.Valid.Gid() {
			if err = os.Chown(n.getRealPath(), int(req.Uid), int(req.Gid)); err != nil {
				return translateError(err)
			}
		}
		fi, err := os.Stat(n.getRealPath())
		if err != nil {
			return translateError(err)
		}
		s := fi.Sys().(*syscall.Stat_t)
		if req.Valid.Uid() {
			if err = os.Chown(n.getRealPath(), int(req.Uid), int(s.Gid)); err != nil {
				return translateError(err)
			}
		} else {
			if err = os.Chown(n.getRealPath(), int(s.Uid), int(req.Gid)); err != nil {
				return translateError(err)
			}
		}
	}

	if err = n.setattrPlatformSpecific(ctx, req, resp); err != nil {
		return translateError(err)
	}

	fi, err := os.Stat(n.getRealPath())
	if err != nil {
		return translateError(err)
	}

	fillAttrWithFileInfo(&resp.Attr, fi)

	return nil
}

var _ fs.NodeRenamer = (*Node)(nil)

// Rename implements fs.NodeRenamer interface for *Node
func (n *Node) Rename(ctx context.Context,
	req *fuse.RenameRequest, newDir fs.Node) (err error) {
	time.Sleep(latency)
	np := filepath.Join(newDir.(*Node).getRealPath(), req.NewName)
	op := filepath.Join(n.getRealPath(), req.OldName)
	defer func() {
		log.Printf("%s.Rename(%s->%s): error=%v",
			n.getRealPath(), op, np, err)
	}()
	defer func() {
		if err == nil {
			n.fs.moveAllxattrs(ctx, op, np)
			n.fs.nodeRenamed(op, np)
		}
	}()
	return os.Rename(op, np)
}

var _ fs.NodeGetxattrer = (*Node)(nil)

// Getxattr implements fs.Getxattrer interface for *Node
func (n *Node) Getxattr(ctx context.Context,
	req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) (err error) {
	time.Sleep(latency)
	if !inMemoryXattr {
		return fuse.ENOTSUP
	}

	defer func() {
		log.Printf("%s.Getxattr(%s): error=%v", n.getRealPath(), req.Name, err)
	}()
	n.fs.xlock.RLock()
	defer n.fs.xlock.RUnlock()
	if x := n.fs.xattrs[n.getRealPath()]; x != nil {

		var ok bool
		resp.Xattr, ok = x[req.Name]
		if ok {
			return nil
		}
	}
	return fuse.ENOATTR
}

var _ fs.NodeListxattrer = (*Node)(nil)

// Listxattr implements fs.Listxattrer interface for *Node
func (n *Node) Listxattr(ctx context.Context,
	req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) (err error) {
	time.Sleep(latency)
	if !inMemoryXattr {
		return fuse.ENOTSUP
	}

	defer func() {
		log.Printf("%s.Listxattr(%d,%d): error=%v",
			n.getRealPath(), req.Position, req.Size, err)
	}()
	n.fs.xlock.RLock()
	defer n.fs.xlock.RUnlock()
	if x := n.fs.xattrs[n.getRealPath()]; x != nil {
		names := make([]string, 0)
		for k := range x {
			names = append(names, k)
		}
		sort.Strings(names)

		if int(req.Position) >= len(names) {
			return nil
		}
		names = names[int(req.Position):]

		s := int(req.Size)
		if s == 0 || s > len(names) {
			s = len(names)
		}
		if s > 0 {
			resp.Append(names[:s]...)
		}
	}

	return nil
}

var _ fs.NodeSetxattrer = (*Node)(nil)

// Setxattr implements fs.Setxattrer interface for *Node
func (n *Node) Setxattr(ctx context.Context,
	req *fuse.SetxattrRequest) (err error) {
	time.Sleep(latency)
	if !inMemoryXattr {
		return fuse.ENOTSUP
	}

	defer func() {
		log.Printf("%s.Setxattr(%s): error=%v", n.getRealPath(), req.Name, err)
	}()
	n.fs.xlock.Lock()
	defer n.fs.xlock.Unlock()
	if n.fs.xattrs[n.getRealPath()] == nil {
		n.fs.xattrs[n.getRealPath()] = make(map[string][]byte)
	}
	buf := make([]byte, len(req.Xattr))
	copy(buf, req.Xattr)

	n.fs.xattrs[n.getRealPath()][req.Name] = buf
	return nil
}

var _ fs.NodeRemovexattrer = (*Node)(nil)

// Removexattr implements fs.Removexattrer interface for *Node
func (n *Node) Removexattr(ctx context.Context,
	req *fuse.RemovexattrRequest) (err error) {
	time.Sleep(latency)
	if !inMemoryXattr {
		return fuse.ENOTSUP
	}

	defer func() {
		log.Printf("%s.Removexattr(%s): error=%v", n.getRealPath(), req.Name, err)
	}()
	n.fs.xlock.Lock()
	defer n.fs.xlock.Unlock()

	name := req.Name

	if x := n.fs.xattrs[n.getRealPath()]; x != nil {
		var ok bool
		_, ok = x[name]
		if ok {
			delete(x, name)
			return nil
		}
	}
	return fuse.ENOATTR
}

var _ fs.NodeForgetter = (*Node)(nil)

// Forget implements fs.NodeForgetter interface for *Node
func (n *Node) Forget() {
	n.fs.forgetNode(n)
}
