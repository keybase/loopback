// Copyright 2017 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

// +build linux darwin

package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

const (
	g_KAUTH_FILESEC_XATTR = "org.apple.system.Security"
	a_KAUTH_FILESEC_XATTR = "com.apple.system.Security"
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
}

func newFS(rootPath string) *FS {
	return &FS{
		rootPath: rootPath,
		xattrs:   make(map[string]map[string][]byte),
	}
}

// Root implements fs.FS interface for *FS
func (f *FS) Root() (n fs.Node, err error) {
	time.Sleep(latency)
	defer func() { log.Printf("FS.Root(): %#+v error=%v", n, err) }()
	return &Node{realPath: f.rootPath, isDir: true, fs: f}, nil
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

// Handle represent an open file or directory
type Handle struct {
	fs        *FS
	reopener  func() (*os.File, error)
	forgetter func()

	f *os.File
}

var _ fs.HandleFlusher = (*Handle)(nil)

// Flush implements fs.HandleFlusher interface for *Handle
func (h *Handle) Flush(ctx context.Context,
	req *fuse.FlushRequest) (err error) {
	time.Sleep(latency)
	defer func() { log.Printf("Handle(%s).Flush(): error=%v", h.f.Name(), err) }()
	return h.f.Sync()
}

var _ fs.HandleReadAller = (*Handle)(nil)

// ReadAll implements fs.HandleReadAller interface for *Handle
func (h *Handle) ReadAll(ctx context.Context) (d []byte, err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Handle(%s).ReadAll(): error=%v",
			h.f.Name(), err)
	}()
	return ioutil.ReadAll(h.f)
}

var _ fs.HandleReadDirAller = (*Handle)(nil)

// ReadDirAll implements fs.HandleReadDirAller interface for *Handle
func (h *Handle) ReadDirAll(ctx context.Context) (
	dirs []fuse.Dirent, err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Handle(%s).ReadDirAll(): %#+v error=%v",
			h.f.Name(), dirs, err)
	}()
	fis, err := h.f.Readdir(0)
	if err != nil {
		return nil, translateError(err)
	}

	// Readdir() reads up the entire dir stream but never resets the pointer.
	// Consequently, when Readdir is called again on the same *File, it gets
	// nothing. As a result, we need to close the file descriptor and re-open it
	// so next call would work.
	if err = h.f.Close(); err != nil {
		return nil, translateError(err)
	}
	if h.f, err = h.reopener(); err != nil {
		return nil, translateError(err)
	}

	return getDirentsWithFileInfos(fis), nil
}

var _ fs.HandleReader = (*Handle)(nil)

// Read implements fs.HandleReader interface for *Handle
func (h *Handle) Read(ctx context.Context,
	req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Handle(%s).Read(): error=%v",
			h.f.Name(), err)
	}()

	if _, err = h.f.Seek(req.Offset, 0); err != nil {
		return translateError(err)
	}
	resp.Data = make([]byte, req.Size)
	n, err := h.f.Read(resp.Data)
	resp.Data = resp.Data[:n]
	return translateError(err)
}

var _ fs.HandleReleaser = (*Handle)(nil)

// Release implements fs.HandleReleaser interface for *Handle
func (h *Handle) Release(ctx context.Context,
	req *fuse.ReleaseRequest) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Handle(%s).Release(): error=%v",
			h.f.Name(), err)
	}()
	if h.forgetter != nil {
		h.forgetter()
	}
	return h.f.Close()
}

var _ fs.HandleWriter = (*Handle)(nil)

// Write implements fs.HandleWriter interface for *Handle
func (h *Handle) Write(ctx context.Context,
	req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("Handle(%s).Write(): error=%v",
			h.f.Name(), err)
	}()

	if _, err = h.f.Seek(req.Offset, 0); err != nil {
		return translateError(err)
	}
	n, err := h.f.Write(req.Data)
	resp.Size = n
	return translateError(err)
}

// Node is the node for both directories and files
type Node struct {
	fs *FS

	realPath string
	isDir    bool

	lock     sync.RWMutex
	flushers map[*Handle]bool
}

var _ fs.NodeAccesser = (*Node)(nil)

// Access implements fs.NodeAccesser interface for *Node
func (n *Node) Access(ctx context.Context, a *fuse.AccessRequest) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("%s.Access(%o): error=%v", n.realPath, a.Mask, err)
	}()
	fi, err := os.Stat(n.realPath)
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
	defer func() { log.Printf("%s.Attr(): %#+v error=%v", n.realPath, a, err) }()
	fi, err := os.Stat(n.realPath)
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
			n.realPath, name, ret, err)
	}()

	if !n.isDir {
		return nil, fuse.ENOTSUP
	}

	p := filepath.Join(n.realPath, name)
	fi, err := os.Stat(p)

	err = translateError(err)
	if err != nil {
		return nil, translateError(err)
	}

	if fi.IsDir() {
		return &Node{realPath: p, isDir: true, fs: n.fs}, nil
	}
	return &Node{realPath: p, isDir: false, fs: n.fs}, nil
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

func (n *Node) rememberHandle(h *Handle) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.flushers == nil {
		n.flushers = make(map[*Handle]bool)
	}
	n.flushers[h] = true
}

func (n *Node) forgetHandle(h *Handle) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if n.flushers == nil {
		return
	}
	delete(n.flushers, h)
}

var _ fs.NodeOpener = (*Node)(nil)

// Open implements fs.NodeOpener interface for *Node
func (n *Node) Open(ctx context.Context,
	req *fuse.OpenRequest, resp *fuse.OpenResponse) (h fs.Handle, err error) {
	time.Sleep(latency)
	flags, perm := fuseOpenFlagsToOSFlagsAndPerms(req.Flags)
	defer func() {
		log.Printf("%s.Open(): %o %o error=%v",
			n.realPath, flags, perm, err)
	}()

	opener := func() (*os.File, error) {
		return os.OpenFile(n.realPath, flags, perm)
	}

	f, err := opener()
	if err != nil {
		return nil, translateError(err)
	}

	handle := &Handle{fs: n.fs, f: f, reopener: opener}
	n.rememberHandle(handle)
	handle.forgetter = func() {
		n.forgetHandle(handle)
	}
	return handle, nil
}

var _ fs.NodeCreater = (*Node)(nil)

// Create implements fs.NodeCreater interface for *Node
func (n *Node) Create(
	ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (
	fsn fs.Node, fsh fs.Handle, err error) {
	time.Sleep(latency)
	flags, _ := fuseOpenFlagsToOSFlagsAndPerms(req.Flags)
	name := filepath.Join(n.realPath, req.Name)
	defer func() {
		log.Printf("%s.Create(%s): %o %o error=%v",
			n.realPath, name, flags, req.Mode, err)
	}()

	opener := func() (f *os.File, err error) {
		return os.OpenFile(name, flags, req.Mode)
	}

	f, err := opener()
	if err != nil {
		return nil, nil, translateError(err)
	}

	h := &Handle{fs: n.fs, f: f, reopener: opener}

	node := &Node{
		realPath: filepath.Join(n.realPath, req.Name),
		isDir:    req.Mode.IsDir(),
		fs:       n.fs,
	}
	node.rememberHandle(h)
	h.forgetter = func() {
		node.forgetHandle(h)
	}
	return node, h, nil
}

var _ fs.NodeMkdirer = (*Node)(nil)

// Mkdir implements fs.NodeMkdirer interface for *Node
func (n *Node) Mkdir(ctx context.Context,
	req *fuse.MkdirRequest) (created fs.Node, err error) {
	time.Sleep(latency)
	defer func() { log.Printf("%s.Mkdir(%s): error=%v", n.realPath, req.Name, err) }()
	name := filepath.Join(n.realPath, req.Name)
	if err = os.Mkdir(name, req.Mode); err != nil {
		return nil, translateError(err)
	}
	return &Node{realPath: name, isDir: true, fs: n.fs}, nil
}

var _ fs.NodeRemover = (*Node)(nil)

// Remove implements fs.NodeRemover interface for *Node
func (n *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	time.Sleep(latency)
	name := filepath.Join(n.realPath, req.Name)
	defer func() { log.Printf("%s.Remove(%s): error=%v", n.realPath, name, err) }()
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
	defer func() { log.Printf("%s.Fsync(): error=%v", n.realPath, err) }()
	n.lock.RLock()
	defer n.lock.RUnlock()
	for h := range n.flushers {
		return h.f.Sync()
	}
	return fuse.EIO
}

var _ fs.NodeSetattrer = (*Node)(nil)

// Setattr implements fs.NodeSetattrer interface for *Node
func (n *Node) Setattr(ctx context.Context,
	req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	time.Sleep(latency)
	defer func() {
		log.Printf("%s.Setattr(valid=%x): error=%v", n.realPath, req.Valid, err)
	}()
	if req.Valid.Size() {
		if err = syscall.Truncate(n.realPath, int64(req.Size)); err != nil {
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
			n.realPath)
	}

	if req.Valid.Mode() {
		if err = os.Chmod(n.realPath, req.Mode); err != nil {
			return translateError(err)
		}
	}

	if req.Valid.Uid() || req.Valid.Gid() {
		if req.Valid.Uid() && req.Valid.Gid() {
			if err = os.Chown(n.realPath, int(req.Uid), int(req.Gid)); err != nil {
				return translateError(err)
			}
		}
		fi, err := os.Stat(n.realPath)
		if err != nil {
			return translateError(err)
		}
		s := fi.Sys().(*syscall.Stat_t)
		if req.Valid.Uid() {
			if err = os.Chown(n.realPath, int(req.Uid), int(s.Gid)); err != nil {
				return translateError(err)
			}
		} else {
			if err = os.Chown(n.realPath, int(s.Uid), int(req.Gid)); err != nil {
				return translateError(err)
			}
		}
	}

	if err = n.setattrPlatformSpecific(ctx, req, resp); err != nil {
		return translateError(err)
	}

	fi, err := os.Stat(n.realPath)
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
	np := filepath.Join(newDir.(*Node).realPath, req.NewName)
	op := filepath.Join(n.realPath, req.OldName)
	defer func() {
		log.Printf("%s.Rename(%s->%s): error=%v",
			n.realPath, op, np, err)
	}()
	defer func() {
		if err == nil {
			n.fs.moveAllxattrs(ctx, op, np)
		}
	}()
	return os.Rename(op, np)
}

func xattrNameMutate(name string) string {
	if strings.HasPrefix(name, a_KAUTH_FILESEC_XATTR) {
		name = g_KAUTH_FILESEC_XATTR + name[len(g_KAUTH_FILESEC_XATTR):]
	}
	return name
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
		log.Printf("%s.Getxattr(%s): error=%v", n.realPath, req.Name, err)
	}()
	n.fs.xlock.RLock()
	defer n.fs.xlock.RUnlock()
	if x := n.fs.xattrs[n.realPath]; x != nil {

		name := xattrNameMutate(req.Name)

		var ok bool
		resp.Xattr, ok = x[name]
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
			n.realPath, req.Position, req.Size, err)
	}()
	n.fs.xlock.RLock()
	defer n.fs.xlock.RUnlock()
	if x := n.fs.xattrs[n.realPath]; x != nil {
		names := make([]string, 0)
		for k := range x {
			if !strings.HasPrefix(k, g_KAUTH_FILESEC_XATTR) {
				names = append(names, k)
			}
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
		log.Printf("%s.Setxattr(%s): error=%v", n.realPath, req.Name, err)
	}()
	n.fs.xlock.Lock()
	defer n.fs.xlock.Unlock()
	if n.fs.xattrs[n.realPath] == nil {
		n.fs.xattrs[n.realPath] = make(map[string][]byte)
	}
	buf := make([]byte, len(req.Xattr))
	copy(buf, req.Xattr)

	name := xattrNameMutate(req.Name)

	n.fs.xattrs[n.realPath][name] = buf
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
		log.Printf("%s.Removexattr(%s): error=%v", n.realPath, req.Name, err)
	}()
	n.fs.xlock.Lock()
	defer n.fs.xlock.Unlock()

	name := xattrNameMutate(req.Name)

	if x := n.fs.xattrs[n.realPath]; x != nil {
		var ok bool
		_, ok = x[name]
		if ok {
			delete(x, name)
			return nil
		}
	}
	return fuse.ENOATTR
}
