package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/google/crfs/stargz"
	imgtypes "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	mediaTypeContainerdCheckpoint = "application/vnd.containerd.container.criu.checkpoint.criu.stargz"
)

var (
	registry string
	inode    uint64 = 1
)

func main() {
	app := cli.NewApp()
	app.Name = "ccfs"
	app.Usage = "ccfs REGISTRY MOUNTPOINT"
	app.Action = func(c *cli.Context) error {
		registry = c.Args().Get(0)
		if registry == "" {
			return errors.New("registry must be provided")
		}
		mountpoint := c.Args().Get(1)
		if mountpoint == "" {
			return errors.New("mountpoint must be provided")
		}
		conn, err := fuse.Mount(
			mountpoint,
			fuse.FSName("ccfs"),
			fuse.Subtype("ccfs"),
		)
		if err != nil {
			return err
		}
		defer conn.Close()

		if err = fs.Serve(conn, ccfs{}); err != nil {
			return err
		}
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		logrus.Error(err)
	}
}

type ccfs struct {
}

func (ccfs) Root() (fs.Node, error) {
	return newRootDirectory(), nil
}

func newRootDirectory() *rootDirectory {
	return &rootDirectory{
		entries: map[string]fuseEntry{},
	}
}

type rootDirectory struct {
	entries map[string]fuseEntry
}

func (*rootDirectory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0444
	return nil
}

func (rd *rootDirectory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if obj, exists := rd.entries[name]; exists {
		return obj.(fs.Node), nil
	}
	return nil, syscall.ENOENT
}

func (rd *rootDirectory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	all := []fuse.Dirent{}
	for _, entry := range rd.entries {
		all = append(all, entry.entryInfo())
	}
	return all, nil
}

func (*rootDirectory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, syscall.ENOTSUP
}

func (rd *rootDirectory) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if strings.Trim(req.Name, " \n\t") == "" {
		return nil, syscall.ENOTSUP
	}
	dir := newDirectory(ctx, req.Name)
	rd.entries[req.Name] = dir
	return dir, nil
}

type fuseEntry interface {
	entryInfo() fuse.Dirent
}

func newDirectory(ctx context.Context, name string) *directory {
	i := atomic.AddUint64(&inode, 1)
	entries := map[string]*file{}
	valid := true
	reader, err := getStargzReader(ctx, name)
	if err != nil {
		logrus.WithError(err).Info("failed to create stargz reader")
		valid = false
	} else {
		files, err := getFiles(name, reader)
		if err != nil {
			logrus.WithError(err).Error("failed to get files from stargz reader")
			valid = false
		} else {
			for _, file := range files {
				entries[file.Name] = file
			}
		}
	}
	if !valid {
		invalid := newFile(name, ".invalid", 0, nil)
		entries[invalid.Name] = invalid
	}
	return &directory{
		Dirent: fuse.Dirent{
			Inode: i,
			Name:  name,
			Type:  fuse.DT_Dir,
		},
		entries: entries,
	}
}

type directory struct {
	fuse.Dirent
	reader  *stargz.Reader
	entries map[string]*file
}

func getStargzReader(ctx context.Context, name string) (*stargz.Reader, error) {
	parts := strings.Split(name, ":")
	imageName := parts[0]
	imageTag := "latest"
	if len(parts) > 2 {
		imageTag = parts[1]
	}
	index, err := getIndex(ctx, imageName, imageTag)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get index for %s:%s", imageName, imageTag)
	}
	desc := getManifestByMediaType(index, mediaTypeContainerdCheckpoint)
	if desc == nil {
		return nil, errors.Errorf("%s:%s is not a valid containerd checkpoint image", imageName, imageTag)
	}
	stargzURLTemp := "http://%s/v2/%s/blobs/%s"
	stargzURL := fmt.Sprintf(stargzURLTemp, registry, imageName, string(desc.Digest))
	reader, err := stargz.Open(
		io.NewSectionReader(
			&urlReaderAt{
				url: stargzURL,
			},
			0,
			desc.Size,
		))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create stargz reader")
	}
	return reader, nil
}

func getFiles(parent string, reader *stargz.Reader) ([]*file, error) {
	toc, exists := reader.Lookup("")
	if !exists {
		return nil, errors.New("failed to lookup root dir")
	}
	files := []*file{}
	toc.ForeachChild(func(_ string, ent *stargz.TOCEntry) bool {
		if ent.Type == "reg" {
			files = append(files, newFile(parent, ent.Name, uint64(ent.Size), reader))
		}
		return true
	})
	return files, nil
}

func (d *directory) entryInfo() fuse.Dirent {
	return d.Dirent
}

func (d *directory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = d.Inode
	a.Mode = os.ModeDir | 0444
	return nil
}

func (d *directory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if f, exists := d.entries[name]; exists {
		return f, nil
	}
	return nil, syscall.ENOENT
}

func (d *directory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	all := []fuse.Dirent{}
	for _, entry := range d.entries {
		all = append(all, entry.entryInfo())
	}
	return all, nil
}

func (d *directory) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	return nil, syscall.ENOTSUP
}

func (d *directory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, syscall.ENOTSUP
}

type file struct {
	fuse.Dirent
	reader    *stargz.Reader
	size      uint64
	parentDir string
}

func newFile(parent string, name string, size uint64, reader *stargz.Reader) *file {
	i := atomic.AddUint64(&inode, 1)
	return &file{
		Dirent: fuse.Dirent{
			Inode: i,
			Name:  name,
			Type:  fuse.DT_File,
		},
		reader: reader,
		size:   size,
	}
}

func (f *file) entryInfo() fuse.Dirent {
	return f.Dirent
}

func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = f.Inode
	a.Mode = 0444
	a.Size = f.size
	return nil
}

func (f *file) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if !req.Flags.IsReadOnly() {
		return nil, syscall.EACCES
	}
	var r *io.SectionReader
	var err error
	// reader is nil for .invalid file
	if f.reader != nil {
		r, err = f.reader.OpenFile(f.Name)
		if err != nil {
			logrus.WithError(err).Errorf("failed to open %s", path.Join(f.parentDir, f.Name))
			return nil, syscall.EIO
		}
	}
	h := &readHandler{
		f:       f,
		sReader: r,
	}
	resp.Handle = h.handleID()
	logrus.Infof("open %s for read", path.Join(f.parentDir, f.Name))
	return h, nil
}

type readHandler struct {
	f       *file
	sReader *io.SectionReader
}

func (rh *readHandler) handleID() fuse.HandleID {
	return fuse.HandleID(uintptr(unsafe.Pointer(rh)))
}

func (rh *readHandler) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	if rh.sReader == nil {
		resp.Data = make([]byte, 0)
		return nil
	}
	size := int(rh.f.size - uint64(req.Offset))
	if req.Size < size {
		size = req.Size
	}
	logrus.Infof("read %s at %v with size %v",
		path.Join(rh.f.parentDir, rh.f.Name),
		req.Offset,
		size,
	)
	resp.Data = make([]byte, size)
	_, err := rh.sReader.ReadAt(resp.Data, req.Offset)
	if err != nil {
		logrus.WithError(err).Errorf("failed to read %s", path.Join(rh.f.parentDir, rh.f.Name))
		return syscall.EIO
	}
	return nil
}

func getIndex(ctx context.Context, name, tag string) (*imgtypes.Index, error) {
	manifestURLTemp := "http://%s/v2/%s/manifests/%s"
	manifestURL := fmt.Sprintf(manifestURLTemp, registry, name, tag)
	req, err := http.NewRequest("GET", manifestURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http request")
	}
	timeCtx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	req = req.WithContext(timeCtx)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		errBytes, _ := ioutil.ReadAll(response.Body)
		return nil, errors.Errorf("non-200 for %s: %v, %s", manifestURL, response.Status, errBytes)
	}
	contentType := response.Header.Get("Content-Type")
	if contentType != imgtypes.MediaTypeImageIndex {
		return nil, errors.Errorf("invalid content type %s", contentType)
	}
	index := &imgtypes.Index{}
	if err := json.NewDecoder(response.Body).Decode(index); err != nil {
		return nil, errors.Wrap(err, "failed to decode response")
	}
	return index, nil
}

func getManifestByMediaType(index *imgtypes.Index, mediaType string) *imgtypes.Descriptor {
	for _, desc := range index.Manifests {
		if desc.MediaType == mediaType {
			return &desc
		}
	}
	return nil
}

type urlReaderAt struct {
	url string
}

func (r *urlReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
	defer cancel()
	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		return 0, err
	}
	req = req.WithContext(ctx)
	rangeVal := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	req.Header.Set("Range", rangeVal)
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return 0, errors.Wrapf(err, "range read of %s", r.url)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusPartialContent {
		return 0, errors.Wrapf(err, "range read of %s, status %v", r.url, res.Status)
	}
	return io.ReadFull(res.Body, p)
}
