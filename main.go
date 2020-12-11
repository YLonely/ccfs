package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
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
	inode    uint64 = 0
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
	return newDirectory(context.Background(), "", true), nil
}

func newDirectory(ctx context.Context, name string, root bool) *directory {
	i := atomic.AddUint64(&inode, 1)
	var index *imgtypes.Index
	var desc *imgtypes.Descriptor
	var err error
	entries := map[string]fuseEntry{}
	valid := true
	if !root {
		parts := strings.Split(name, ":")
		imageName := parts[0]
		imageTag := "latest"
		if len(parts) > 2 {
			imageTag = parts[1]
		}
		index, err = getIndex(ctx, imageName, imageTag)
		if err != nil {
			logrus.WithError(err).Infof("failed to get index for %s:%s", imageName, imageTag)
			valid = false
		} else {
			desc = getManifestByMediaType(index, mediaTypeContainerdCheckpoint)
			if desc == nil {
				logrus.Infof("%s:%s is not a valid containerd checkpoint image", imageName, imageTag)
				valid = false
			}
		}
	}
	if !valid {
		invalid := newFile(".invalid")
		entries[invalid.Name] = invalid
	}
	return &directory{
		Dirent: fuse.Dirent{
			Inode: i,
			Name:  name,
			Type:  fuse.DT_Dir,
		},
		entries: entries,
		root:    root,
	}
}

func getFiles(ctx context.Context, name string, stargzDesc *imgtypes.Descriptor) ([]*file, error) {

}

type fuseEntry interface {
	entryInfo() fuse.Dirent
}

type directory struct {
	fuse.Dirent
	entries map[string]fuseEntry
	root    bool
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
	if obj, exists := d.entries[name]; exists {
		return obj.(fs.Node), nil
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
	if !d.root || strings.Trim(req.Name, " \n\t") == "" {
		return nil, syscall.ENOTSUP
	}
	dir := newDirectory(ctx, req.Name, false)
	d.entries[req.Name] = dir
	return dir, nil
}

func (d *directory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, syscall.ENOTSUP
}

type file struct {
	fuse.Dirent
	size uint64
}

func newFile(name string) *file {
	i := atomic.AddUint64(&inode, 1)
	return &file{
		Dirent: fuse.Dirent{
			Inode: i,
			Name:  name,
			Type:  fuse.DT_File,
		},
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

func (f *file) ReadAll(ctx context.Context) ([]byte, error) {
	return []byte{}, nil
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
