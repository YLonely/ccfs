package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/YLonely/ccfs/cache"
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
	registry   string
	inode      uint64 = 1
	bufferPool        = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
)

func main() {
	app := cli.NewApp()
	app.Name = "ccfs"
	app.Usage = "ccfs REGISTRY MOUNTPOINT"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config",
			Usage: "specifiy the path to the cache config",
		},
	}
	app.Action = func(c *cli.Context) error {
		registry = c.Args().Get(0)
		if registry == "" {
			return errors.New("registry must be provided")
		}
		mountpoint := c.Args().Get(1)
		if mountpoint == "" {
			return errors.New("mountpoint must be provided")
		}
		configPath := c.String("config")
		config := &cache.Config{}
		if configPath == "" {
			logrus.Info("running ccfs without cache")
			config = nil
		} else {
			data, err := ioutil.ReadFile(configPath)
			if err != nil {
				return errors.Wrap(err, "failed to read config")
			}
			if err = json.Unmarshal(data, config); err != nil {
				return err
			}
			logrus.WithFields(logrus.Fields{
				"directory":              config.Directory,
				"level1MaxLRUCacheEntry": config.Level1MaxLRUCacheEntry,
				"maxLRUCacheEntry":       config.MaxLRUCacheEntry,
				"gcInterval":             config.GCInterval,
			}).Info("running ccfs with cache")
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
		if err = fs.Serve(conn, ccfs{config: config}); err != nil {
			return err
		}
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		logrus.Error(err)
	}
}

type ccfs struct {
	config *cache.Config
}

type fuseEntry interface {
	EntryInfo() fuse.Dirent
}

type fuseSizedEntry interface {
	fuseEntry
	Size() uint64
}

func (c ccfs) Root() (fs.Node, error) {
	return newRootDirectory(c.config)
}

func newRootDirectory(config *cache.Config) (*rootDirectory, error) {
	rd := &rootDirectory{
		entries: map[string]*directory{},
		config:  config,
	}
	if config == nil {
		return rd, nil
	}
	wc, err := cache.NewWeightedBlobCache(config.Directory, cache.WeightedBlobCacheConfig{
		Level1MaxLRUCacheEntry: config.Level1MaxLRUCacheEntry,
		MaxLRUCacheEntry:       config.MaxLRUCacheEntry,
		GCInterval:             config.GCInterval,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create weighted cache")
	}
	rd.weightedCache = wc
	return rd, nil
}

type rootDirectory struct {
	sync.Mutex
	entries       map[string]*directory
	config        *cache.Config
	weightedCache *cache.WeightedBlobCache
}

func (*rootDirectory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0444
	return nil
}

func (rd *rootDirectory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if dir, exists := rd.entries[name]; exists {
		return dir, nil
	}
	return nil, syscall.ENOENT
}

func (rd *rootDirectory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	all := []fuse.Dirent{}
	for _, entry := range rd.entries {
		all = append(all, entry.EntryInfo())
	}
	return all, nil
}

func (*rootDirectory) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, syscall.ENOTSUP
}

func (rd *rootDirectory) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if _, exists := rd.entries[req.Name]; !exists {
		return syscall.ENOENT
	}
	rd.Lock()
	defer rd.Unlock()
	delete(rd.entries, req.Name)
	return nil
}

func (rd *rootDirectory) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if strings.Trim(req.Name, " \n\t") == "" {
		return nil, syscall.ENOTSUP
	}
	var dir *directory
	var exists bool
	if dir, exists = rd.entries[req.Name]; exists {
		return nil, syscall.EEXIST
	}
	rd.Lock()
	defer rd.Unlock()
	if dir, exists = rd.entries[req.Name]; exists {
		return dir, nil
	}
	dir = newDirectory(ctx, req.Name, rd.weightedCache)
	rd.entries[req.Name] = dir
	return dir, nil
}

func newDirectory(ctx context.Context, name string, wc *cache.WeightedBlobCache) *directory {
	i := atomic.AddUint64(&inode, 1)
	entries := map[string]fuseEntry{}
	valid := true
	reader, err := getStargzReader(ctx, name)
	if err != nil {
		logrus.WithError(err).Info("failed to create stargz reader")
		valid = false
	} else {
		var bc cache.BlobCache
		if wc != nil {
			if err = wc.AddCache(name, 0); err == nil {
				bc, _ = wc.GetCache(name)
			} else {
				valid = false
				logrus.WithError(err).Errorf("failed to add cache with id %q", name)
			}
		}
		files, err := getFiles(name, reader, bc)
		if err != nil {
			logrus.WithError(err).Error("failed to get files from stargz reader")
			valid = false
		} else {
			for _, file := range files {
				entries[file.EntryInfo().Name] = file
			}
		}
	}
	var endFile *staticFile
	if !valid {
		endFile = newStaticFile(name, ".end", "invalid")
	} else {
		if wc != nil {
			wf := newWeightFile(name, ".weight", 0, 0, func(w float32) error {
				return wc.Adjust(name, w)
			})
			entries[wf.name] = wf
		}
		endFile = newStaticFile(name, ".end", "valid")
	}
	entries[endFile.name] = endFile
	return &directory{
		inode:   i,
		name:    name,
		entries: entries,
	}
}

type directory struct {
	sync.Mutex
	inode   uint64
	name    string
	entries map[string]fuseEntry
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

func getFiles(parent string, reader *stargz.Reader, bc cache.BlobCache) ([]fuseEntry, error) {
	toc, exists := reader.Lookup("")
	if !exists {
		return nil, errors.New("failed to lookup root dir")
	}
	files := []fuseEntry{}
	toc.ForeachChild(func(_ string, ent *stargz.TOCEntry) bool {
		if ent.Type == "reg" {
			files = append(files, newRemoteFile(parent, ent.Name, uint64(ent.Size), reader, bc))
		}
		return true
	})
	return files, nil
}

func (d *directory) EntryInfo() fuse.Dirent {
	return fuse.Dirent{
		Inode: d.inode,
		Type:  fuse.DT_Dir,
		Name:  d.name,
	}
}

func (d *directory) Attr(ctx context.Context, a *fuse.Attr) error {
	var size uint64
	d.Lock()
	defer d.Unlock()
	a.Inode = d.inode
	a.Mode = os.ModeDir | 0444
	for _, e := range d.entries {
		sized, ok := e.(fuseSizedEntry)
		if ok {
			size += sized.Size()
		}
	}
	a.Size = size
	return nil
}

func (d *directory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if f, exists := d.entries[name]; exists {
		return f.(fs.Node), nil
	}
	return nil, syscall.ENOENT
}

func (d *directory) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if _, exists := d.entries[req.Name]; !exists {
		return syscall.ENOENT
	}
	d.Lock()
	defer d.Unlock()
	delete(d.entries, req.Name)
	return nil
}

func (d *directory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	all := []fuse.Dirent{}
	for _, entry := range d.entries {
		all = append(all, entry.EntryInfo())
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
	inode     uint64
	name      string
	mode      os.FileMode
	size      uint64
	parentDir string
}

func (f *file) EntryInfo() fuse.Dirent {
	return fuse.Dirent{
		Inode: f.inode,
		Name:  f.name,
		Type:  fuse.DT_File,
	}
}

func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = f.inode
	a.Mode = f.mode
	a.Size = f.size
	return nil
}

func (f *file) Size() uint64 {
	return f.size
}

type weightFile struct {
	file
	staticWeight int
	dynamicWeght int
	content      string
	onChange     func(float32) error
}

func newWeightFile(parent string, name string, static, dynamic int, onChange func(float32) error) *weightFile {
	i := atomic.AddUint64(&inode, 1)
	content := fmt.Sprintf("%d,%d", static, dynamic)
	return &weightFile{
		file: file{
			inode:     i,
			name:      name,
			size:      uint64(len(content)),
			mode:      0644,
			parentDir: parent,
		},
		staticWeight: static,
		dynamicWeght: dynamic,
		content:      content,
		onChange:     onChange,
	}
}

func (wf *weightFile) ReadAll(ctx context.Context) ([]byte, error) {
	return []byte(wf.content), nil
}

func (wf *weightFile) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write([]byte(wf.content)[:int(req.Offset)])
	buf.Write(req.Data)
	wf.content = buf.String()
	wf.size = uint64(buf.Len())
	resp.Size = buf.Len()
	s, d, err := splitWeight(buf.String())
	if err != nil {
		logrus.WithError(err).Errorf("invalid file write to %s", path.Join(wf.parentDir, wf.name))
	} else {
		wf.staticWeight = s
		wf.dynamicWeght = d
		if wf.onChange != nil {
			if err = wf.onChange(wf.weight()); err != nil {
				logrus.WithError(err).Error("failed to call onChange handler")
			}
		}
	}
	return nil
}

func (wf *weightFile) weight() float32 {
	total := float32(wf.staticWeight) + float32(wf.dynamicWeght)/10
	if total > 20 {
		return 20
	}
	return total
}

func splitWeight(content string) (int, int, error) {
	parts := strings.Split(content, ",")
	if len(parts) != 2 {
		return 0, 0, errors.Errorf("invalid content %q", content)
	}
	static, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to convert %q to number", parts[0])
	}
	dynamic, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to convert %q to number", parts[1])
	}
	return static, dynamic, nil
}

type remoteFile struct {
	file
	reader *stargz.Reader
	bc     cache.BlobCache
}

func newRemoteFile(parent string, name string, size uint64, reader *stargz.Reader, bc cache.BlobCache) *remoteFile {
	i := atomic.AddUint64(&inode, 1)
	return &remoteFile{
		file: file{
			inode:     i,
			name:      name,
			mode:      0444,
			size:      size,
			parentDir: parent,
		},
		reader: reader,
		bc:     bc,
	}
}

func (rf *remoteFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if !req.Flags.IsReadOnly() {
		return nil, syscall.EACCES
	}
	var r *io.SectionReader
	var err error
	r, err = rf.reader.OpenFile(rf.name)
	if err != nil {
		logrus.WithError(err).Errorf("failed to open %s", path.Join(rf.parentDir, rf.name))
		return nil, syscall.EIO
	}
	h := &readHandler{
		f:       rf,
		sReader: r,
	}
	resp.Handle = h.handleID()
	logrus.Debugf("open %s for read", path.Join(rf.parentDir, rf.name))
	return h, nil
}

type staticFile struct {
	file
	content string
}

func newStaticFile(parent, name, content string) *staticFile {
	i := atomic.AddUint64(&inode, 1)
	return &staticFile{
		file: file{
			inode:     i,
			name:      name,
			mode:      0444,
			size:      uint64(len(content)),
			parentDir: parent,
		},
		content: content,
	}
}

func (sf *staticFile) ReadAll(ctx context.Context) ([]byte, error) {
	return []byte(sf.content), nil
}

type readHandler struct {
	f       *remoteFile
	sReader *io.SectionReader
}

func (rh *readHandler) handleID() fuse.HandleID {
	return fuse.HandleID(uintptr(unsafe.Pointer(rh)))
}

func (rh *readHandler) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fullName := path.Join(rh.f.parentDir, rh.f.name)
	logrus.Debugf("read %s at %v with size %v",
		fullName,
		req.Offset,
		req.Size,
	)
	result := make([]byte, req.Size)
	nr := 0
	if rh.f.bc == nil {
		// don't use cache at all
		var err error
		if nr, err = rh.sReader.ReadAt(result, req.Offset); err != nil && err != io.EOF {
			logrus.WithError(err).Errorf(
				"failed to read %s at %d with size %d from remote",
				fullName,
				req.Offset,
				req.Size,
			)
		}
	} else {
		//use cache as much as possible
		for nr < len(result) {
			ce, ok := rh.f.reader.ChunkEntryForOffset(rh.f.name, req.Offset+int64(nr))
			if !ok {
				break
			}
			var (
				id           = generateID(rh.f.name, ce.ChunkOffset, ce.ChunkSize)
				lowerDiscard = positive(req.Offset - ce.ChunkOffset)
				upperDiscard = positive(ce.ChunkOffset + ce.ChunkSize - (req.Offset + int64(len(result))))
				expectedSize = ce.ChunkSize - upperDiscard - lowerDiscard
			)
			// try to fetch in cache
			n, err := rh.f.bc.FetchAt(id, lowerDiscard, result[nr:int64(nr)+expectedSize])
			if err != nil && int64(n) == expectedSize {
				start := req.Offset + int64(nr)
				logrus.Debugf("cache hit for file %s at range %d-%d", fullName, start, start+expectedSize)
				nr += n
				continue
			}
			// missed cache, take it from underlying reader and store it to the cache
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			buf.Grow(int(ce.ChunkSize))
			ip := buf.Bytes()[:ce.ChunkSize]
			if _, err := rh.sReader.ReadAt(ip, ce.ChunkOffset); err != nil {
				logrus.WithError(err).Errorf(
					"failed to read %s at range %d-%d from remote",
					fullName,
					ce.ChunkOffset,
					ce.ChunkSize,
				)
				return syscall.EIO
			}
			n = copy(result[nr:], ip[lowerDiscard:ce.ChunkSize-upperDiscard])
			if int64(n) != expectedSize {
				logrus.Errorf("unexpected copied data size %d, expected %d", n, expectedSize)
				return syscall.EIO
			}
			// cache this chunk
			go func() {
				if err := rh.f.bc.Add(id, ip); err != nil {
					logrus.WithError(err).Errorf(
						"failed to cache range %d-%d for file %s",
						ce.ChunkOffset,
						ce.ChunkSize,
						fullName,
					)
				} else {
					logrus.Debugf(
						"cache range %d-%d for file %s",
						ce.ChunkOffset,
						ce.ChunkSize,
						fullName,
					)
				}
				bufferPool.Put(buf)
			}()
			nr += n
		}
	}
	resp.Data = result[:nr]
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

func generateID(s string, a, b int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%v-%v-%v", s, a, b)))
	return fmt.Sprintf("%x", sum)
}

func positive(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
