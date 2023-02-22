package flatfs

//源数据块的解压缩文件
import (
	//"context"
	"encoding/json"
	//"errors"
	"fmt"
	//"math"
	//"math/rand"
	"github.com/ipfs/go-datastore"
	"os"
	"path/filepath"
	"strings"
	"sync"
	//"sync/atomic"
	//"syscall"
	"time"
	//cid "github.com/ipfs/go-cid"
	//dshelp "github.com/ipfs/go-ipfs-ds-help"

	"archive/zip"
	"compress/zlib"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"

	"bytes"
	"io"
	"io/ioutil"
)

var maps sync.RWMutex
var mapLit = New[int]()

// var myTimer = time.Now().Unix() // 启动定时器
var ticker = time.NewTicker(60 * time.Second)   //计时器
var ticker1 = time.NewTicker(300 * time.Minute) //计时器

// var hclist = make(map[string][]byte)
var hclist = New[[]byte]()
var cb = func(exists bool, valueInMap int, newValue int) int {
	if !exists {
		return newValue
	}
	valueInMap += newValue
	return valueInMap
}
var ps = &Datastore{}

func putfs(fs *Datastore) {
	ps = fs
}
func init() {
	go func() {
		for {
			select {
			case <-ticker.C:
				Pr()
				updata_hc()
				//default:
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ticker1.C:
				for key, v := range maphot.Items() {
					if v <= 9 {
						dir := filepath.Join(ps.path, ps.getDir(key))
						file := filepath.Join(dir, key+extension)
						ps.Get_writer(dir, file)
						maphot.Remove(key)
						mapw := maphot.Items()
						ps.WriteBlockhotFile(mapw, true)
					} else {
						maphot.Set(key, 1)
					}
				}
				fmt.Println("更新本地热数据表成功")
			}
		}

	}()
}
func hc(key string) ([]byte, bool) {
	data, f := hclist.Get(key)
	return data, f
}
func put_hc(key string, data []byte) {
	hclist.Set(key, data)
}
func updata_hc() {
	println("缓冲大小", hclist.Count())
	hclist.Clear()
	println("缓冲大小", hclist.Count())
}

// lz4解压缩
func Lz4_compress(val []byte) (value []byte) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	writer.Write(val)
	writer.Close()

	return buf.Bytes()
}
func Lz4_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	//var out bytes.Buffer
	r := lz4.NewReader(b)
	//io.Copy(&out, r)
	val, err := ioutil.ReadAll(r)
	if err != nil {
		println("解压错误", err)
		return data
	}

	return val
}

// snappy解压缩
func Snappy_compress(val []byte) (value []byte) {

	//---------------压缩

	var buf bytes.Buffer
	writer := snappy.NewBufferedWriter(&buf)
	writer.Write(val)
	writer.Close()

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Snappy_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	//var out bytes.Buffer
	r := snappy.NewReader(b)
	val, err := ioutil.ReadAll(r)
	if err != nil {
		println("解压错误", err)
		return data
	}
	//io.Copy(&out,val)
	return val
}

// zip解压缩
func Zip_compress(val []byte) (value []byte) {

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------
	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	wr, _ := w.CreateHeader(&zip.FileHeader{
		Name:   fmt.Sprintf("block"),
		Method: zip.Deflate, // avoid Issue 6136 and Issue 6138
	})
	wr.Write(val)
	if err := w.Close(); err != nil {
		fmt.Println(err)
	}
	//fmt.Println(len(val))
	//fmt.Println(len(buf.Bytes()))
	return buf.Bytes()
}
func Zip_decompress(data []byte) (value []byte) {
	//---------------------------解压
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		fmt.Println(err)
		return data
	}
	r, _ := zr.File[0].Open()
	defer r.Close()
	//var out bytes.Buffer
	//_, err = io.Copy(&out, r)
	val, err := ioutil.ReadAll(r)
	if err != nil {
		println("解压错误", err)
		return data
	}
	return val
}

// zlib解压缩
func Zlib_compress(val []byte) (value []byte) {

	//---------------压缩
	var buf bytes.Buffer
	compressor := zlib.NewWriter(&buf)
	compressor.Write(val)
	compressor.Close()
	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Zlib_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	var out bytes.Buffer
	r, err := zlib.NewReader(b)
	if err != nil {
		println("解压错误", err)
		return data
	}
	io.Copy(&out, r)
	return out.Bytes()

}

// Zstd解压缩
func Zstd_compress(val []byte) (value []byte) {

	var buf bytes.Buffer
	writer, _ := zstd.NewWriter(&buf)
	writer.Write(val)
	writer.Close()

	//fmt.Println("put------------")
	////	//fmt.Println(val)
	////	//fmt.Println(buf.Bytes())
	//fmt.Println(len(buf.Bytes()))
	//fmt.Println(len(val))
	//fmt.Println("put------------")
	//----------

	return buf.Bytes()
}
func Zstd_decompress(data []byte) (value []byte) {
	//---------------------------解压
	b := bytes.NewReader(data)
	//var out bytes.Buffer
	r, err := zstd.NewReader(b)
	val, err := ioutil.ReadAll(r)
	if err != nil {
		println("解压错误", err)
		return data
	}
	//io.Copy(&out, r)
	return val
}

func Pr() {
	mapLit.Clear()
}
func Jl(key string) {
	//------------------------------------------------------------
	//s:= dshelp.MultihashToDsKey(k.Hash()).String()
	s := key
	s = strings.Replace(s, "/", "", -1)
	n, _ := mapLit.Get(s)
	if n < 99 {
		mapLit.Upsert(s, 1, cb)
	}

	//var endtime =time.Now().Unix()
	//stime:=endtime-myTimer
	//// do sth repeatly
	//if stime>=30{
	//	fmt.Println("-------------------------------")
	//	for i,n:= range mapLit{
	//		fmt.Println(i,n)
	//	}
	//	fmt.Println("-------------------------------")
	//	mapLit = make(map[string]int, 1000)
	//	myTimer =time.Now().Unix()
	//
	//}
}
func Deljl(key string) {
	//---------------------------------------------------------------------
	s := key
	s = strings.Replace(s, "/", "", -1)
	mapLit.Remove(s)

}
func getmap(key string) int {
	n, _ := mapLit.Get(key)
	return n
}
func (fs *Datastore) dohotPut(key datastore.Key, val []byte) error {

	dir, path := fs.encode(key)
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Datastore) Get_writer(dir string, path string) (err error) {

	data, err := readFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return err
	}

	fs.shutdownLock.RLock()
	defer fs.shutdownLock.RUnlock()
	if fs.shutdown {
		return ErrClosed
	}

	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}

	//压缩

	//Jl(key.String())
	va := Zlib_compress(data)
	if _, err := tmp.Write(va); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())
	fmt.Printf("get_writer触发\n")

	return nil
}

// readBlockhotFile is only safe to call in Open()
func (fs *Datastore) readBlockhotFile() int64 {
	fpath := filepath.Join(fs.path, block_hot)
	duB, err := readFile(fpath)
	if err != nil {
		return 0
	}
	var temp = make(map[string]int)
	err = json.Unmarshal(duB, &temp)
	maphot.MSet(temp)
	if err != nil {
		return 0
	}
	return 1
}
func (fs *Datastore) WriteBlockhotFile(hot map[string]int, doSync bool) {
	tmp, err := fs.tempFile()
	if err != nil {
		log.Warnw("could not write hot usage", "error", err)
		return
	}

	removed := false
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}

	}()

	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(hot); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	if doSync {
		if err := tmp.Sync(); err != nil {
			log.Warnw("cound not sync", "error", err, "file", DiskUsageFile)
			return
		}
	}
	if err := tmp.Close(); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	closed = true
	if err := rename(tmp.Name(), filepath.Join(fs.path, block_hot)); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	removed = true
}
