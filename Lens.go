package flatfs

//源数据块的解压缩文件
import (
	//"context"
	"encoding/json"
	hc "github.com/897243839/HcdComp"
	"time"
	//"errors"
	"fmt"
	"github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	//"math"
	//"math/rand"
	"os"
	"path/filepath"
)

var ps = &Datastore{}

func putfs(fs *Datastore) {
	ps = fs
}
func listencool() {
	for {
		select {
		case <-hc.Ticker.C:
			hc.MapLit.Clear()
		case key := <-hc.Tsf:
			println("热转换块等待：", key)
			if hc.Maphot.Count() < hc.Num {
				ps.dohotPut(key)
				println("热转换：", key)
			} else {
				time.Sleep(30)
				ps.dohotPut(key)
			}
		default:
		}
	}
}
func listenkey() {
	for {
		select {
		case c := <-hc.Hck:
			key := dshelp.MultihashToDsKey(c.Hash()).String()[1:]
			println("更新热数据使用次数：", key)
			v, ok := hc.Maphot.Get(key)
			if !ok {
				hc.Maphot.Set(key, 1)
			}
			if v > 999 {
			} else {
				v += 1
				hc.Maphot.Set(key, v)
			}
		case c := <-hc.Clk:
			key := dshelp.MultihashToDsKey(c.Hash()).String()[1:]
			println("cool数据使用：", key)
			v, ok := hc.MapLit.Get(key)
			if !ok {
				hc.MapLit.Set(key, 1)
			}
			if v == 5 {
				hc.Tsf <- key
				v += 1
				hc.MapLit.Set(key, v)
			} else if v < 5 {
				v += 1
				hc.MapLit.Set(key, v)
			}
		case key := <-hc.Hotk:
			println("add数据：", key)
			hc.Maphot.Set(key, 1)
		default:
		}
	}
}
func listenhot() {
	for {
		select {
		case <-hc.Ticker1.C:
			UpdateMaphot()
			if hc.Maphot.Count() < 800000 {
				hc.Num = 1000000
			} else {
				hc.Num = 2000000
			}
			fmt.Println("更新本地热数据表成功")
		default:
			if hc.Maphot.Count() >= hc.Num {
				UpdateMaphot()
				if hc.Maphot.Count() < 800000 {
					hc.Num = 1000000
				} else {
					hc.Num = 2000000
				}
			}

		}
	}
}
func init() {
	go listencool()
	go listenhot()
	go listenkey()
}
func UpdateMaphot() {

	for key, v := range hc.Maphot.Items() {
		if v <= 9 {
			dir := filepath.Join(ps.path, ps.getDir(key))
			file := filepath.Join(dir, key+extension)
			startTime := time.Now()
			ps.Get_writer(dir, file)
			dur := time.Since(startTime)
			fmt.Printf("冷热转换写时间：%s\n", dur)
			hc.Maphot.Remove(key)
		} else {
			hc.Maphot.Set(key, 1)
		}
	}
	mapw := hc.Maphot.Items()
	ps.WriteJson(mapw, true, hc.Block_hot, ps.path)
	fmt.Println("本地热数据更新&&保存成功")
}
func (fs *Datastore) dohotPut(key string) error {

	dir := filepath.Join(ps.path, ps.getDir(key))
	if err := fs.makeDir(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, key+extension)
	val, err := readFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return err
	}
	comptype := hc.GetCompressorType(val)
	if comptype == hc.UnknownCompressor {
		fmt.Printf("不需要dohot\n")
		return nil
	}
	val = hc.Decompress(val, comptype)
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
	hc.Maphot.Set(key, 1)
	hc.MapLit.Remove(key)
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
	if hc.GetCompressorType(data) != hc.UnknownCompressor {
		fmt.Printf("get_writer触发-已压缩\n")
		return nil
	}
	data = hc.Compress(data, hc.Mode)
	if _, err := tmp.Write(data); err != nil {
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
func (fs *Datastore) readJson(path string, name string) (map[string]int, int) {
	fpath := filepath.Join(path, name)
	duB, err := readFile(fpath)
	if err != nil {
		println("读json错误")
		return nil, 0
	}
	temp := make(map[string]int)
	err = json.Unmarshal(duB, &temp)
	if err != nil {
		println("读json错误")
		return nil, 0
	}

	return temp, 1
}
func (fs *Datastore) WriteJson(hot map[string]int, doSync bool, name string, path string) {
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
	if err := rename(tmp.Name(), filepath.Join(path, name)); err != nil {
		log.Warnw("cound not write block hot", "error", err)
		return
	}
	removed = true
}
