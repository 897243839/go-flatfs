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
			if hc.Maphot.Count() < hc.Num {
				ps.dohotPut(key)
			} else {
				time.Sleep(30)
			}
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
}
func UpdateMaphot() {

	for key, v := range hc.Maphot.Items() {
		if v <= 9 {
			dir := filepath.Join(ps.path, ps.getDir(key))
			file := filepath.Join(dir, key+extension)
			ps.Get_writer(dir, file)
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
	if hc.GetCompressorType(val) == hc.UnknownCompressor {
		fmt.Printf("不需要dohot\n")
		return nil
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
	hc.Maphot.Set(key, 1)
	hc.Maphot.Remove(key)
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
