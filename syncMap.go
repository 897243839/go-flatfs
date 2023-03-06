package flatfs

import (
	"sync"
)

type KeyValue struct {
	sync.RWMutex
	data map[string]int
}
func NewKeyValue() *KeyValue {
	return &KeyValue{data: make(map[string]int)}
}
func (kv *KeyValue) Range(f func(key string, value int)) {
	//kv.RLock()
	//defer kv.RUnlock()
	for key, value := range kv.data {
		f(key, value)
	}
}
func (kv *KeyValue) Get(key string) (int, bool) {
	kv.RLock()
	defer kv.RUnlock()
	value, ok := kv.data[key]
	return value, ok
}
func (kv *KeyValue) Count() int {
	kv.RLock()
	defer kv.RUnlock()
	count:=len(kv.data)
	return count
}
func (kv *KeyValue) Set(key string, value int) {
	kv.Lock()
	defer kv.Unlock()
	kv.data[key] = value
}
func (kv *KeyValue) Incr(key string, delta int) {
	kv.Lock()
	defer kv.Unlock()
	kv.data[key] += delta
}
func (kv *KeyValue) Decr(key string, delta int) {
	kv.Lock()
	defer kv.Unlock()
	kv.data[key] -= delta
}
func (kv *KeyValue) Delete(key string) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.data, key)
}
func (kv *KeyValue) Clear() {
	kv.Lock()
	defer kv.Unlock()
	kv.data = make(map[string]int)
}

type KeyByte struct {
	sync.RWMutex
	data map[string][]byte
}
func NewKeyByte() *KeyByte {
	return &KeyByte{data: make(map[string][]byte)}
}
func (kv *KeyByte) RangeByte(f func(key string, value []byte)) {
	//kv.RLock()
	//defer kv.RUnlock()
	for key, value := range kv.data {
		f(key, value)
	}
}
func (kv *KeyByte) GetByte(key string) ([]byte, bool) {
	kv.RLock()
	defer kv.RUnlock()
	value, ok := kv.data[key]
	return value, ok
}
func (kv *KeyByte) CountByte() int {
	kv.RLock()
	defer kv.RUnlock()
	count:=len(kv.data)
	return count
}
func (kv *KeyByte) SetByte(key string, value []byte) {
	kv.Lock()
	defer kv.Unlock()
	kv.data[key] = value
}

func (kv *KeyByte) DeleteByte(key string) {
	kv.Lock()
	defer kv.Unlock()
	delete(kv.data, key)
}
func (kv *KeyByte) ClearByte() {
	kv.Lock()
	defer kv.Unlock()
	kv.data = make(map[string][]byte)
}
