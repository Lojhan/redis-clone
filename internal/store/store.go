package store

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type ObjectType byte

const (
	ObjString ObjectType = iota
	ObjList
	ObjHash
	ObjSet
	ObjZSet
)

type ObjectEncoding byte

const (
	EncodingInt ObjectEncoding = iota
	EncodingEmbstr
	EncodingRaw

	EncodingQuicklist

	EncodingZiplist
	EncodingHT

	EncodingIntset

	EncodingSkiplist
)

const (
	EmbstrMaxSize = 44
)

type RedisObject struct {
	Type     ObjectType
	Encoding ObjectEncoding
	LRU      uint32
	RefCount int32
	Ptr      any
}

type KeyModifiedCallback func(key string)

type Store struct {
	data               map[string]*RedisObject
	expires            map[string]time.Time
	mu                 sync.RWMutex
	keyModifiedHandler KeyModifiedCallback
	evictionConfig     *EvictionConfig
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]*RedisObject),
		expires: make(map[string]time.Time),
	}
}

func (s *Store) SetKeyModifiedHandler(handler KeyModifiedCallback) {
	s.keyModifiedHandler = handler
}

func (s *Store) SetEvictionConfig(config *EvictionConfig) {
	s.evictionConfig = config
}

func (s *Store) updateLRU(obj *RedisObject) {

	obj.LRU = uint32(time.Now().Unix())
}

func (s *Store) notifyKeyModified(key string) {
	if s.keyModifiedHandler != nil {
		s.keyModifiedHandler(key)
	}
}

func (s *Store) Set(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingObj, keyExists := s.data[key]
	oldSize := int64(0)
	if keyExists {
		oldSize = EstimateObjectSize(existingObj) + EstimateKeySize(key)
	}

	obj := createStringObject(value)
	newSize := EstimateObjectSize(obj) + EstimateKeySize(key)

	s.UpdateMemoryUsage(newSize - oldSize)

	if err := s.TryEvictUntilUnderLimit(); err != nil {

		s.UpdateMemoryUsage(oldSize - newSize)
		return err
	}

	s.updateLRU(obj)
	s.data[key] = obj
	s.notifyKeyModified(key)
	return nil
}

func (s *Store) SetWithExpiry(key string, value string, expiry time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingObj, keyExists := s.data[key]
	oldSize := int64(0)
	if keyExists {
		oldSize = EstimateObjectSize(existingObj) + EstimateKeySize(key)
	}

	obj := createStringObject(value)
	newSize := EstimateObjectSize(obj) + EstimateKeySize(key)

	s.UpdateMemoryUsage(newSize - oldSize)

	if err := s.TryEvictUntilUnderLimit(); err != nil {

		s.UpdateMemoryUsage(oldSize - newSize)
		return err
	}

	s.updateLRU(obj)
	s.data[key] = obj
	s.expires[key] = expiry
	s.notifyKeyModified(key)
	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key) {
		return "", false
	}

	obj, exists := s.data[key]
	if !exists {
		return "", false
	}

	if obj.Type != ObjString {
		return "", false
	}

	s.updateLRU(obj)

	return extractString(obj), true
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return false
	}

	_, exists := s.data[key]
	return exists
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, exists := s.data[key]
	if !exists {
		return false
	}

	if s.evictionConfig != nil && s.evictionConfig.memoryTracking {
		s.evictionConfig.currentMemory -= EstimateObjectSize(obj)
		s.evictionConfig.currentMemory -= EstimateKeySize(key)
	}

	delete(s.data, key)
	delete(s.expires, key)
	s.notifyKeyModified(key)
	return true
}

func (s *Store) SetNX(key string, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists && !s.isExpired(key) {
		return false, nil
	}

	existingObj, keyExists := s.data[key]
	oldSize := int64(0)
	if keyExists {
		oldSize = EstimateObjectSize(existingObj) + EstimateKeySize(key)
	}

	obj := createStringObject(value)
	newSize := EstimateObjectSize(obj) + EstimateKeySize(key)

	s.UpdateMemoryUsage(newSize - oldSize)

	if err := s.TryEvictUntilUnderLimit(); err != nil {

		s.UpdateMemoryUsage(oldSize - newSize)
		return false, err
	}

	s.updateLRU(obj)
	s.data[key] = obj
	delete(s.expires, key)
	s.notifyKeyModified(key)
	return true, nil
}

func (s *Store) SetXX(key string, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; !exists || s.isExpired(key) {
		return false, nil
	}

	existingObj := s.data[key]
	oldSize := EstimateObjectSize(existingObj) + EstimateKeySize(key)

	obj := createStringObject(value)
	newSize := EstimateObjectSize(obj) + EstimateKeySize(key)

	s.UpdateMemoryUsage(newSize - oldSize)

	if err := s.TryEvictUntilUnderLimit(); err != nil {

		s.UpdateMemoryUsage(oldSize - newSize)
		return false, err
	}

	s.updateLRU(obj)
	s.data[key] = obj
	s.notifyKeyModified(key)
	return true, nil
}

func (s *Store) GetType(key string) (ObjectType, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, false
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, false
	}

	return obj.Type, true
}

func (s *Store) LPush(key string, values ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		list.PushHead(value)
	}

	s.notifyKeyModified(key)
	return int64(list.Len()), nil
}

func (s *Store) RPush(key string, values ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		list.PushTail(value)
	}

	s.notifyKeyModified(key)
	return int64(list.Len()), nil
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.getList(key)
	if !ok {
		return "", false
	}

	value, ok := list.PopHead()
	if !ok {
		return "", false
	}

	if list.Len() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	s.notifyKeyModified(key)
	return value, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.getList(key)
	if !ok {
		return "", false
	}

	value, ok := list.PopTail()
	if !ok {
		return "", false
	}

	if list.Len() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	s.notifyKeyModified(key)
	return value, true
}

func (s *Store) LLen(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := obj.Ptr.(*Quicklist)
	if !ok {
		return 0, fmt.Errorf("invalid list encoding")
	}

	return int64(list.Len()), nil
}

func (s *Store) LRange(key string, start, stop int64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.getList(key)
	if !ok {
		return []string{}, nil
	}

	return list.Range(start, stop), nil
}

func (s *Store) getList(key string) (*Quicklist, bool) {
	if s.isExpired(key) {
		return nil, false
	}

	obj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	if obj.Type != ObjList {
		return nil, false
	}

	list, ok := obj.Ptr.(*Quicklist)
	return list, ok
}

func (s *Store) getOrCreateList(key string) (*Quicklist, error) {
	if s.isExpired(key) {
		delete(s.data, key)
		delete(s.expires, key)
	}

	obj, exists := s.data[key]
	if !exists {

		list := NewQuicklist()
		s.data[key] = &RedisObject{
			Type:     ObjList,
			Encoding: EncodingQuicklist,
			Ptr:      list,
		}
		return list, nil
	}

	if obj.Type != ObjList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list, ok := obj.Ptr.(*Quicklist)
	if !ok {
		return nil, fmt.Errorf("invalid list encoding")
	}

	return list, nil
}

func (s *Store) HSet(key, field, value string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash, err := s.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	isNew := hash.Set(field, value)
	s.notifyKeyModified(key)
	if isNew {
		return 1, nil
	}
	return 0, nil
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.getHash(key)
	if !ok {
		return "", false
	}

	return hash.Get(field)
}

func (s *Store) HDel(key string, fields ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hash, ok := obj.Ptr.(*HashTable)
	if !ok {
		return 0, fmt.Errorf("invalid hash encoding")
	}

	count := int64(0)
	for _, field := range fields {
		if hash.Delete(field) {
			count++
		}
	}

	if hash.Len() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	if count > 0 {
		s.notifyKeyModified(key)
	}
	return count, nil
}

func (s *Store) HExists(key, field string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.getHash(key)
	if !ok {
		return false
	}

	return hash.Exists(field)
}

func (s *Store) HLen(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjHash {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hash, ok := obj.Ptr.(*HashTable)
	if !ok {
		return 0, fmt.Errorf("invalid hash encoding")
	}

	return int64(hash.Len()), nil
}

func (s *Store) HGetAll(key string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.getHash(key)
	if !ok {
		return make(map[string]string), nil
	}

	return hash.GetAll(), nil
}

func (s *Store) HKeys(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.getHash(key)
	if !ok {
		return []string{}, nil
	}

	return hash.Fields(), nil
}

func (s *Store) HVals(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.getHash(key)
	if !ok {
		return []string{}, nil
	}

	return hash.Values(), nil
}

func (s *Store) getHash(key string) (*HashTable, bool) {
	if s.isExpired(key) {
		return nil, false
	}

	obj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	if obj.Type != ObjHash {
		return nil, false
	}

	hash, ok := obj.Ptr.(*HashTable)
	return hash, ok
}

func (s *Store) getOrCreateHash(key string) (*HashTable, error) {
	if s.isExpired(key) {
		delete(s.data, key)
		delete(s.expires, key)
	}

	obj, exists := s.data[key]
	if !exists {

		hash := NewHashTable()
		s.data[key] = &RedisObject{
			Type:     ObjHash,
			Encoding: EncodingHT,
			Ptr:      hash,
		}
		return hash, nil
	}

	if obj.Type != ObjHash {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	hash, ok := obj.Ptr.(*HashTable)
	if !ok {
		return nil, fmt.Errorf("invalid hash encoding")
	}

	return hash, nil
}

func (s *Store) SAdd(key string, members ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, err := s.getOrCreateSet(key)
	if err != nil {
		return 0, err
	}

	added := set.Add(members...)
	if added > 0 {
		s.notifyKeyModified(key)
	}
	return int64(added), nil
}

func (s *Store) SRem(key string, members ...string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	set, ok := obj.Ptr.(*Set)
	if !ok {
		return 0, fmt.Errorf("invalid set encoding")
	}

	removed := set.Remove(members...)

	if set.Card() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	if removed > 0 {
		s.notifyKeyModified(key)
	}
	return int64(removed), nil
}

func (s *Store) SIsMember(key, member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set, ok := s.getSet(key)
	if !ok {
		return false
	}

	return set.IsMember(member)
}

func (s *Store) SMembers(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set, ok := s.getSet(key)
	if !ok {
		return []string{}, nil
	}

	return set.Members(), nil
}

func (s *Store) SCard(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	set, ok := obj.Ptr.(*Set)
	if !ok {
		return 0, fmt.Errorf("invalid set encoding")
	}

	return int64(set.Card()), nil
}

func (s *Store) SPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.getSet(key)
	if !ok {
		return "", false
	}

	member, ok := set.Pop()
	if !ok {
		return "", false
	}

	if set.Card() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	s.notifyKeyModified(key)
	return member, true
}

func (s *Store) getSet(key string) (*Set, bool) {
	if s.isExpired(key) {
		return nil, false
	}

	obj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	if obj.Type != ObjSet {
		return nil, false
	}

	set, ok := obj.Ptr.(*Set)
	return set, ok
}

func (s *Store) getOrCreateSet(key string) (*Set, error) {
	if s.isExpired(key) {
		delete(s.data, key)
		delete(s.expires, key)
	}

	obj, exists := s.data[key]
	if !exists {

		set := NewSet()
		s.data[key] = &RedisObject{
			Type:     ObjSet,
			Encoding: EncodingHT,
			Ptr:      set,
		}
		return set, nil
	}

	if obj.Type != ObjSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	set, ok := obj.Ptr.(*Set)
	if !ok {
		return nil, fmt.Errorf("invalid set encoding")
	}

	return set, nil
}

func (s *Store) ZAdd(key string, score float64, member string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	zset, err := s.getOrCreateZSet(key)
	if err != nil {
		return 0, err
	}

	added := zset.Add(score, member)
	s.notifyKeyModified(key)
	if added {
		return 1, nil
	}
	return 0, nil
}

func (s *Store) ZRem(key string, member string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := obj.Ptr.(*ZSet)
	if !ok {
		return 0, fmt.Errorf("invalid zset encoding")
	}

	removed := zset.Remove(member)

	if zset.Card() == 0 {
		delete(s.data, key)
		delete(s.expires, key)
	}

	if removed {
		s.notifyKeyModified(key)
		return 1, nil
	}
	return 0, nil
}

func (s *Store) ZScore(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zset, ok := s.getZSet(key)
	if !ok {
		return 0, false
	}

	return zset.Score(member)
}

func (s *Store) ZCard(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, nil
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, nil
	}

	if obj.Type != ObjZSet {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := obj.Ptr.(*ZSet)
	if !ok {
		return 0, fmt.Errorf("invalid zset encoding")
	}

	return int64(zset.Card()), nil
}

func (s *Store) ZRank(key, member string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zset, ok := s.getZSet(key)
	if !ok {
		return -1, false
	}

	return zset.Rank(member)
}

func (s *Store) ZRange(key string, start, stop int64) ([]ZSetMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zset, ok := s.getZSet(key)
	if !ok {
		return []ZSetMember{}, nil
	}

	return zset.Range(start, stop), nil
}

func (s *Store) getZSet(key string) (*ZSet, bool) {
	if s.isExpired(key) {
		return nil, false
	}

	obj, exists := s.data[key]
	if !exists {
		return nil, false
	}

	if obj.Type != ObjZSet {
		return nil, false
	}

	zset, ok := obj.Ptr.(*ZSet)
	return zset, ok
}

func (s *Store) getOrCreateZSet(key string) (*ZSet, error) {
	if s.isExpired(key) {
		delete(s.data, key)
		delete(s.expires, key)
	}

	obj, exists := s.data[key]
	if !exists {

		zset := NewZSet()
		s.data[key] = &RedisObject{
			Type:     ObjZSet,
			Encoding: EncodingSkiplist,
			Ptr:      zset,
		}
		return zset, nil
	}

	if obj.Type != ObjZSet {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset, ok := obj.Ptr.(*ZSet)
	if !ok {
		return nil, fmt.Errorf("invalid zset encoding")
	}

	return zset, nil
}

func (s *Store) isExpired(key string) bool {
	expiry, hasExpiry := s.expires[key]
	if !hasExpiry {
		return false
	}

	if time.Now().After(expiry) {

		delete(s.data, key)
		delete(s.expires, key)
		return true
	}

	return false
}

func (s *Store) SetObject(key string, obj *RedisObject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = obj
	s.notifyKeyModified(key)
}

func (s *Store) SetObjectExpire(key string, expiry time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expires[key] = expiry
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		if !s.isExpired(key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for key := range s.data {
		if !s.isExpired(key) {
			count++
		}
	}
	return count
}

func (s *Store) Snapshot() (map[string]*RedisObject, map[string]time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dataCopy := make(map[string]*RedisObject, len(s.data))
	expiresCopy := make(map[string]time.Time, len(s.expires))

	for key, obj := range s.data {
		if !s.isExpired(key) {
			dataCopy[key] = obj
			if exp, ok := s.expires[key]; ok {
				expiresCopy[key] = exp
			}
		}
	}

	return dataCopy, expiresCopy
}

func (s *Store) RestoreSnapshot(data map[string]*RedisObject, expires map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	s.expires = expires
}

func (s *Store) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*RedisObject)
	s.expires = make(map[string]time.Time)
}

func createStringObject(value string) *RedisObject {

	if num, err := strconv.ParseInt(value, 10, 64); err == nil {
		return &RedisObject{
			Type:     ObjString,
			Encoding: EncodingInt,
			Ptr:      num,
		}
	}

	if len(value) <= EmbstrMaxSize {
		return &RedisObject{
			Type:     ObjString,
			Encoding: EncodingEmbstr,
			Ptr:      value,
		}
	}

	return &RedisObject{
		Type:     ObjString,
		Encoding: EncodingRaw,
		Ptr:      value,
	}
}

func extractString(obj *RedisObject) string {
	switch obj.Encoding {
	case EncodingInt:
		return strconv.FormatInt(obj.Ptr.(int64), 10)
	case EncodingEmbstr, EncodingRaw:
		return obj.Ptr.(string)
	default:
		return ""
	}
}

func (s *Store) GetEncoding(key string) (ObjectEncoding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpired(key) {
		return 0, false
	}

	obj, exists := s.data[key]
	if !exists {
		return 0, false
	}

	return obj.Encoding, true
}
