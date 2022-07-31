package collections_helper

import "sync"

type dType interface {
	int | int32 | int64 | float32 | float64 | string
}

type Set[T dType] struct {
	m      map[T]struct{}
	rwLock sync.RWMutex
}

func (s *Set[T]) initMap() {
	if s.m == nil {
		s.rwLock.Lock()
		if s.m == nil {
			s.m = make(map[T]struct{})
		}
		s.rwLock.Unlock()
	}
}

func (s *Set[T]) Add(data ...T) {
	s.initMap()
	s.rwLock.Lock()
	for _, d := range data {
		s.m[d] = struct{}{}
	}
	s.rwLock.Unlock()
}

func (s *Set[T]) Remove(data T) {
	s.initMap()
	s.rwLock.Lock()
	delete(s.m, data)
	s.rwLock.Unlock()
}

func (s *Set[T]) Has(data T) bool {
	s.initMap()
	s.rwLock.RLock()
	_, ok := s.m[data]
	s.rwLock.RUnlock()
	return ok
}

func (s *Set[T]) Clear() {
	s.rwLock.Lock()
	s.m = make(map[T]struct{}, 0)
	s.rwLock.Unlock()
}

func (s Set[T]) Size() int {
	s.initMap()
	s.rwLock.RLock()
	num := len(s.m)
	s.rwLock.RUnlock()
	return num
}

func SliceToSet[T dType](arr []T) *Set[T] {
	s := &Set[T]{}
	for _, data := range arr {
		s.Add(data)
	}
	return s
}

func MapKeyToSet[kt dType, vt dType](m map[kt]vt) *Set[kt] {
	s := &Set[kt]{}
	for key, _ := range m {
		s.Add(key)
	}
	return s
}

func MapValueToSet[kt dType, vt dType](m map[kt]vt) *Set[vt] {
	s := &Set[vt]{}
	for _, v := range m {
		s.Add(v)
	}
	return s
}
