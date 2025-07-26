package tree

import "fmt"

func insertAt[T any](s []T, index int, value T) []T {
	if index < 0 || index > len(s) {
		panic("insertAt: index out of range")
	}
	res := make([]T, len(s)+1)
	copy(res, s[:index])
	res[index] = value
	copy(res[index+1:], s[index:])
	return res
}

// removeAt removes an element at the given index from a slice.
func removeAt[T any](s []T, index int) []T {
	if index < 0 || index >= len(s) {
		panic("removeAt: index out of range")
	}
	return append(s[:index], s[index+1:]...)
}

// upperBound finds the first index where key < keys[index] is false.
func upperBound[K Key](key K, keys []K) int {
	l, r := 0, len(keys)
	for l < r {
		m := (l + r) / 2
		if key.Less(keys[m]) {
			r = m
		} else {
			l = m + 1
		}
	}
	return l
}

// leafUpperBound finds the index to insert a key in a leaf node.
func leafUpperBound[K Key, V any](key K, pairs []LeafPair[K, V]) int {
	l, r := 0, len(pairs)
	for l < r {
		m := (l + r) / 2
		if key.Less(pairs[m].K) {
			r = m
		} else {
			l = m + 1
		}
	}
	return l
}

// leafBinarySearch searches for a key in a leaf node.
func leafBinarySearch[K Key, V any](key K, pairs []LeafPair[K, V]) int {
	l, r := 0, len(pairs)-1
	for l <= r {
		m := (l + r) / 2
		if pairs[m].K.Equal(key) {
			return m
		} else if key.Less(pairs[m].K) {
			r = m - 1
		} else {
			l = m + 1
		}
	}
	return -1
}

func (t *Tree[K, V]) Print() {
	if t.Root == nil {
		fmt.Println("Tree is empty")
		return
	}
	type LevelNode struct {
		node  Node[V]
		level int
	}
	queue := []LevelNode{{t.Root, 0}}
	currentLevel := 0
	fmt.Printf("Level %d: ", currentLevel)
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		if item.level != currentLevel {
			currentLevel = item.level
			fmt.Println()
			fmt.Printf("Level %d: ", currentLevel)
		}
		if item.node.isLeaf() {
			leaf := item.node.(*LeafNode[K, V])
			fmt.Print("[")
			for _, pair := range leaf.Pairs {
				fmt.Printf("(%v: %v) ", pair.K, pair.Value)
			}
			fmt.Print("] ")
		} else {
			interm := item.node.(*IntermNode[K, V])
			fmt.Print("[")
			for _, k := range interm.Keys {
				fmt.Printf("%v ", k)
			}
			fmt.Print("] ")
			for _, child := range interm.Pointers {
				queue = append(queue, LevelNode{child, item.level + 1})
			}
		}
	}
	fmt.Println()
}
