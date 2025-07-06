package tree

import "fmt"

type Key struct {
	Value int
}

type Node interface {
	isLeaf() bool
}

type Tree struct {
	Root  Node
	Order int
}

type IntermNode struct {
	Pointers []Node // len = len(Keys)+1
	Keys     []Key
}

func (n *IntermNode) isLeaf() bool { return false }

type LeafPair struct {
	K     Key
	Value string
}

type LeafNode struct {
	Pairs []LeafPair
	next  *LeafNode
	prev  *LeafNode
}

func (l *LeafNode) isLeaf() bool { return true }

// promoted key , the right node that is pushed upwards

func insert(val Key, addr string, node Node, order int) (*Key, Node) {
	if node.isLeaf() {
		n, ok := node.(*LeafNode)
		if !ok {
			panic("expected a leaf node")
		}

		// Extract keys
		keys := make([]Key, len(n.Pairs))
		for i, pair := range n.Pairs {
			keys[i] = pair.K
		}
		index := UpperBound(val.Value, keys)

		newElem := LeafPair{val, addr}
		//fmt.Println(index)
		newSlice := insertAt(n.Pairs, index, newElem)
		//fmt.Println(newSlice)
		if len(n.Pairs) == order-1 {
			num := int((order - 1) / 2)
			n.Pairs = newSlice[:num]
			right := newSlice[num:]

			r := &LeafNode{
				Pairs: right,
				prev:  n,
				next:  n.next,
			}
			n.next = r

			return &right[0].K, r
		}
		n.Pairs = newSlice
		return nil, nil
	}

	// --- Internal node case ---
	n, ok := node.(*IntermNode)
	if !ok {
		panic("expected an interim node")
	}

	index := UpperBound(val.Value, n.Keys)
	promotedKey, newRight := insert(val, addr, n.Pointers[index], order)

	if promotedKey == nil && newRight == nil {
		return nil, nil
	}

	// Insert promotedKey into n.Keys
	n.Keys = insertAt(n.Keys, index, *promotedKey)

	// Insert newRight into n.Pointers at index + 1
	n.Pointers = insertAt(n.Pointers, index+1, newRight)

	// insertion already done
	if len(n.Keys) == order {
		num := int((order-1) / 2)
		midKey := n.Keys[num]

		rightKeys := append([]Key{}, n.Keys[num+1:]...)
		rightPtrs := append([]Node{}, n.Pointers[num+1:]...)

		n.Keys = n.Keys[:num]
		n.Pointers = n.Pointers[:num+1]

		rightNode := &IntermNode{
			Keys:     rightKeys,
			Pointers: rightPtrs,
		}

		return &midKey, rightNode
	}

	return nil, nil
}

func (t *Tree) Insert(val Key, addr string) {
	// Initial insert into the tree
	promotedKey, newRight := insert(val, addr, t.Root, t.Order)

	// If there's no split at the root, we're done
	if promotedKey == nil && newRight == nil {
		return
	}

	// Root was split — create a new root
	newRoot := &IntermNode{
		Keys:     []Key{*promotedKey},
		Pointers: []Node{t.Root, newRight},
	}
	t.Root = newRoot
}

func dfs(val Key, node Node) string {
	// now what we have to do is find the upper bound
	if !node.isLeaf() {
		// fist we need to typecast the node to interimNode
		n, ok := node.(*IntermNode)
		if !ok {
			panic("expected an *IntermNode")
		}
		index := UpperBound(val.Value, n.Keys)
		// now we find the correspoinding tree pointer
		tp := n.Pointers[index]
		// now we call the dfs on this
		return dfs(val, tp)

	} else {
		n, ok := node.(*LeafNode)
		if !ok {
			panic("expected an *LeafNode")
		}
		ind := BinarySearch(val.Value, n.Pairs)
		if ind == -1 {
			return "not in tree"
		}
		return n.Pairs[ind].Value
	}
}

func (t *Tree) Print() {
	if t.Root == nil {
		fmt.Println("Tree is empty")
		return
	}

	type LevelNode struct {
		node  Node
		level int
	}

	queue := []LevelNode{{t.Root, 0}}
	currentLevel := 0

	fmt.Printf("Level %d: ", currentLevel)

	for len(queue) > 0 {
		// Dequeue
		item := queue[0]
		queue = queue[1:]

		if item.level != currentLevel {
			currentLevel = item.level
			fmt.Println()
			fmt.Printf("Level %d: ", currentLevel)
		}

		if item.node.isLeaf() {
			leaf := item.node.(*LeafNode)
			fmt.Print("[")
			for _, pair := range leaf.Pairs {
				fmt.Printf("(%d: %s) ", pair.K.Value, pair.Value)
			}
			fmt.Print("] ")
		} else {
			interm := item.node.(*IntermNode)
			fmt.Print("[")
			for _, k := range interm.Keys {
				fmt.Printf("%d ", k.Value)
			}
			fmt.Print("] ")
			for _, child := range interm.Pointers {
				queue = append(queue, LevelNode{child, item.level + 1})
			}
		}
	}
	fmt.Println()
}

func (t *Tree) Search(val Key) string {
	r := t.Root

	return dfs(val, r)

}

// now we write the insertion logic
