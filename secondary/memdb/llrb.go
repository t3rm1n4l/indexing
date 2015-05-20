// Copyright 2010 Petar Maymounkov. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A Left-Leaning Red-Black (LLRB) implementation of 2-3 balanced binary search trees,
// based on the following work:
//
//   http://www.cs.princeton.edu/~rs/talks/LLRB/08Penn.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/LLRB.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java
//
//  2-3 trees (and the run-time equivalent 2-3-4 trees) are the de facto standard BST
//  algoritms found in implementations of Python, Java, and other libraries. The LLRB
//  implementation of 2-3 trees is a recent improvement on the traditional implementation,
//  observed and documented by Robert Sedgewick.
//
package memdb

// Tree is a Left-Leaning Red-Black (LLRB) implementation of 2-3 trees
type LLRB struct {
	count int
	root  *Node
}

type Node struct {
	Item
	Left, Right *Node // Pointers to left and right child nodes
	Black       bool  // If set, the color of the link (incoming from the parent) is black
	// In the LLRB, new nodes are always red, hence the zero-value for node
}

//
func less(x, y Item) bool {
	if x == pinf {
		return false
	}
	if x == ninf {
		return true
	}
	return x.Less(y)
}

// Inf returns an Item that is "bigger than" any other item, if sign is positive.
// Otherwise  it returns an Item that is "smaller than" any other item.
func Inf(sign int) Item {
	if sign == 0 {
		panic("sign")
	}
	if sign > 0 {
		return pinf
	}
	return ninf
}

var (
	ninf = nInf{}
	pinf = pInf{}
)

type nInf struct{}

func (nInf) Less(Item) bool {
	return true
}

type pInf struct{}

func (pInf) Less(Item) bool {
	return false
}

// New() allocates a new tree
func NewLLRB() *LLRB {
	return &LLRB{}
}

// SetRoot sets the root node of the tree.
// It is intended to be used by functions that deserialize the tree.
func (t *LLRB) SetRoot(r *Node) {
	t.root = r
}

// Root returns the root node of the tree.
// It is intended to be used by functions that serialize the tree.
func (t *LLRB) Root() *Node {
	return t.root
}

// Len returns the number of nodes in the tree.
func (t *LLRB) Len() int { return t.count }

// Has returns true if the tree contains an element whose order is the same as that of key.
func (t *LLRB) Has(key Item) bool {
	return t.Get(key) != nil
}

// Get retrieves an element from the tree whose order is the same as that of key.
func (t *LLRB) Get(key Item) Item {
	h := t.root
	for h != nil {
		switch {
		case less(key, h.Item):
			h = h.Left
		case less(h.Item, key):
			h = h.Right
		default:
			return h.Item
		}
	}
	return nil
}

// Min returns the minimum element in the tree.
func (t *LLRB) Min() Item {
	h := t.root
	if h == nil {
		return nil
	}
	for h.Left != nil {
		h = h.Left
	}
	return h.Item
}

// Max returns the maximum element in the tree.
func (t *LLRB) Max() Item {
	h := t.root
	if h == nil {
		return nil
	}
	for h.Right != nil {
		h = h.Right
	}
	return h.Item
}

// InsertNoReplace inserts item into the tree. If an existing
// element has the same order, both elements remain in the tree.
func (t *LLRB) InsertNoReplace(item Item) {
	if item == nil {
		panic("inserting nil item")
	}
	t.root = t.insertNoReplace(t.root, item)
	t.root.Black = true
	t.count++
}

func (t *LLRB) insertNoReplace(h *Node, item Item) *Node {
	if h == nil {
		return newNode(item)
	}

	h = walkDownRot23(h)

	if less(item, h.Item) {
		h.Left = t.insertNoReplace(h.Left, item)
	} else {
		h.Right = t.insertNoReplace(h.Right, item)
	}

	return walkUpRot23(h)
}

// Rotation driver routines for 2-3 algorithm

func walkDownRot23(h *Node) *Node { return copyNode(h) }

func walkUpRot23(h *Node) *Node {
	if isRed(h.Right) && !isRed(h.Left) {
		h = rotateLeft(h)
	}

	if isRed(h.Left) && isRed(h.Left.Left) {
		h = rotateRight(h)
	}

	if isRed(h.Left) && isRed(h.Right) {
		h = flip(h)
	}

	return h
}

// DeleteMin deletes the minimum element in the tree and returns the
// deleted item or nil otherwise.
func (t *LLRB) DeleteMin() Item {
	var deleted Item
	t.root, deleted = deleteMin(t.root)
	if t.root != nil {
		t.root.Black = true
	}
	if deleted != nil {
		t.count--
	}
	return deleted
}

// deleteMin code for LLRB 2-3 trees
func deleteMin(h *Node) (*Node, Item) {
	if h == nil {
		return nil, nil
	}
	if h.Left == nil {
		return nil, h.Item
	}

	if !isRed(h.Left) && !isRed(h.Left.Left) {
		h = moveRedLeft(h)
	}

	var deleted Item
	h = copyNode(h)
	h.Left, deleted = deleteMin(h.Left)

	return fixUp(h), deleted
}

// DeleteMax deletes the maximum element in the tree and returns
// the deleted item or nil otherwise
func (t *LLRB) DeleteMax() Item {
	var deleted Item
	t.root, deleted = deleteMax(t.root)
	if t.root != nil {
		t.root.Black = true
	}
	if deleted != nil {
		t.count--
	}
	return deleted
}

func deleteMax(h *Node) (*Node, Item) {
	if h == nil {
		return nil, nil
	}
	if isRed(h.Left) {
		h = rotateRight(h)
	}
	if h.Right == nil {
		return nil, h.Item
	}
	if !isRed(h.Right) && !isRed(h.Right.Left) {
		h = moveRedRight(h)
	}
	var deleted Item
	h = copyNode(h)
	h.Right, deleted = deleteMax(h.Right)

	return fixUp(h), deleted
}

// Delete deletes an item from the tree whose key equals key.
// The deleted item is return, otherwise nil is returned.
func (t *LLRB) Delete(key Item) Item {
	var deleted Item
	t.root, deleted = t.delete(t.root, key)
	if t.root != nil {
		t.root.Black = true
	}
	if deleted != nil {
		t.count--
	}
	return deleted
}

func (t *LLRB) delete(h *Node, item Item) (*Node, Item) {
	var deleted Item
	if h == nil {
		return nil, nil
	}

	h = copyNode(h)
	if less(item, h.Item) {
		if h.Left == nil { // item not present. Nothing to delete
			return h, nil
		}
		if !isRed(h.Left) && !isRed(h.Left.Left) {
			h = moveRedLeft(h)
		}
		h.Left, deleted = t.delete(h.Left, item)
	} else {
		if isRed(h.Left) {
			h = rotateRight(h)
		}
		// If @item equals @h.Item and no right children at @h
		if !less(h.Item, item) && h.Right == nil {
			return nil, h.Item
		}
		// PETAR: Added 'h.Right != nil' below
		if h.Right != nil && !isRed(h.Right) && !isRed(h.Right.Left) {
			h = moveRedRight(h)
		}
		// If @item equals @h.Item, and (from above) 'h.Right != nil'
		if !less(h.Item, item) {
			var subDeleted Item
			h.Right, subDeleted = deleteMin(h.Right)
			if subDeleted == nil {
				panic("logic")
			}
			deleted, h.Item = h.Item, subDeleted
		} else { // Else, @item is bigger than @h.Item
			h.Right, deleted = t.delete(h.Right, item)
		}
	}

	return fixUp(h), deleted
}

// Internal node manipulation routines

func newNode(item Item) *Node { return &Node{Item: item} }

func copyNode(n *Node) *Node { return &Node{Item: n.Item, Left: n.Left, Right: n.Right, Black: n.Black} }

func isRed(h *Node) bool {
	if h == nil {
		return false
	}
	return !h.Black
}

func rotateLeft(h *Node) *Node {
	x := copyNode(h.Right)
	if x.Black {
		panic("rotating a black link")
	}
	h = copyNode(h)
	h.Right = x.Left
	x.Left = h
	x.Black = h.Black
	h.Black = false
	return x
}

func rotateRight(h *Node) *Node {
	x := copyNode(h.Left)
	if x.Black {
		panic("rotating a black link")
	}
	h = copyNode(h)
	h.Left = x.Right
	x.Right = h
	x.Black = h.Black
	h.Black = false
	return x
}

// REQUIRE: Left and Right children must be present
func flip(h *Node) *Node {
	h = copyNode(h)
	h.Black = !h.Black
	h.Left = copyNode(h)
	h.Left.Black = !h.Left.Black
	h.Right = copyNode(h.Right)
	h.Right.Black = !h.Right.Black
	return h
}

// REQUIRE: Left and Right children must be present
func moveRedLeft(h *Node) *Node {
	h = flip(h)
	if isRed(h.Right.Left) {
		h = copyNode(h)
		h.Right = rotateRight(h.Right)
		h = rotateLeft(h)
		h = flip(h)
	}
	return h
}

// REQUIRE: Left and Right children must be present
func moveRedRight(h *Node) *Node {
	h = flip(h)
	if isRed(h.Left.Left) {
		h = rotateRight(h)
		h = flip(h)
	}
	return h
}

func fixUp(h *Node) *Node {
	if isRed(h.Right) {
		h = rotateLeft(h)
	}

	if isRed(h.Left) && isRed(h.Left.Left) {
		h = rotateRight(h)
	}

	if isRed(h.Left) && isRed(h.Right) {
		h = flip(h)
	}

	return h
}

func (t *LLRB) Clone() MemDB {
	t2 := &LLRB{}
	t2.count = t.count
	t2.root = t.root
	return t2
}

func clone(h *Node) *Node {
	if h == nil {
		return nil
	}

	n := &Node{
		Black: h.Black,
		Item:  h.Item,
	}

	n.Left = clone(h.Left)
	n.Right = clone(h.Right)

	return n
}
