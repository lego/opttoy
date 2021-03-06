package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
)

type node struct {
	op          operator
	class       string // equivalence class
	classIdx    int
	left, right *node
}

func parse(s string) *node {
	var n *node
	for _, p := range strings.Split(s, ",") {
		t := &node{op: scanOp{}, class: p}
		if n == nil {
			n = t
		} else {
			n = &node{
				op:    joinOp{},
				left:  n,
				right: t,
			}
		}
	}
	return n
}

func (n *node) Debug() string {
	switch n.op.(type) {
	case joinOp:
		return fmt.Sprintf("(%s ⋈ %s):%d", n.left.Debug(), n.right.Debug(), n.classIdx)
	case scanOp:
		return fmt.Sprintf("%s:%d", n.class, n.classIdx)
	default:
		return "not reached"
	}
}

func (n *node) String() string {
	switch n.op.(type) {
	case joinOp:
		return fmt.Sprintf("(%s ⋈ %s)", n.left, n.right)
	case scanOp:
		return fmt.Sprintf("%s", n.class)
	default:
		return "not reached"
	}
}

func (n *node) EquivClass() string {
	// For now, we use the list of the tables in alphabetical order to check
	// if two nodes belong in the same memo class.  Needless to say, this is
	// only going to take us so far...
	tables := n.JoinSig()
	sort.Strings(tables)
	return strings.Join(tables, ",")
}

func (n *node) JoinSig() []string {
	var res []string
	switch n.op.(type) {
	case joinOp:
		children := [2]*node{n.left, n.right}
		for _, child := range children {
			res = append(res, child.JoinSig()...)
		}
	case scanOp:
		res = append(res, n.class)
	}
	return res
}

type expr struct {
	op          operator
	class       string // equivalence class
	left, right int    // class id's
}

func (e *expr) String() string {
	switch e.op.(type) {
	case joinOp:
		return fmt.Sprintf("(%d ⋈ %d)", e.left, e.right)
	case scanOp:
		return fmt.Sprintf("%s", e.class)
	default:
		return "not reached"
	}
}

type class struct {
	id    int
	m     map[string]int
	exprs []*expr
}

func newClass(id int) *class {
	return &class{
		id: id,
		m:  make(map[string]int),
	}
}

func (c *class) add(e *expr) bool {
	id := e.String()
	i, ok := c.m[id]
	if ok {
		return false
	}
	i = len(c.exprs)
	c.exprs = append(c.exprs, e)
	c.m[id] = i
	return true
}

type memo struct {
	exprMap  map[string]int
	classMap map[string]int
	classes  []*class
}

func newMemo() *memo {
	m := &memo{
		exprMap:  make(map[string]int),
		classMap: make(map[string]int),
	}
	return m
}

func (m *memo) build(n *node) {
	switch n.op.(type) {
	case joinOp:
		m.build(n.left)
		m.build(n.right)
		m.add(n)

	case scanOp:
		m.add(n)
	}
}

type xform interface {
	apply(n *node) []*node
}

// A ⋈ B => B ⋈ A
type joinCommuteXform struct{}

func (joinCommuteXform) check(n *node) bool {
	_, ok := n.op.(joinOp)
	return ok
}

func (jc joinCommuteXform) apply(n *node) []*node {
	if !jc.check(n) {
		return nil
	}
	return []*node{{
		op:    joinOp{},
		class: n.class,
		left:  n.right,
		right: n.left,
	}}
}

// (A ⋈ B) ⋈ C  => A ⋈ (B ⋈ C)
type joinAssocXform struct{}

func (joinAssocXform) check(n *node) bool {
	if _, ok := n.op.(joinOp); ok {
		_, ok := n.left.op.(joinOp)
		return ok
	}
	return false
}

func (ja joinAssocXform) apply(n *node) []*node {
	if !ja.check(n) {
		return nil
	}
	r := &node{
		op:    joinOp{},
		left:  n.left.right,
		right: n.right,
	}
	return []*node{
		r,
		{
			op:    joinOp{},
			class: n.class,
			left:  n.left.left,
			right: r,
		},
	}
}

type operator interface {
	compatXform() []xform
}

var joinXforms = []xform{
	joinAssocXform{},
	joinCommuteXform{},
}

type joinOp struct{}

func (joinOp) compatXform() []xform {
	return joinXforms
}

type scanOp struct{}

func (scanOp) compatXform() []xform {
	return nil
}

func (m *memo) genTrees(e *expr) []*node {
	res := make([]*node, 0)
	var left, right []*node
	if e.left != -1 {
		for _, e := range m.classes[e.left].exprs {
			left = append(left, m.genTrees(e)...)
		}
	} else {
		left = []*node{{
			op:    e.op,
			class: e.class,
			left:  nil,
			right: nil,
		}}
	}
	if e.right != -1 {
		for _, e := range m.classes[e.right].exprs {
			right = append(right, m.genTrees(e)...)
		}
	} else {
		right = []*node{{
			op:    e.op,
			class: e.class,
			left:  nil,
			right: nil,
		}}
	}
	for _, lnode := range left {
		for _, rnode := range right {
			n := &node{
				op:    e.op,
				class: e.class,
				left:  lnode,
				right: rnode,
			}
			res = append(res, n)
		}
	}
	return res
}

// Exhaustive search:
// for each memo class
//     for each forest starting at a node in that class
//         for each expression in that forest
//             for each transformation that is compatible with that expression
//                 apply that transformation
func (m *memo) expand() int {
	var count int
	for _, c := range m.classes {
		for _, e := range c.exprs {
			for _, n := range m.genTrees(e) {
				for _, x := range n.op.compatXform() {
					for _, t := range x.apply(n) {
						if m.add(t) {
							count++
						}
					}
				}
			}
		}
	}
	return count
}

func (m *memo) expandAll() {
	fmt.Println(m)
	for {
		n := m.expand()
		if n == 0 {
			break
		}
		fmt.Printf("%d expansions\n%s\n", n, m)
	}
}

func (m *memo) add(n *node) bool {
	id := n.String()
	if n.class == "" {
		n.class = id
	}
	if _, ok := m.exprMap[id]; ok {
		return false
	}
	ec := n.EquivClass()
	i, ok := m.classMap[ec]
	if !ok {
		i = len(m.classes)
		c := newClass(i)
		m.classes = append(m.classes, c)
		m.classMap[ec] = i
	}
	lexpr := -1
	if n.left != nil {
		lexpr = m.exprMap[n.left.class]
	}
	rexpr := -1
	if n.right != nil {
		rexpr = m.exprMap[n.right.class]
	}
	e := &expr{n.op, n.class, lexpr, rexpr}
	m.exprMap[id] = i
	return m.classes[i].add(e)
}

func (m *memo) list(n *node) {
	i := m.exprMap[n.String()]
	for _, e := range m.classes[i].exprs {
		fmt.Println(e)
	}
}

type dfsStatus int

const (
	white dfsStatus = iota
	gray
	black
)

type dfsInfo struct {
	Me     *class    // this class
	Parent *class    // parent
	D      int       // discovery time
	F      int       // finished visiting time
	Color  dfsStatus // WHITE (not discovered), GRAY (not visited), BLACK (done)
}

type dfsInfoList []*dfsInfo

func (m *memo) DFS() dfsInfoList {
	state := make(map[*class]*dfsInfo, len(m.classes))
	for _, c := range m.classes {
		state[c] = &dfsInfo{c, nil, -1, -1, white}
	}
	t := 0

	res := make([]*dfsInfo, 0, len(m.classes))
	for _, c := range m.classes {
		if state[c].Color == white {
			m.dfsVisit(c, &t, state)
		}
		res = append(res, state[c])
	}
	return res
}

func (m *memo) dfsVisit(c *class, t *int, state map[*class]*dfsInfo) {
	*t++
	state[c].D = *t
	state[c].Color = gray

	for _, e := range c.exprs {
		for i := 0; i < 2; i++ {
			var v int
			if i == 0 {
				v = e.left
			} else {
				v = e.right
			}
			if v == -1 {
				continue
			}

			vc := m.classes[v]
			if state[vc].Color == white {
				state[vc].Parent = c
				m.dfsVisit(vc, t, state)
			}
		}
	}

	state[c].Color = black
	*t++
	state[c].F = *t
}

func (m *memo) topoSort() []*class {
	dfs := m.DFS()
	sort.Slice(dfs, func(i, j int) bool { return dfs[i].F >= dfs[j].F })

	res := make([]*class, 0, len(dfs))
	for i := range dfs {
		res = append(res, dfs[i].Me)
	}

	return res
}

func (m *memo) String() string {
	var buf bytes.Buffer

	sorted := m.topoSort()
	for _, c := range sorted {
		fmt.Fprintf(&buf, "%d:", c.id)
		for _, e := range c.exprs {
			fmt.Fprintf(&buf, " [%s]", e.String())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: opttoy <query>\n")
		os.Exit(1)
	}
	n := parse(os.Args[1])
	m := newMemo()
	m.build(n)
	m.expandAll()
	m.list(n)
}
