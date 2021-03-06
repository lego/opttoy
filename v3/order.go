package v3

import (
	"bytes"
)

func init() {
	registerOperator(orderByOp, "orderBy", orderBy{})
}

type orderBy struct{}

func (orderBy) kind() operatorKind {
	return relationalKind
}

func (orderBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	// formatExprs(buf, "sorting", e.sortings(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (orderBy) updateProps(e *expr) {
	unimplemented("%s.updateProperties", e.op)
}
