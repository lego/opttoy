package v3

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	registerOperator(innerJoinOp, "inner join", innerJoin{})
	registerOperator(leftJoinOp, "left join", nil)
	registerOperator(rightJoinOp, "right join", nil)
	registerOperator(fullJoinOp, "full join", nil)
	registerOperator(semiJoinOp, "semi-join", innerJoin{})
	registerOperator(antiJoinOp, "anti-join", innerJoin{})
}

type innerJoin struct{}

func (innerJoin) kind() operatorKind {
	return relationalKind
}

func (innerJoin) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (innerJoin) updateProps(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}

	e.props.notNullCols = 0
	var providedInputVars bitmap
	for _, input := range e.inputs() {
		e.props.notNullCols |= input.props.notNullCols
		outputVars := input.props.outputVars()
		input.props.requiredOutputVars = outputVars
		providedInputVars |= outputVars
	}

	e.inputVars &^= (e.props.outputVars() | providedInputVars)
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	e.props.applyFilters(e.filters())

	for _, input := range e.inputs() {
		log.Errorf(context.TODO(), "Inputs here:\n=====%s\n======\n", e.String())
		e.props.mergeEquivilancyGroups(input.props)
	}
}
