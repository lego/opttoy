package v3

import (
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func aggregationSplitting(e *expr) {
	if e.op == innerJoinOp {
		inputs := e.inputs()
		left := inputs[0]
		right := inputs[1]
		// Try to eliminate the right side of the join. Because inner join is
		// symmetric, we can use the same code to try and eliminate the left side
		// of the join.
		if !maybeSplitAggregation(e, left, right) {
			maybeSplitAggregation(e, right, left)
		}
	}
	for _, input := range e.inputs() {
		aggregationSplitting(input)
	}
	e.updateProps()
}

// Check to see if the right side of the join is unnecessary.
func maybeSplitAggregation(e, left, right *expr) bool {
	// Check to see if the required output vars only depend on the left side of the join.
	leftOutputVars := left.props.outputVars()
	if (e.props.requiredOutputVars & leftOutputVars) != e.props.requiredOutputVars {
		return false
	}

	// Look for a foreign key in the left side of the join which maps to a unique
	// index on the right side of the join.
	rightOutputVars := right.props.outputVars()
	var fkey *foreignKeyProps
	for i := range left.props.foreignKeys {
		fkey = &left.props.foreignKeys[i]
		if (fkey.dest & rightOutputVars) == fkey.dest {
			// The target of the foreign key is the right side of the join.
			break
		}
		fkey = nil
	}
	if fkey == nil {
		return false
	}

	// Make sure any filters present other than the join condition only apply to
	// the left hand side of the join.
	filters := e.filters()
	for _, filter := range filters {
		// TODO(peter): pushDownFilters() should ensure we only have join
		// conditions here making this test and the one for the left output vars
		// unnecessary.
		if (filter.inputVars & rightOutputVars) == filter.inputVars {
			// The filter only utilizes variables from the right hand side of the
			// join.
			return false
		}
		if (filter.inputVars & leftOutputVars) == filter.inputVars {
			// The filter only utilizes variables from the left hand side of the
			// join.
			continue
		}
		// TODO(peter): how to check for the join conditions? We need the join
		// condition to match the foreign key. This is easy for simple "a.x = b.x"
		// style join conditions. But what about "a.x = b.x AND a.y = b.y"? And
		// "(a.x, a.y) = (b.x, b.y)".
	}

	// Move any filters down to the left hand side of the join.
	for _, filter := range filters {
		if (filter.inputVars & leftOutputVars) == filter.inputVars {
			left.addFilter(filter)
		}
	}
	left.props.applyFilters(left.filters())

	// The source columns for the foreign key might be NULL-able. Construct a
	// filter to ensure rows containing NULLs are removed.
	//
	// TODO(peter): Rather than generating the filter here, it would be better to
	// have pushDownFilters generate IS NOT NULL filters on the join conditions.
	var notNull []*expr
	for v := fkey.src & ^left.props.notNullCols; v != 0; {
		i := bitmapIndex(bits.TrailingZeros64(uint64(v)))
		v.clear(i)
		t := &expr{
			op: isNotOp,
			children: []*expr{
				left.props.newColumnExprByIndex(i),
				&expr{
					op:      constOp,
					props:   left.props,
					private: parser.DNull,
				},
			},
		}
		t.updateProps()
		notNull = append(notNull, t)
	}
	if len(notNull) > 1 {
		t := &expr{
			op:       orOp,
			children: notNull,
			props:    left.props,
		}
		t.updateProps()
		left.addFilter(t)
	} else if len(notNull) == 1 {
		left.addFilter(notNull[0])
	}

	*e = *left
	return true
}
