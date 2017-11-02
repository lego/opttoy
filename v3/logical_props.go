package v3

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// queryState holds per-query state such as the tables referenced by the query
// and the mapping from table name to the column index for those tables columns
// within the query.
type queryState struct {
	catalog map[string]*table
	tables  map[string]bitmapIndex
	nextVar bitmapIndex
}

type columnProps struct {
	name   string
	tables []string
	index  bitmapIndex
	// TODO(peter): value constraints.
}

func (c columnProps) hasColumn(tableName, colName string) bool {
	if colName != c.name {
		return false
	}
	if tableName == "" {
		return true
	}
	return c.hasTable(tableName)
}

func (c columnProps) hasTable(tableName string) bool {
	for _, t := range c.tables {
		if t == tableName {
			return true
		}
	}
	return false
}

func (c columnProps) resolvedName(tableName string) *parser.ColumnItem {
	if tableName == "" {
		if len(c.tables) > 0 {
			tableName = c.tables[0]
		}
	}
	return &parser.ColumnItem{
		TableName: parser.TableName{
			TableName:               parser.Name(tableName),
			DBNameOriginallyOmitted: true,
		},
		ColumnName: parser.Name(c.name),
	}
}

func (c columnProps) newVariableExpr(tableName string, props *logicalProps) *expr {
	e := &expr{
		op:      variableOp,
		props:   props,
		private: c.resolvedName(tableName),
	}
	e.inputVars.set(c.index)
	e.updateProps()
	return e
}

type foreignKeyProps struct {
	src  bitmap
	dest bitmap
}

type logicalProps struct {
	columns []columnProps

	// Bitmap indicating which output columns cannot be NULL. The NULL-ability of
	// columns flows from the inputs and can also be derived from filters that
	// are NULL-intolerant.
	notNullCols bitmap

	// Required output vars is the set of output variables that parent expression
	// requires. This must be a subset of logicalProperties.outputVars.
	requiredOutputVars bitmap

	// A column set is a key if no two rows are equal after projection onto that
	// set. A requirement for a column set to be a key is for no columns in the
	// set to be NULL-able. This requirement stems from the property of NULL
	// where NULL != NULL. The simplest example of a key is the primary key for a
	// table (recall that all of the columns of the primary key are defined to be
	// NOT NULL).
	//
	// A weak key is a set of columns where no two rows containing non-NULL
	// values are equal after projection onto that set. A UNIQUE index on a table
	// is a weak key and possibly a key if all of the columns are NOT NULL. A
	// weak key is a key if "(weakKeys[i] & notNullColumns) == weakKeys[i]".
	weakKeys []bitmap

	// TODO(peter): When to initialize foreign keys? In order to know the
	// destination columns we have to have encountered all of the tables in the
	// query. Perhaps as a first prep pass. How to propagate keys and foreign
	// keys through groupBy, join, orderBy, project, rename and set operations?
	//
	// A foreign key is a set of columns in the source table that uniquely
	// identify a single row in the destination table. A foreign key thus refers
	// to a primary key or unique key in the destination table. If the source
	// columns are NOT NULL a foreign key can prove the existence of a row in the
	// destination table and can also be used to infer the cardinality of joins
	// when joining on the foreign key. Consider the schema:
	//
	//   CREATE TABLE departments (
	//     dept_id INT PRIMARY KEY,
	//     name STRING
	//   );
	//
	//   CREATE TABLE employees (
	//     emp_id INT PRIMARY KEY,
	//     dept_id INT REFERENCES d (dept_id),
	//     name STRING,
	//     salary INT
	//   );
	//
	// And the query:
	//
	//   SELECT e.name, e.salary
	//   FROM employees e, departments d
	//   WHERE e.dept_id = d.dept_id
	//
	// The foreign key constraint specifies that employees.dept_id must match a
	// value in departments.dept_id or be NULL. Because departments.dept_id is NOT
	// NULL (due to being part of the primary key), we know the only rows from
	// employees that will not be in the join are those with a NULL dept_id. So we
	// can transform the query into:
	//
	//   SELECT e.name, e.salary
	//   FROM employees e
	//   WHERE e.dept_id IS NOT NULL
	foreignKeys []foreignKeyProps

	// An equivilancy group indicates when columns have equivilant
	// values. equivilancyGroups itself is a list of the groups, where
	// each group is a columns bitmap. It is derived from foreign keys
	// and the equalities of joins and filters.
	equivilancyGroups []bitmap

	// The global query state.
	state *queryState
}

func (p *logicalProps) String() string {
	var buf bytes.Buffer
	p.format(&buf, 0)
	return buf.String()
}

func (p *logicalProps) format(buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%scolumns:", indent)
	for _, col := range p.columns {
		buf.WriteString(" ")
		if p.requiredOutputVars.get(col.index) {
			buf.WriteString("+")
		}
		if tables := col.tables; len(tables) > 1 {
			buf.WriteString("{")
			for j, table := range tables {
				if j > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(table)
			}
			buf.WriteString("}")
		} else if len(tables) == 1 {
			buf.WriteString(tables[0])
		}
		buf.WriteString(".")
		buf.WriteString(col.name)
		buf.WriteString(":")
		fmt.Fprintf(buf, "%d", col.index)
		if p.notNullCols.get(col.index) {
			buf.WriteString("*")
		}
	}
	buf.WriteString("\n")
	for _, key := range p.weakKeys {
		var prefix string
		if (key & p.notNullCols) != key {
			prefix = "weak "
		}
		fmt.Fprintf(buf, "%s%skey: %s\n", indent, prefix, key)
	}
	for _, fkey := range p.foreignKeys {
		fmt.Fprintf(buf, "%sforeign key: %s -> %s\n", indent, fkey.src, fkey.dest)
	}
	if p.equivilancyGroups != nil {
		buf.WriteString(indent)
		buf.WriteString("equivilancies:\n")
		for _, group := range p.equivilancyGroups {
			buf.WriteString(indent)
			buf.WriteString("  ")
			for _, col := range p.columns {
				if group.get(col.index) {
					buf.WriteString(" ")
					if p.requiredOutputVars.get(col.index) {
						buf.WriteString("+")
					}
					if tables := col.tables; len(tables) > 1 {
						buf.WriteString("{")
						for j, table := range tables {
							if j > 0 {
								buf.WriteString(",")
							}
							buf.WriteString(table)
						}
						buf.WriteString("}")
					} else if len(tables) == 1 {
						buf.WriteString(tables[0])
					}
					buf.WriteString(".")
					buf.WriteString(col.name)
					buf.WriteString(":")
					fmt.Fprintf(buf, "%d", col.index)
					if p.notNullCols.get(col.index) {
						buf.WriteString("*")
					}
				}
			}
			buf.WriteByte('\n')
		}
	}
}

func (p *logicalProps) newColumnExpr(name string) *expr {
	for _, col := range p.columns {
		if col.name == name {
			return col.newVariableExpr(col.tables[0], p)
		}
	}
	return nil
}

func (p *logicalProps) newColumnExprByIndex(index bitmapIndex) *expr {
	for _, col := range p.columns {
		if col.index == index {
			return col.newVariableExpr(col.tables[0], p)
		}
	}
	return nil
}

func (p *logicalProps) addEquivilancy(b bitmap) {
	if p.equivilancyGroups == nil {
		p.equivilancyGroups = append(p.equivilancyGroups, b)
	} else {
		// FIXME(joey): This is an allocation heavy strategy for running
		// a merge of all equivilancy groups. A possible alternative
		// would be to have a map of column to equivilancy group index.
		// We will still need to do an expensive removal operation.
		// Gotta think about the best approach more, but for now this
		// approach is functional.
		var newEquivilancyGroups []bitmap
		for _, group := range p.equivilancyGroups {
			if b|group != 0 {
				b |= group
			} else {
				newEquivilancyGroups = append(newEquivilancyGroups, group)
			}
		}
		newEquivilancyGroups = append(newEquivilancyGroups, b)
		p.equivilancyGroups = newEquivilancyGroups
	}
}

// Yuck.. oh the complexity.
func (p *logicalProps) mergeEquivilancyGroups(otherProps *logicalProps) {
	if otherProps.equivilancyGroups == nil {
		return
	} else if p.equivilancyGroups == nil {
		p.equivilancyGroups = otherProps.equivilancyGroups
		return
	}

	// Merge two sets of equivilancy groups.
	groupMapping := make(map[bitmapIndex]int)
	var newEquivilancyGroups []bitmap
	for _, group := range p.equivilancyGroups {
		newIdx := -1
		// TODO(joey): An alterantive might be to iterate through all of
		// the existing equivilancy groups and check if any group has
		// overlap. That is probably more efficient...
		for _, col := range p.columns {
			// Find every column in the equivilancy group and check if
			// it already belongs to a group (via groupMapping). If it
			// does, merge this group with the existing group.
			if group.get(col.index) {
				if idx, ok := groupMapping[col.index]; ok {
					newEquivilancyGroups[idx] |= group
					newIdx = idx
					break
				}
			}
		}
		// If none of the columns belonged to an existing group, append this new group.
		if newIdx == -1 {
			newEquivilancyGroups = append(newEquivilancyGroups, group)
			newIdx = len(newEquivilancyGroups) - 1
		}
		for _, col := range p.columns {
			groupMapping[col.index] = newIdx
		}
	}
	p.equivilancyGroups = newEquivilancyGroups
}

// Add additional not-NULL columns based on the filtering expressions.
// Also formulate any column equivilancies.
func (p *logicalProps) applyFilters(filters []*expr) {
	for _, filter := range filters {
		// TODO(peter): !isNullTolerant(filter)
		for v := filter.inputVars; v != 0; {
			i := bitmapIndex(bits.TrailingZeros64(uint64(v)))
			v.clear(i)
			p.notNullCols.set(i)
		}
		// FIXME(joey): This may not be able to handle nested ANDs with
		// equivilances.
		if filter.op == eqOp {
			left := filter.inputs()[0]
			right := filter.inputs()[1]
			if left.op == variableOp && right.op == variableOp {
				p.addEquivilancy(filter.inputVars)
			}
		}
	}
}

// A filter is compatible with the logical properties for an expression if all
// of the input variables used by the filter are provided by the columns.
func (p *logicalProps) isFilterCompatible(filter *expr) bool {
	v := filter.inputVars
	for i := 0; v != 0 && i < len(p.columns); i++ {
		v.clear(p.columns[i].index)
	}
	return v == 0
}

func (p *logicalProps) outputVars() bitmap {
	var b bitmap
	for _, col := range p.columns {
		b.set(col.index)
	}
	return b
}

func updateProps(e *expr) {
	for _, input := range e.inputs() {
		updateProps(input)
	}
	e.updateProps()
}
