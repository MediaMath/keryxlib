package filters

import "testing"

type i struct {
	id      uint32
	columns []string
}

var relations = map[string][]string{
	"foo": []string{"bar", "baz", "boff"},
	"moo": []string{"*"},
}

var relationIds = map[string]uint32{
	"foo": 98,
	"moo": 101,
	"noo": 9990,
}

type fakeMapping string

func (f fakeMapping) ConvertRelNamesToIds(names []string) map[uint32]string {
	mapping := make(map[uint32]string)
	for name, id := range relationIds {
		mapping[id] = name
	}
	return mapping
}

var relationTest = []struct {
	exclusive bool
	relation  string
	filter    bool
}{
	{true, "???", false},
	{true, "noo", false},
	{true, "foo", false},
	{true, "moo", true},
	{false, "???", true},
	{false, "noo", true},
	{false, "foo", false},
	{false, "moo", false},
}

func TestRelationFilters(t *testing.T) {
	for _, test := range relationTest {
		if relationFilter(test.exclusive, test.relation) != test.filter {
			t.Errorf("relation: %v", test)
		}
	}
}

func relationFilter(exclusive bool, relation string) bool {
	f := Inclusive(fakeMapping("test"), relations)
	if exclusive {
		f = Exclusive(fakeMapping("test"), relations)
	}

	return f.FilterRelID(relationIds[relation])
}

var columnTest = []struct {
	exclusive bool
	relation  string
	column    string
	filter    bool
}{
	{true, "noo", "boo", false},
	{true, "foo", "bog", false},
	{true, "foo", "baz", true},
	{true, "moo", "bar", true},
	{false, "noo", "boo", true},
	{false, "foo", "bog", true},
	{false, "foo", "baz", false},
	{false, "moo", "bar", false},
}

func TestColumnFilters(t *testing.T) {
	for _, test := range columnTest {
		if columnFilter(test.exclusive, test.relation, test.column) != test.filter {
			t.Errorf("column: %v", test)
		}
	}
}

func columnFilter(exclusive bool, relation string, column string) bool {
	f := Inclusive(fakeMapping("test"), relations)
	if exclusive {
		f = Exclusive(fakeMapping("test"), relations)
	}

	return f.FilterColumn(relation, column)
}
