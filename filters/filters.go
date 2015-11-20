package filters

import "time"

//MessageFilter determines if a table or column should be removed from the output set.
type MessageFilter interface {
	FilterRelID(id uint32) bool
	FilterColumn(rel string, column string) bool
}

//FilterNone lets all tables and columns through
type FilterNone string

//FilterRelID always returns false
func (f FilterNone) FilterRelID(id uint32) bool {
	return false
}

//FilterColumn always returns false
func (f FilterNone) FilterColumn(rel string, col string) bool {
	return false
}

//RelationNameConverter turns table names into a map from int to name
type RelationNameConverter interface {
	ConvertRelNamesToIds(names []string) map[uint32]string
}

//ColumnMapFiltering uses a list of relations/columns to filter on. In the form of db.ns.table:{col1, col2}.  Also supports db.ns.table:{'*'}
type ColumnMapFiltering struct {
	sr             RelationNameConverter
	relations      map[string][]string
	idMap          map[uint32]string
	idUpdateTicker <-chan time.Time
	exclusive      bool
	relationNames  []string
}

//Exclusive filters exclude the provided relations from the output set.
func Exclusive(sr RelationNameConverter, relations map[string][]string) *ColumnMapFiltering {
	return newColumnMapFiltering(sr, relations, true, time.Second*1)
}

//Inclusive filters include the provided relations from the output set.
func Inclusive(sr RelationNameConverter, relations map[string][]string) *ColumnMapFiltering {
	return newColumnMapFiltering(sr, relations, false, time.Second*1)
}

func newColumnMapFiltering(sr RelationNameConverter, relations map[string][]string, exclusive bool, refresh time.Duration) *ColumnMapFiltering {
	var names []string

	for relname := range relations {
		names = append(names, relname)
	}

	relIds := sr.ConvertRelNamesToIds(names)

	return &ColumnMapFiltering{
		sr:             sr,
		relations:      relations,
		idMap:          relIds,
		idUpdateTicker: time.Tick(refresh),
		exclusive:      exclusive,
		relationNames:  names,
	}
}

func (f *ColumnMapFiltering) periodicallyUpdateIDMap() {
	select {
	case <-f.idUpdateTicker:
		f.idMap = f.sr.ConvertRelNamesToIds(f.relationNames)
	default:
	}
}

//FilterRelID first makes sure the name/id map is up to date.  Then checks the provided relations against that map to determine if they should be provided or not.
func (f *ColumnMapFiltering) FilterRelID(id uint32) bool {
	f.periodicallyUpdateIDMap()
	rel := f.idMap[id]
	columns, listed := f.relations[rel]
	if f.exclusive {
		return listed && len(columns) == 1 && columns[0] == "*"
	}

	return !listed
}

//FilterColumn reads the column list to determine whether a filter should be applied.
func (f *ColumnMapFiltering) FilterColumn(rel string, column string) bool {
	columns := f.relations[rel]

	for _, listed := range columns {
		if listed == column || listed == "*" {
			return f.exclusive
		}
	}

	return !f.exclusive
}
