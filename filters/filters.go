package filters

import "time"

type MessageFilter interface {
	FilterRelId(id uint32) bool
	FilterColumn(rel string, column string) bool
}

type FilterNone string

func (f FilterNone) FilterRelId(id uint32) bool {
	return false
}

func (f FilterNone) FilterColumn(rel string, col string) bool {
	return false
}

type RelationNameConverter interface {
	ConvertRelNamesToIds(names []string) map[uint32]string
}

type ColumnMapFiltering struct {
	sr             RelationNameConverter
	relations      map[string][]string
	idMap          map[uint32]string
	idUpdateTicker <-chan time.Time
	exclusive      bool
	relationNames  []string
}

func Exclusive(sr RelationNameConverter, relations map[string][]string) *ColumnMapFiltering {
	return newColumnMapFiltering(sr, relations, true, time.Second*1)
}

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

func (f *ColumnMapFiltering) periodicallyUpdateIdMap() {
	select {
	case <-f.idUpdateTicker:
		f.idMap = f.sr.ConvertRelNamesToIds(f.relationNames)
	default:
	}
}

func (f *ColumnMapFiltering) FilterRelId(id uint32) bool {
	f.periodicallyUpdateIdMap()
	rel := f.idMap[id]
	columns, listed := f.relations[rel]
	if f.exclusive {
		return listed && len(columns) == 1 && columns[0] == "*"
	} else {
		return !listed
	}
}

func (f *ColumnMapFiltering) FilterColumn(rel string, column string) bool {
	columns := f.relations[rel]

	for _, listed := range columns {
		if listed == column || listed == "*" {
			return f.exclusive
		}
	}

	return !f.exclusive
}
