package message

import (
	"fmt"
	"strings"
)

type matcher func(*Message) bool

func FilterOnlyRelations(relationPatterns map[string][]string, input <-chan *Message) (chan *Message, error) {

	matchers, err := makeMatchers(relationPatterns)
	if err != nil {
		return nil, err
	}

	output := make(chan *Message)

	relMap := createRelMap(relationPatterns)

	go filter(input, output, matchers, relMap)

	return output, nil
}

func filter(input <-chan *Message, output chan *Message, matchers []matcher, filter map[string]bool) {

	for msg := range input {
		if msg.Type == CommitMessage {
			output <- msg
		} else {
		MatcherLoop:
			for _, matches := range matchers {
				//if relation matches then filter the columns based on config
				if matches(msg) {
					msg.Fields = getFilteredFields(filter, msg)
					output <- msg
					break MatcherLoop
				}
			}
		}
	}
}

func makeMatchers(relationPatterns map[string][]string) ([]matcher, error) {
	var ms []matcher
	for relPat := range relationPatterns {
		m, err := makeMatcher(relPat)
		if err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	return ms, nil
}

func makeMatcher(relationPattern string) (matcher, error) {
	patternParts := strings.Split(relationPattern, ".")
	err := verifyRelationPattern(patternParts)
	if err != nil {
		return nil, err
	}

	return func(msg *Message) bool {
		relStr := relationString(msg)
		relationParts := strings.Split(relStr, ".")
		return len(relationParts) == 3 &&
			compareNth(0, patternParts, relationParts) &&
			compareNth(1, patternParts, relationParts) &&
			compareNth(2, patternParts, relationParts)
	}, nil
}

func compareNth(n int, patternParts, relationParts []string) bool {
	return patternParts[n] == relationParts[n]
}

func relationString(msg *Message) string {
	return fmt.Sprintf("%v.%v.%v", msg.DatabaseName, msg.Namespace, msg.Relation)
}

func getFilteredFields(filterMap map[string]bool, msg *Message) (fs []Field) {
	for _, f := range msg.Fields {
		if filterMap[relationStringWithCol(msg, f.Name)] {
			fs = append(fs, f)
		}
	}
	return
}

func createRelMap(relationPattern map[string][]string) map[string]bool {
	relMap := map[string]bool{}

	for k, v := range relationPattern {
		for _, sv := range v {
			relMap[fmt.Sprintf("%v.%v", k, sv)] = true
		}
	}
	return relMap
}

func relationStringWithCol(msg *Message, fieldName string) string {
	return fmt.Sprintf("%v.%v.%v.%v", msg.DatabaseName, msg.Namespace, msg.Relation, fieldName)
}

func verifyRelationPattern(patternParts []string) error {
	var err error

	if len(patternParts) != 3 {
		err = fmt.Errorf("invalid relation patterns, verify relation_to_include in the config: %v", patternParts)
	} else {
		for n, p := range patternParts {
			if len(p) == 0 {
				err = fmt.Errorf("relation part[%v] is of zero length, Please check the relation_to_include in config", n)
			}
		}
	}
	return err
}

func verifyColumnPattern(fieldPattern []string) error {
	var err error
	if len(fieldPattern) <= 0 {
		err = fmt.Errorf("invalid columns patterns, verify relation_to_include in the config: %v", fieldPattern)
	}
	for idx, field := range fieldPattern {
		if len(field) == 0 {
			err = fmt.Errorf("columns part[%v] is of zero length, provide a column from the table in realtion_to_include", idx)
		}
	}
	return err
}
