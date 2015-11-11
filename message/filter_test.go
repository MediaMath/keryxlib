package message

import (
	"log"
	"testing"
	"time"
)

func TestFilterOnlyRelations(t *testing.T) {
	input := make(chan *Message)
	output, err := FilterOnlyRelations(map[string][]string{"a.b.c": {"id"}}, input)
	if err != nil {
		t.Fatal(err)
	}

	expectations := []struct {
		msg               *Message
		shouldPassthrough bool
	}{
		{makeMessage("a", "b", "c"), true},
		{makeMessage("a", "b", "z"), false},
		{makeMessage("a", "b", "c"), true},
		{makeMessage("a", "b", "z"), false},
	}

	waitBriefly := func() <-chan time.Time {
		return time.After(10 * time.Millisecond)
	}

	discardOutput := func() {
		select {
		case <-output:
		case <-waitBriefly():
			t.Fatal("timed out waiting for message")
		}
	}

	publishMessage := func(msg *Message, shouldPassthrough bool) {
		select {
		case input <- msg:
			if shouldPassthrough {
				discardOutput()
			}
		case <-waitBriefly():
			t.Fatal("timed out publishing message")
		}
	}

	for _, e := range expectations {
		publishMessage(e.msg, e.shouldPassthrough)
	}
}

func TestFilterOnlyRelationsPropagatesError(t *testing.T) {
	input := make(chan *Message)
	_, err := FilterOnlyRelations(map[string][]string{"": {""}}, input)
	if err == nil {
		t.Error("Should have failed")
	}
}

func TestMakeMatchersAllGood(t *testing.T) {
	goodPatterns := map[string][]string{
		"a.b.c": {"a", "b"},
		"a.d.c": {"a", "b"},
		"D.R.Y": {"a", "b"},
	}

	ms, err := makeMatchers(goodPatterns)

	if err != nil || len(ms) != len(goodPatterns) {
		t.Fatalf("should have gotten more matchers")
	}
}

func TestMakeMatchersSomeBad(t *testing.T) {
	goodPatterns := map[string][]string{
		"a.b.c": {"a", "b"},
		"a..c":  {"a", "b"},
		"F.R.Y": {"a", "b"},
	}

	_, err := makeMatchers(goodPatterns)

	if err == nil {
		t.Fatal("Should have failed")
	}
}

func TestMakeMatcherFailsBadPattern(t *testing.T) {
	badPatterns := map[string][]string{
		"a":       {"a"},
		"a.b":     {"a"},
		"a.b.c.d": {"a"},
		"..":      {"a"},
		"..c":     {"a"},
		".b.":     {"a"},
		".b.c":    {"a"},
		"a..":     {"a"},
		"a..c":    {"a"},
		"a.b.":    {"a"},
		"":        {"a"},
	}

	check := func(pat string) {
		_, err := makeMatcher(pat)
		if err == nil {
			t.Fatal("Should have failed")
		}
	}

	for bp, _ := range badPatterns {
		check(bp)
	}
}

func TestRelationString(t *testing.T) {
	expected := [][4]string{
		[4]string{"a", "b", "c", "a.b.c"},
		[4]string{"a", "b", "", "a.b."},
		[4]string{"a", "", "c", "a..c"},
		[4]string{"a", "", "", "a.."},
		[4]string{"", "b", "c", ".b.c"},
		[4]string{"", "b", "", ".b."},
		[4]string{"", "", "c", "..c"},
		[4]string{"", "", "", ".."},
	}

	check := func(a, b, c, expected string) {
		msg := makeMessage(a, b, c)
		relStr := relationString(msg)
		if expected != relStr {
			log.Fatalf("strings %q, %q and %q yielded %q when %q was expected", a, b, c, relStr, expected)
		}
	}

	for _, e := range expected {
		check(e[0], e[1], e[2], e[3])
	}
}

func TestMakeMatcherReturnsWorkingMatcher(t *testing.T) {
	messages := []*Message{
		makeMessage("a", "b", "c"),
	}

	expectations := map[string][]bool{
		"a.b.c": []bool{true, false, false, false, false, false, false, false},
	}

	check := func(pattern string, expectedResult []bool) {
		m, err := makeMatcher(pattern)
		if err != nil {
			t.Fatalf("failed to make matcher for %q", pattern)
		}

		for i, msg := range messages {
			result := m(msg)
			if result != expectedResult[i] {
				t.Fatalf("unexpected result (%v) from matcher %q with msg %v", result, pattern, msg)
			}
		}
	}

	for pat, results := range expectations {
		check(pat, results)
	}
}

func makeMessage(a, b, c string) *Message {
	return &Message{DatabaseName: a, Namespace: b, Relation: c}
}

func TestVerifyGoodColumnPattern(t *testing.T) {
	colPattern := []string{"race", "id", "name"}
	err := verifyColumnPattern(colPattern)
	if err != nil {
		t.Fatal(err)
	}
}

func TestVerifyNoColumnPattern(t *testing.T) {
	colPattern := []string{}
	err := verifyColumnPattern(colPattern)
	if err == nil {
		t.Fatal("Should have failed")
	}
}

func TestVerifyEmptyColumnPattern(t *testing.T) {
	colPattern := []string{""}
	err := verifyColumnPattern(colPattern)
	if err == nil {
		t.Fatal("Should have failed")
	}
}

func BenchmarkGetNFields(b *testing.B) {
	msg := makeMessage("a", "b", "c")
	testFields := []Field{Field{Name: "id", Kind: "test", Value: "1"}}
	msg.Fields = testFields
	testFilter := map[string]bool{"a.b.c.id": true}

	for i := 0; i < b.N; i++ {
		getFilteredFields(testFilter, msg)
	}
}
