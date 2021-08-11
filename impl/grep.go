package main

import (
	"regexp"
	"strings"

	"mapreduce.paper/mr"
)

//
// a grep(for filtering text) application "plugin" for MapReduce.
//
var expr, _ = regexp.Compile("(Title)|(Author)")

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}

	for _, line := range strings.Split(contents, "\n") {
		if expr.MatchString(line) {
			kva = append(kva, mr.KeyValue{line, ""})
		}
	}

	return kva
}

func Reduce(key string, values []string) string {
	return key
}
