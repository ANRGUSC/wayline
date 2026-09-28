package main

import "testing"

func TestTemplateOfRun(t *testing.T) {
	cases := map[string]string{
		"e11-heft-warm-run-twmlr": "e11-heft-warm",
		"demo-iot-run-h4r98":      "demo-iot",
		"a-run-b-run-c":           "a-run-b",
		"no-template-here":        "",
	}
	for in, want := range cases {
		if got := templateOfRun(in); got != want {
			t.Errorf("templateOfRun(%q) = %q, want %q", in, got, want)
		}
	}
}
