package main

import "testing"

func TestObjectDurability(t *testing.T) {
	yes, no := true, false
	tasks := []taskSpec{
		{Name: "src", Outputs: []outputSpec{{Name: "mid"}, {Name: "keep", Durable: &yes}}},
		{Name: "a", Dependencies: []string{"src"}, Inputs: []inputSpec{{Producer: "src", Object: "mid"}}},
		{Name: "b", Dependencies: []string{"a"}},                     // reads a's default output
		{Name: "cached", CacheKey: "k", Dependencies: []string{"b"}}, // consumed? no, but cacheable
		{Name: "sink", Dependencies: []string{"a"}},
		{Name: "quiet", Durable: &no},
	}
	byName := map[string]taskSpec{}
	for _, x := range tasks {
		byName[x.Name] = x
	}
	check := func(policy, task, out string, wantD, wantOK bool) {
		t.Helper()
		d, ok := objectDurability(policy, byName[task], out, tasks)
		if d != wantD || ok != wantOK {
			t.Errorf("%s %s.%s = (%v,%v), want (%v,%v)", policy, task, out, d, ok, wantD, wantOK)
		}
	}
	check("auto", "src", "mid", false, true) // intermediate
	check("auto", "src", "keep", true, true) // explicit override
	check("auto", "a", "", false, true)      // read by b and sink
	check("auto", "sink", "", true, true)    // final output
	check("auto", "cached", "", true, true)  // cacheKey
	check("all", "src", "mid", true, true)
	check("none", "sink", "", false, true)
	check("none", "src", "keep", true, true)  // override beats policy
	check("node", "src", "mid", false, false) // agent default
	check("auto", "quiet", "", false, true)   // task override beats "final output"

	env := durabilityEnv("auto", byName["src"], tasks)
	got := map[string]string{}
	for _, e := range env {
		got[e.Name] = e.Value
	}
	if got["WL_OUT_MID_DURABLE"] != "0" || got["WL_OUT_KEEP_DURABLE"] != "1" {
		t.Errorf("durabilityEnv = %v", got)
	}
}
