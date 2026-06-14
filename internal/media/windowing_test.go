package media

import "testing"

func withWindowVars(t *testing.T, engage, ahead, back, header, punchMin int64) {
	t.Helper()
	oe, oa, ob, oh, op := windowEngageBytes, windowAheadBytes, windowBackBytes, windowHeaderKeep, windowPunchMin
	windowEngageBytes, windowAheadBytes, windowBackBytes, windowHeaderKeep, windowPunchMin = engage, ahead, back, header, punchMin
	t.Cleanup(func() {
		windowEngageBytes, windowAheadBytes, windowBackBytes, windowHeaderKeep, windowPunchMin = oe, oa, ob, oh, op
	})
}

func TestNextPunch(t *testing.T) {
	withWindowVars(t, 0, 1000, 1000, 100, 200)

	// observed too low: nothing past the header window is safe to free yet.
	if _, _, ok := nextPunch(900, 0); ok {
		t.Fatal("punch with observed below back+header should be skipped")
	}

	// observed 2000 -> punchTo = 1000; from = header 100; length 900 >= punchMin.
	off, length, ok := nextPunch(2000, 0)
	if !ok || off != 100 || length != 900 {
		t.Fatalf("nextPunch(2000,0) = (%d,%d,%v), want (100,900,true)", off, length, ok)
	}

	// already punched up to 1000, observed unchanged -> below punchMin, skip.
	if _, _, ok := nextPunch(2000, 1000); ok {
		t.Fatal("punch below punchMin increment should be skipped")
	}

	// observed advances to 3000 -> punchTo 2000, from 1000, length 1000.
	off, length, ok = nextPunch(3000, 1000)
	if !ok || off != 1000 || length != 1000 {
		t.Fatalf("nextPunch(3000,1000) = (%d,%d,%v), want (1000,1000,true)", off, length, ok)
	}
}
