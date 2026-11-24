package config

import "testing"

func TestRecreateLostSlots(t *testing.T) {
	cases := map[string]struct {
		value   string
		set     bool
		want    bool
		wantErr bool
	}{
		"unset": {},
		"true":  {value: "true", set: true, want: true},
		"false": {value: "false", set: true},
		"empty": {value: "", set: true, wantErr: true},
		"1":     {value: "1", set: true, wantErr: true},
		"on":    {value: "on", set: true, wantErr: true},
	}
	for name, c := range cases {
		got, err := RecreateLostSlots(func(key string) (string, bool) {
			if key != RecreateLostSlotsVar {
				t.Fatalf("%s: looked up %s", name, key)
			}
			return c.value, c.set
		})
		if got != c.want || (err != nil) != c.wantErr {
			t.Errorf("%s: %v %v, want %v and an error %v", name, got, err, c.want, c.wantErr)
		}
	}
}
