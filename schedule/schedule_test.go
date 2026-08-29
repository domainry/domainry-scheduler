package schedule

import (
	"testing"
	"time"
)

func TestScheduleMatrix(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		data map[string]any
		want time.Time
	}{
		{map[string]any{"schedule_type": "interval", "interval_seconds": 30}, now.Add(30 * time.Second)},
		{map[string]any{"schedule_type": "daily_at", "time_of_day": "13:00"}, time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)},
		{map[string]any{"schedule_type": "weekly_at", "weekday": "monday", "weekly_at": "09:00"}, time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)},
		{map[string]any{"schedule_type": "monthly_at", "day_of_month": 20, "monthly_at": "09:00"}, time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)},
		{map[string]any{"schedule_type": "cron", "schedule_expression": "0 13 * * *"}, time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		if got := Next(test.data, now); !got.Equal(test.want) {
			t.Fatalf("next %#v = %s, want %s", test.data, got, test.want)
		}
	}
}

func TestParsingAndTimezone(t *testing.T) {
	if Type(map[string]any{"schedule_expression": "5m"}) != "interval" {
		t.Fatal("duration was not classified")
	}
	if IntervalSeconds(map[string]any{"interval_minutes": 15}) != 900 {
		t.Fatal("interval minutes mismatch")
	}
	if _, ok := Cron(map[string]any{"schedule_expression": "invalid"}); ok {
		t.Fatal("invalid cron accepted")
	}
	if weekday, ok := ParseWeekday("Friday"); !ok || weekday != time.Friday {
		t.Fatal("weekday mismatch")
	}
	now := time.Date(2026, 7, 19, 12, 30, 0, 0, time.UTC)
	got := Next(map[string]any{"schedule_type": "daily_at", "time_of_day": "21:00", "timezone": "Asia/Shanghai"}, now)
	if want := time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("timezone next=%s want=%s", got, want)
	}
}
