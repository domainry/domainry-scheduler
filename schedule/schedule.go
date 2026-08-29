// Package schedule owns Scheduler recurrence parsing and wall-clock planning.
package schedule

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	schedulersdk "github.com/domainry/domainry-scheduler-sdk"
	"github.com/robfig/cron/v3"
)

func Data(value schedulersdk.Schedule) map[string]any {
	data := map[string]any{"schedule_type": value.Type, "schedule_expression": value.Expression, "timezone": value.Timezone}
	if value.IntervalSeconds > 0 {
		data["interval_seconds"] = value.IntervalSeconds
	}
	if value.TimeOfDay != "" {
		data["time_of_day"] = value.TimeOfDay
	}
	if value.DayOfWeek != "" {
		data["day_of_week"] = value.DayOfWeek
	}
	if value.DayOfMonth > 0 {
		data["day_of_month"] = value.DayOfMonth
	}
	return data
}

func NextSchedule(value schedulersdk.Schedule, now time.Time) time.Time {
	return Next(Data(value), now)
}

func Validate(value schedulersdk.Schedule) error {
	data := Data(value)
	switch Type(data) {
	case "interval":
		if IntervalSeconds(data) <= 0 {
			return fmt.Errorf("scheduler interval must be positive")
		}
	case "cron":
		if _, ok := Cron(data); !ok {
			return fmt.Errorf("scheduler cron expression is invalid")
		}
	case "daily_at", "weekly_at", "monthly_at":
		if _, _, _, ok := ParseClock(FirstNonEmpty(value.TimeOfDay, value.Expression)); !ok {
			return fmt.Errorf("scheduler wall clock is invalid")
		}
		if Type(data) == "weekly_at" {
			if _, ok := ParseWeekday(value.DayOfWeek); !ok {
				return fmt.Errorf("scheduler weekday is invalid")
			}
		}
		if Type(data) == "monthly_at" && (value.DayOfMonth < 1 || value.DayOfMonth > 31) {
			return fmt.Errorf("scheduler month day is invalid")
		}
	default:
		return fmt.Errorf("unsupported scheduler type %q", value.Type)
	}
	if value.Timezone != "" {
		if _, err := time.LoadLocation(value.Timezone); err != nil {
			return fmt.Errorf("scheduler timezone is invalid: %w", err)
		}
	}
	return nil
}

func WindowSuffix(data map[string]any, now time.Time) string {
	localNow := now.In(location(data))
	switch Type(data) {
	case "interval":
		seconds := IntervalSeconds(data)
		if seconds <= 0 {
			seconds = 86400
		}
		return fmt.Sprintf("interval%d_%d", seconds, now.UTC().Unix()/int64(seconds))
	case "weekly_at", "legacy_weekly":
		year, week := localNow.ISOWeek()
		return fmt.Sprintf("%04dW%02d", year, week)
	case "monthly_at", "legacy_monthly":
		return localNow.Format("200601")
	case "cron":
		return localNow.Format("200601021504")
	case "legacy_hourly":
		return localNow.Format("2006010215")
	default:
		return localNow.Format("20060102")
	}
}

func Next(data map[string]any, now time.Time) time.Time {
	loc := location(data)
	localNow := now.In(loc)
	switch Type(data) {
	case "interval":
		seconds := IntervalSeconds(data)
		if seconds <= 0 {
			seconds = 86400
		}
		return now.Add(time.Duration(seconds) * time.Second).UTC()
	case "daily_at":
		hour, minute, second := clock(data)
		return nextDaily(localNow, loc, hour, minute, second).UTC()
	case "weekly_at":
		hour, minute, second := clock(data)
		return nextWeekly(localNow, loc, weekday(data), hour, minute, second).UTC()
	case "monthly_at":
		hour, minute, second := clock(data)
		return nextMonthly(localNow, loc, monthDay(data), hour, minute, second).UTC()
	case "cron":
		if parsed, ok := Cron(data); ok {
			return parsed.Next(localNow).UTC()
		}
		return now.AddDate(0, 0, 1).UTC()
	case "legacy_hourly":
		return now.Add(time.Hour).UTC()
	case "legacy_weekly":
		return now.AddDate(0, 0, 7).UTC()
	case "legacy_monthly":
		return now.AddDate(0, 1, 0).UTC()
	default:
		return now.AddDate(0, 0, 1).UTC()
	}
}

func Type(data map[string]any) string {
	raw := strings.ToLower(strings.TrimSpace(fmt.Sprint(data["schedule_type"])))
	switch raw {
	case "interval", "daily_at", "weekly_at", "monthly_at", "cron":
		return raw
	}
	expression := strings.ToLower(strings.TrimSpace(fmt.Sprint(data["schedule_expression"])))
	switch expression {
	case "hourly", "@hourly":
		return "legacy_hourly"
	case "weekly", "@weekly":
		return "legacy_weekly"
	case "monthly", "@monthly":
		return "legacy_monthly"
	case "daily", "@daily", "@midnight", "":
		return "legacy_daily"
	}
	if _, err := time.ParseDuration(expression); err == nil {
		return "interval"
	}
	if strings.HasPrefix(expression, "@") || len(strings.Fields(expression)) == 5 {
		return "cron"
	}
	return "legacy_daily"
}

func IntervalSeconds(data map[string]any) int {
	for field, multiplier := range map[string]int{"interval_seconds": 1, "interval_minutes": 60, "interval_hours": 3600} {
		if value := Int(data[field], 0); value > 0 {
			return value * multiplier
		}
	}
	expression := strings.ToLower(strings.TrimSpace(fmt.Sprint(data["schedule_expression"])))
	switch expression {
	case "hourly", "@hourly":
		return 3600
	case "daily", "@daily", "@midnight":
		return 86400
	case "weekly", "@weekly":
		return 604800
	}
	if duration, err := time.ParseDuration(expression); err == nil && duration > 0 {
		return int(duration.Seconds())
	}
	return 0
}

func ParseClock(raw string) (int, int, int, bool) {
	value := strings.TrimSpace(raw)
	if value == "" || value == "<nil>" || strings.Contains(value, " ") {
		return 0, 0, 0, false
	}
	for _, layout := range []string{"15:04:05", "15:04"} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.Hour(), parsed.Minute(), parsed.Second(), true
		}
	}
	return 0, 0, 0, false
}

func ParseWeekday(raw string) (time.Weekday, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "sun", "sunday":
		return time.Sunday, true
	case "1", "mon", "monday":
		return time.Monday, true
	case "2", "tue", "tues", "tuesday":
		return time.Tuesday, true
	case "3", "wed", "wednesday":
		return time.Wednesday, true
	case "4", "thu", "thur", "thurs", "thursday":
		return time.Thursday, true
	case "5", "fri", "friday":
		return time.Friday, true
	case "6", "sat", "saturday":
		return time.Saturday, true
	default:
		return time.Monday, false
	}
}

func Cron(data map[string]any) (cron.Schedule, bool) {
	expression := strings.TrimSpace(fmt.Sprint(data["schedule_expression"]))
	if expression == "" || expression == "<nil>" {
		return nil, false
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	parsed, err := parser.Parse(expression)
	return parsed, err == nil
}

func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" && trimmed != "<nil>" {
			return trimmed
		}
	}
	return ""
}

func Int(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if value, err := typed.Int64(); err == nil {
			return int(value)
		}
	case string:
		var out int
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &out); err == nil {
			return out
		}
	}
	return fallback
}

func location(data map[string]any) *time.Location {
	value := strings.TrimSpace(fmt.Sprint(data["timezone"]))
	if value == "" || value == "<nil>" {
		return time.UTC
	}
	loc, err := time.LoadLocation(value)
	if err != nil {
		return time.UTC
	}
	return loc
}

func clock(data map[string]any) (int, int, int) {
	for _, field := range []string{"time_of_day", "schedule_time", "daily_at", "weekly_at", "monthly_at"} {
		if hour, minute, second, ok := ParseClock(fmt.Sprint(data[field])); ok {
			return hour, minute, second
		}
	}
	if hour, minute, second, ok := ParseClock(fmt.Sprint(data["schedule_expression"])); ok {
		return hour, minute, second
	}
	return 0, 0, 0
}

func weekday(data map[string]any) time.Weekday {
	value, ok := ParseWeekday(FirstNonEmpty(fmt.Sprint(data["day_of_week"]), fmt.Sprint(data["weekday"]), fmt.Sprint(data["week_day"])))
	if !ok {
		return time.Monday
	}
	return value
}

func monthDay(data map[string]any) int {
	for _, field := range []string{"day_of_month", "month_day"} {
		if day := Int(data[field], 0); day > 0 {
			if day > 31 {
				return 31
			}
			return day
		}
	}
	return 1
}

func nextDaily(now time.Time, loc *time.Location, hour, minute, second int) time.Time {
	candidate := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, 0, loc)
	if !candidate.After(now) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return candidate
}

func nextWeekly(now time.Time, loc *time.Location, target time.Weekday, hour, minute, second int) time.Time {
	for offset := 0; offset <= 7; offset++ {
		day := now.AddDate(0, 0, offset)
		if day.Weekday() != target {
			continue
		}
		candidate := time.Date(day.Year(), day.Month(), day.Day(), hour, minute, second, 0, loc)
		if candidate.After(now) {
			return candidate
		}
	}
	day := now.AddDate(0, 0, 7)
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, second, 0, loc)
}

func nextMonthly(now time.Time, loc *time.Location, day, hour, minute, second int) time.Time {
	candidate := monthly(now.Year(), now.Month(), loc, day, hour, minute, second)
	if !candidate.After(now) {
		next := now.AddDate(0, 1, 0)
		candidate = monthly(next.Year(), next.Month(), loc, day, hour, minute, second)
	}
	return candidate
}

func monthly(year int, month time.Month, loc *time.Location, day, hour, minute, second int) time.Time {
	last := time.Date(year, month+1, 0, hour, minute, second, 0, loc).Day()
	if day > last {
		day = last
	}
	if day <= 0 {
		day = 1
	}
	return time.Date(year, month, day, hour, minute, second, 0, loc)
}
