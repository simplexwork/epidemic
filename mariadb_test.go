package epidemic

import (
	"context"
	"testing"
	"time"
)

func Test(t *testing.T) {
	option := &MariadbOption{
		ServerID: 100,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
		Charset:  "UTF-8",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mariadb := NewMariadb(ctx, option)
	if err := mariadb.Open(); err != nil {
		t.Fatal(err)
	}
	for {
		row, err := mariadb.Read()
		if err != nil {
			t.Fatal(err)
		}
		if row.Action() == ActUpdate {
			t.Log(row.GetFieldValue("title"))
		}
	}
}
