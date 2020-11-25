package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq" // PostgreSQL driver
)

const sqlDriverName = "postgres"

const LOG = "log"

const ERROR = "error"

type DBConfig struct {
	User,
	Password,
	Host string
	Port int
	Database,
	SSLmode string
}

type Postgres struct {
	config            *DBConfig
	conn              *sql.DB
	listener          *pq.Listener
	connectionInfo    string
	listenIdleTimeout time.Duration
	handler           func(string)
	errorHandler      func(error)
}

func NewPostgres() *Postgres {
	return &Postgres{}
}

func (ptr *Postgres) LoadConfig(config *DBConfig) error {
	if len(config.Host) == 0 {
		return errors.New("db config failed, host not found")
	}
	if config.Port == 0 {
		return errors.New("db config failed, port not found")
	}
	if len(config.User) == 0 || len(config.Password) == 0 {
		return errors.New("db config failed, login or password not found")
	}
	if len(config.Database) == 0 {
		return errors.New("db config failed, database name not found")
	}
	ptr.config = config
	return nil
}

func (ptr *Postgres) Connect() (err error) {
	dbConfig := ptr.config
	ptr.connectionInfo = fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.Database,
		dbConfig.SSLmode,
	)

	ptr.conn, err = sql.Open(sqlDriverName, ptr.connectionInfo)
	if err != nil {
		return errors.New("connection failed, " + err.Error())
	}

	return nil
}

/*
Load - selecting data from DB
*/
func (ptr *Postgres) Load(ctx context.Context, query string) (*sql.Rows, error) {
	if err := ptr.checkConnection(ctx); err != nil {
		return nil, err
	}

	rows, err := ptr.Exec(ctx, query)
	if err != nil {
		return rows, err
	}

	return rows, nil
}

/*
Save — method inserts in DB row on duplicate key updates fields
*/
func (ptr *Postgres) Save(ctx context.Context, table string, fields []string, values []interface{}, keys []string) (sql.Result, error) {
	if len(fields) != len(values) {
		return nil, errors.New("length of fields and length of values are different")
	}
	query := ptr.generateInsertQuery(table, fields)
	query += ptr.generateOnConflictQuery(fields, keys)
	return ptr.execute(ctx, query, values)
}

/*
Create - creating new row in DB. Does not updates on conflict
*/
func (ptr *Postgres) Create(ctx context.Context, table string, fields []string, values []interface{}) (sql.Result, error) {
	if len(fields) != len(values) {
		return nil, errors.New("length of fields and length of values are different")
	}
	query := ptr.generateInsertQuery(table, fields)
	return ptr.execute(ctx, query, values)
}

func (ptr *Postgres) execute(ctx context.Context, query string, values []interface{}) (res sql.Result, err error) {
	if err = ptr.checkConnection(ctx); err != nil {
		return
	}

	stmt, err := ptr.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, errors.New("preparing statement error, " + err.Error() + ", query: " + query)
	}
	defer stmt.Close()

	return stmt.ExecContext(ctx, values...)
}

func (ptr *Postgres) Update(ctx context.Context, table string, fields []string, values []interface{}, condition string) (sql.Result, error) {
	if len(fields) != len(values) {
		return nil, errors.New("length of fields and length of values are different")
	}
	query := ptr.generateUpdateQuery(table, fields, condition)
	return ptr.execute(ctx, query, values)
}

/*
Exec - executing prepared SQL string
*/
func (ptr *Postgres) Exec(ctx context.Context, SQL string) (rows *sql.Rows, err error) {
	if err = ptr.checkConnection(ctx); err != nil {
		return
	}

	return ptr.conn.QueryContext(ctx, SQL)
}

func (ptr *Postgres) checkConnection(ctx context.Context) error {
	if ptr.conn == nil {
		return ptr.Connect()
	}
	if ptr.conn.Stats().OpenConnections == 0 {
		return ptr.Connect()
	}
	return nil
	//return ptr.conn.PingContext(ctx)
}

func (ptr *Postgres) generateInsertQuery(table string, fields []string) string {
	query := "INSERT INTO " + table + " (" + strings.Join(fields, ",") + ") VALUES "
	var placeholder []string

	for i := range fields {
		key := strconv.Itoa((i + 1))
		placeholder = append(placeholder, "$"+key)
	}
	query += "(" + strings.Join(placeholder, ",") + ")"
	return query
}

func (ptr *Postgres) generateUpdateQuery(table string, fields []string, condition string) string {
	query := "UPDATE " + table + " SET "
	var placeholder []string

	for i, name := range fields {
		key := strconv.Itoa((i + 1))
		placeholder = append(placeholder, name+"=$"+key)
	}
	query += strings.Join(placeholder, ",")

	if len(condition) != 0 {
		query += " WHERE " + condition
	}

	return query
}

func (ptr *Postgres) generateOnConflictQuery(fields []string, keys []string) string {
	if len(keys) == 0 {
		return " ON CONFLICT DO NOTHING "
	}

	var idx []string
	for _, key := range keys {
		idx = append(idx, key)
	}

	query := " ON CONFLICT (" + strings.Join(idx, ",") + ") DO UPDATE SET "

	var placeholder []string
	for i, field := range fields {
		key := strconv.Itoa(i + 1)
		value := field + " = $" + key + " "
		placeholder = append(placeholder, value)
	}

	query += strings.Join(placeholder, ",")
	query = query[:len(query)-1]
	return query
}

func (ptr *Postgres) InsertBatch(ctx context.Context, table string, fields []string, rows []interface{}, onDuplicate interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	if err := ptr.checkConnection(ctx); err != nil {
		return err
	}

	var values = []interface{}{}
	SQL := "insert into " + table + " (" + strings.Join(fields, ",") + ") values "

	var placeholder []string

	counter := 0
	for _, row := range rows {
		r := row.([]interface{})
		var pl []string
		for i := 0; i < len(r); i++ {
			counter++
			pl = append(pl, "$"+strconv.Itoa(counter))
			values = append(values, r[i])
		}
		placeholder = append(placeholder, "("+strings.Join(pl, ",")+")")
	}

	SQL += strings.Join(placeholder, ",")
	// SQL = SQL[0 : len(SQL)-1]
	if onDuplicate != nil {
		SQL += " ON CONFLICT " + onDuplicate.(string)
	}

	stmt, err := ptr.conn.Prepare(SQL)
	if err != nil {
		return errors.New("preparing statement error, " + err.Error() + ", query: " + SQL)
	}
	defer stmt.Close()

	_, err = stmt.Exec(values...)

	return err
}

func (ptr *Postgres) Listen(ctx context.Context, channel string) error {
	if err := ptr.checkConnection(ctx); err != nil {
		return err
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			if ptr.errorHandler != nil {
				ptr.errorHandler(err)
			}
		}
	}

	ptr.listener = pq.NewListener(ptr.connectionInfo, 10*time.Second, time.Minute, reportProblem)

	if err := ptr.listener.Listen(channel); err != nil {
		return err
	}

	for {
		ptr.HandleListen()

		if IsContextCancelled(ctx) {
			break
		}
	}

	return nil
}

func (ptr *Postgres) HandleListen() {
	l := ptr.listener
	for {
		select {
		case n := <-l.Notify:
			if n != nil {
				ptr.handler(n.Extra)
			}
			return

		case <-time.After(ptr.listenIdleTimeout):
			go func() {
				l.Ping()
			}()
			return
		}
	}
}

func (ptr *Postgres) OnData(handler func(string)) {
	ptr.handler = handler
}

func (ptr *Postgres) OnError(handler func(error)) {
	ptr.errorHandler = handler
}

func (m *Postgres) GetDBInfo() string {
	return m.config.Host + "/" + m.config.Database
}

func (ptr *Postgres) Close() error {
	if ptr.conn != nil {
		return ptr.conn.Close()
	}
	return nil
}
