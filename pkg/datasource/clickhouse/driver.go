package clickhouse

import (
	"database/sql"
	"encoding/csv"
	"io"

	"github.com/mitchellh/mapstructure"
	"github.com/oleh-ozimok/go-clickhouse"

	"github.com/oleh-ozimok/copysql/pkg/datasource"

	_ "github.com/mailru/go-clickhouse"
	"github.com/pkg/errors"

	"github.com/oleh-ozimok/copysql/pkg/datasource"
)

const driverName = "clickhouse"
const maxLength = 100 // todo: remove this dirty hack after driver refactor

func init() {
	datasource.Register(driverName, &detectorFactory{})
}

type detectorFactory struct{}

func (f *detectorFactory) Create(parameters map[string]interface{}) (datasource.Driver, error) {
	return FromParameters(parameters)
}

type DriverParameters struct {
	Address  string
	Username string
	Password string
	Database string
}

type Driver struct {
	cluster *clickhouse.Cluster
}

func FromParameters(parameters map[string]interface{}) (datasource.Driver, error) {
	params := DriverParameters{}

	if err := mapstructure.Decode(parameters, &params); err != nil {
		return nil, err
	}

	return New(params), nil
}

func New(params DriverParameters) *Driver {
	dsn := "http://" + params.Username + ":" + params.Password + "@" + params.Address

	return &Driver{
		cluster: clickhouse.NewCluster(clickhouse.NewConn(dsn, clickhouse.NewHttpTransport(32))),
	}
}

func (d *Driver) Open() error {
	d.cluster.Check()

	if d.cluster.IsDown() {
		return errors.New("all clickhouse hosts down")
	}

	return nil
}

func (d *Driver) CopyFrom(r io.Reader, table string) error {
	query := clickhouse.BuildCSVInsert(table, r)
	return query.Exec(d.cluster.ActiveConn())
}

func (d *Driver) CopyTo(w io.Writer, query string) error {
	iter := clickhouse.NewQuery(query).Iter(d.cluster.ActiveConn())

	if iter.Error() != nil {
		return iter.Error()
	}

	readColumns := make([]interface{}, maxLength)
	writeColumns := make([]sql.NullString, maxLength)

	for i := range writeColumns {
		readColumns[i] = &writeColumns[i]
	}

	record := make([]string, maxLength)

	csvWriter := csv.NewWriter(w)
	csvWriter.UseCRLF = true

	for iter.Scan(readColumns...) {
		for i := range writeColumns {
			record[i] = writeColumns[i].String
		}

		err := csvWriter.Write(record)
		if err != nil {
			return err
		}
	}

	csvWriter.Flush()

	return iter.Error()
}

func (d *Driver) Close() error {
	return nil
}
