package oplog

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// Options : MongoDB connection information for oplog tailing
type Options struct {
	Addrs      []string
	Username   string
	Password   string
	ReplicaSet string
	DB         string
	Collection string
	Events     []string
}

// Log : Oplog document
type Log struct {
	Timestamp    bson.MongoTimestamp    `json:"ts" bson:"ts"`
	HistoryID    int64                  `json:"h" bson:"h"`
	MongoVersion int                    `json:"v" bson:"v"`
	Operation    string                 `json:"op" bson:"op"`
	Namespace    string                 `json:"ns" bson:"ns"`
	Doc          map[string]interface{} `json:"o" bson:"o"`
	Update       map[string]interface{} `json:"o2" bson:"o2"`
	InitialID    interface{}            `json:"initialId" bson:"initialId"`
}

// MgoConn : MongoDB connect
func (o *Options) MgoConn(e chan error) (*mgo.Session, *mgo.Collection, *mgo.Collection) {
	var mgoSess *mgo.Session
	var oplogColl *mgo.Collection
	var lastReadedOplogColl *mgo.Collection

	m := &mgo.DialInfo{
		Addrs:          o.Addrs,
		Database:       "local",
		Direct:         false,
		FailFast:       false,
		Username:       o.Username,
		Password:       o.Password,
		ReplicaSetName: o.ReplicaSet,
		Source:         "admin",
		Mechanism:      "",
		Timeout:        time.Duration(0),
		PoolLimit:      0,
	}
	sess, err := mgo.DialWithInfo(m)
	if err != nil {
		e <- err
		return mgoSess, oplogColl, lastReadedOplogColl
	}
	mgoSess = sess
	oplogColl = mgoSess.DB("local").C("oplog.rs")
	lastReadedOplogColl = mgoSess.DB("local").C("lastReadedOplog")

	return mgoSess, oplogColl, lastReadedOplogColl
}

func (o *Options) MgoDatabaseConn(e chan error) (*mgo.Session, *mgo.Collection) {
	var collection *mgo.Collection

	m := &mgo.DialInfo{
		Addrs:          o.Addrs,
		Database:       "local",
		Direct:         false,
		FailFast:       false,
		Username:       o.Username,
		Password:       o.Password,
		ReplicaSetName: o.ReplicaSet,
		Source:         "admin",
		Mechanism:      "",
		Timeout:        time.Duration(0),
		PoolLimit:      0,
	}
	sess, err := mgo.DialWithInfo(m)
	if err != nil {
		e <- err
		return nil, collection
	}
	collection = sess.DB(o.DB).C(o.Collection)

	return sess, collection
}

func (o *Options) TailFromScratch(l chan []*Log, e chan error) {
	go o.Tail(l, e)
	if !hasEvent(o.Events, "insert") {
		return
	}

	log.Println("[Oplog Tail Scratch Start] collection", o.Collection)

	localSess, oplogCol, lastReadedOplogColl := o.MgoConn(e)
	sess, col := o.MgoDatabaseConn(e)
	defer localSess.Close()
	defer sess.Close()

	ns := fmt.Sprintf("initial.%s.%s", o.DB, o.Collection)
	lastReadedQuery := bson.M{"ns": ns}

	initialID, finalID := getInitialAndFinalID(oplogCol, col, lastReadedOplogColl, lastReadedQuery)

	for {
		registers := []map[string]interface{}{}
		err := col.Find(bson.M{"_id": bson.M{"$gte": initialID, "$lt": finalID}}).Sort("-_id").Limit(10000).All(&registers)

		if len(registers) == 0 {
			break
		}

		if err != nil {
			e <- err
		}

		oplogs := getOplogsFromRegisters(registers, ns, initialID)

		if len(oplogs) != 0 {
			finalID = registers[len(registers)-1]["_id"]
			l <- oplogs

			_, err = lastReadedOplogColl.RemoveAll(lastReadedQuery)
			if err != nil {
				e <- err
			}
			err = lastReadedOplogColl.Insert(oplogs[len(oplogs)-1])
			if err != nil {
				e <- err
			}
		}
	}
}

// Tail : MongoDB oplog tailing start
func (o *Options) Tail(l chan []*Log, e chan error) {
	eventsQuery := getEventsQuery(o.Events, e)
	namespaceQuery := getNamespaceQuery(o.DB, o.Collection)

	mgoSess, oplogColl, lastReadedOplogColl := o.MgoConn(e)
	defer mgoSess.Close()
	log.Println("[Oplog Tail Start] collection", o.Collection, time.Now())

	ns := fmt.Sprintf("%s.%s", o.DB, o.Collection)
	lastReadedQuery := bson.M{"ns": ns}

	lastOplog := Log{}
	err := lastReadedOplogColl.Find(lastReadedQuery).One(&lastOplog)
	if err != nil {
		oplogColl.Find(nil).Sort("$natural").One(&lastOplog)
	}
	sTime := lastOplog.Timestamp

	for {
		var fetchedLog = []*Log{}
		err := oplogColl.Find(bson.M{
			"$and": []bson.M{
				bson.M{"ts": bson.M{"$gt": sTime}},
				namespaceQuery,
				eventsQuery,
			},
		}).Sort("$natural").Limit(5000).All(&fetchedLog)
		if err != nil {
			e <- err
		}
		if len(fetchedLog) != 0 {
			sTime = fetchedLog[len(fetchedLog)-1].Timestamp
			l <- fetchedLog

			_, err = lastReadedOplogColl.RemoveAll(lastReadedQuery)
			if err != nil {
				e <- err
			}
			err = lastReadedOplogColl.Insert(fetchedLog[len(fetchedLog)-1])
			if err != nil {
				e <- err
			}
		}
	}
}

func getEventsQuery(events []string, e chan error) bson.M {
	eventsFilter := []bson.M{}
	for _, v := range events {
		if v != "insert" && v != "update" && v != "delete" {
			e <- errors.New("Events type must be insert, update, delete")
		}
		if v == "insert" {
			eventsFilter = append(eventsFilter, bson.M{"op": "i"})
		} else if v == "update" {
			eventsFilter = append(eventsFilter, bson.M{"op": "u"})
		} else if v == "delete" {
			eventsFilter = append(eventsFilter, bson.M{"op": "d"})
		}
	}

	return bson.M{
		"$or": eventsFilter,
	}
}

func getNamespaceQuery(database, collection string) bson.M {
	if database != "*" && collection != "*" {
		return bson.M{"ns": database + "." + collection}
	} else if database != "*" && collection == "*" {
		return bson.M{"ns": bson.M{"$regex": bson.RegEx{fmt.Sprintf("^%s.", database), ""}}}
	} else if database == "*" && collection != "*" {
		return bson.M{"ns": bson.M{"$regex": bson.RegEx{fmt.Sprintf(".%s$", collection), ""}}}
	}
	return bson.M{}
}

func hasEvent(events []string, event string) bool {
	for _, ev := range events {
		if ev == event {
			return true
		}
	}
	return false
}

func getInitialAndFinalID(oplogCol, col, lastReadedOplogCol *mgo.Collection, lastReadedQuery bson.M) (interface{}, interface{}) {
	firstOplog := Log{}
	err := lastReadedOplogCol.Find(lastReadedQuery).One(&firstOplog)

	var initialID interface{}
	var finalID interface{}
	if err != nil {
		firstRegister := map[string]interface{}{}

		col.Find(nil).Sort("_id").One(&firstRegister)
		oplogCol.Find(bson.M{"op": "i"}).Sort("$natural").One(&firstOplog)

		initialID = firstRegister["_id"]
		if firstOplog.Operation == "u" {
			finalID = firstOplog.Update["_id"]
		} else {
			finalID = firstOplog.Doc["_id"]
		}
	} else {
		initialID = firstOplog.InitialID
		finalID = firstOplog.Doc["_id"]
	}

	return initialID, finalID
}

func getOplogsFromRegisters(registers []map[string]interface{}, namespace string, initialID interface{}) []*Log {
	oplogs := []*Log{}
	for _, register := range registers {
		id := register["_id"].(bson.ObjectId)
		ts, _ := bson.NewMongoTimestamp(id.Time(), 0)
		oplg := Log{
			Timestamp: ts,
			Namespace: namespace,
			Operation: "i",
			Doc:       register,
			InitialID: initialID,
		}

		oplogs = append(oplogs, &oplg)
	}

	return oplogs
}
