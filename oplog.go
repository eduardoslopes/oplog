package oplog

import (
	"errors"
	"fmt"
	"log"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

// Tail : MongoDB oplog tailing start
func (o *Options) Tail(l chan []*Log, e chan error) {
	eventsQuery := getEventsQuery(o.Events, e)
	namespaceQuery := getNamespaceQuery(o.DB, o.Collection)

	mgoSess, oplogColl, lastReadedOplogColl := o.MgoConn(e)
	defer mgoSess.Close()
	log.Println("[Oplog Tail Start] ", time.Now())

	ns := fmt.Sprintf("%s.%s", o.DB, o.Collection)
	lastReadedQuery := bson.M{"ns": ns}

	lastOplog := Log{}
	err := lastReadedOplogColl.Find(lastReadedQuery).One(&lastOplog)
	if err != nil {
		oplogColl.Find(nil).Sort("-$natural").One(&lastOplog)
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
