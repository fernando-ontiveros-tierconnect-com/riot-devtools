done:
- restore the sharaf backup in Mongo Server
- thingSnapshots aren't timeseries as we agreed with T and Waldo, but each record(document) is a snapshots of whole thing, when an udfs had changed.
- write map reduce functions to generate a similar "filled-dense" reports whole table in 57 mins in an old dualcore laptop

{
    "_id" : NumberLong(353),
    "value" : {
        "_id" : NumberLong(353),
        "groupTypeName" : "Facility",
        "groupTypeCode" : "",
        "groupCode" : "I01",
        "groupName" : "UAE, Dubai, IBN Battuta Mall",
        "thingTypeCode" : "sharaf.rfid",
        "name" : "AE1000000000000000001419",
        "serialNumber" : "AE1000000000000000001419",
        "zone" : {
            "thingTypeFieldId" : NumberLong(353),
            "time" : ISODate("2015-07-11T14:42:11.933Z"),
            "value" : "Exit Front"
        },
        "times" : 7.0000000000000000,
        "status" : {
            "thingTypeFieldId" : NumberLong(353),
            "time" : ISODate("2015-07-11T14:41:47.268Z"),
            "value" : "Sold"
        },
        "locationXYZ" : {
            "thingTypeFieldId" : NumberLong(353),
            "time" : ISODate("2015-07-11T14:42:11.933Z"),
            "value" : "40.99;17.0;3.4"
        },
        "location" : {
            "thingTypeFieldId" : NumberLong(353),
            "time" : ISODate("2015-07-11T14:42:11.933Z"),
            "value" : "55.122643442016646;25.0475197135983;3.4"
        },
        "logicalReader" : {
            "thingTypeFieldId" : NumberLong(353),
            "time" : ISODate("2015-07-11T14:42:11.933Z"),
            "value" : "Main Entrance"
        }
    }
}


next steps
- we should define exactly what the "filled-dense" structure should be
- what should deal udfs with the timeseries flag disabled, should they go to the report?
- do we convert the Sharaf thingSnapshot to segment(buckets) timeseries structure?

- changes in corebridge
- changes in UI
 100
  18
  55

27 mins for 100,000 things  = 60 things/sec

35:40
33:04
2:36  156

















