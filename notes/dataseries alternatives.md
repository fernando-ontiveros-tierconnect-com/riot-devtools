# dataseries alternatives

usar map reduce

key :   id, serialnumber, thingtype

almacenar por horas, y en cada hora tener el ultimo value, y el average, and some statistics


-- query para obtener la cantidad records por serialnumber y field

db.thingHistory.aggregate( [
     { $group: {
         _id: { id: "$id", serialNumber: "$serialNumber", field: "$field" },
         count: {$sum: 1 } }
     },
     { $match: {count: {$gt: 7} }},
     { $project: { _id: 0, serialNumber: "$_id.serialNumber", count: 1, field: "$_id.field"}}
  ] )


--- limites en hora

{
    "result" : [
        {
            "minDate" : ISODate("2015-07-28T16:20:16.771Z"),
            "maxDate" : ISODate("2015-07-30T15:07:18.616Z")
        }
    ],
    "ok" : 1.0000000000000000
}



- encontrar los limites
- construir una tabla donde cada registro debe ser por hora


donde entra mr

como llenar los registros en blanco?

la estructura del registro mr puede ser...


id : {id, field, date}
value: {
  array [0..59][0..59]
  lastValue: n;  //this is value for latest
  average,
  min, max
}

the procedure to build the matrix will be:

go over the whole table and project records
in an incremental way, just process from some starting date or any period of time


so, mr should be something like this:

- emit the record...

maybe build the complete the table in some way

- puede ser el primer proceso, pero..

- en cualquier momento talvez tengamos que rellenar los espacios en blanco, los gaps

map = function () {
    emit(
        { this.id, this.field, this.time },
        {
            "lastValue"   : null,
            "average" :  null,
            "min" : null,
            "max" : null
        }
    );
}

reduce = function (key, values) {
    res = {
        "lastValue"   : null,
        "average" :  null,
        "min" : null,
        "max" : null
    };
    return res;
}

1438346328
1438100416
    235912 secs

-- ----------------

- select id, serialnumber, count(*) from thingHistory group by id, serialnumber having count > 10

db.thingHistory.aggregate( [
     { $group: {
         _id: { id: "$id", serialNumber: "$serialNumber", field: "$field" },
         count: {$sum: 1 } }
     },
     { $match: {count: {$gt: 10} }},
     { $project: { _id: 0, serialNumber: "$_id.serialNumber", count: 1, field: "$_id.field"}}
  ] )


-  query to get min and max dates

var boundDates = db.thingHistory.aggregate( [
     { $group: {
         _id: 1 ,
         min: {$min: "$time" },
         max: {$max: "$time" } }
     }
  ] );
var dateStart = parseInt( boundDates.result[0].min.getTime() /3600000);
var dateEnd   = parseInt( boundDates.result[0].max.getTime() /3600000);

for ( d = dateStart; d <= dateEnd; d++ ) {
    var dateHour = new Date(d*3600000);
    print (d, dateHour);
}


-  query ABOUT thingHistory


var boundDates = db.thingHistory.aggregate( [
     { $group: {
         _id: 1 ,
         min: {$min: "$time" },
         max: {$max: "$time" } }
     }
  ] );
var dateStart = parseInt( boundDates.result[0].min.getTime() /3600000);
var dateEnd   = parseInt( boundDates.result[0].max.getTime() /3600000);


var myCursor = db.things.find( { _id : { $lt : 40} } );

var drop = db.timeseries.drop();

while (myCursor.hasNext()) {
    var thing = myCursor.next();


    for ( d = dateStart; d <= dateEnd; d++ ) {
        var date  = new Date( d*3600000 );
        var year  = NumberInt( date.getUTCFullYear());
        var month = NumberInt( date.getUTCMonth());
        var day   = NumberInt( date.getUTCDate());
        var hour  = NumberInt( date.getUTCHours());

        var noTimeseriesFields = ["_id", "groupTypeId", "groupTypeName", "groupTypeCode", "groupId", "groupCode", "groupName",
            "thingTypeId", "thingTypeCode", "lastDetectTime", "name", "serialNumber" ];
        for ( var field in thing ) {
            if ( noTimeseriesFields.indexOf(field) == -1 ) {
                var doc = {
                    "_id"   : {
                        "id" : thing._id,
                        "field" : field,
                        "year"  : year,
                        "month" : month,
                        "day"   : day,
                        "hour"  : hour
                    },
                    "serialNumber" : thing.serialNumber,
                    "lastValue" : null,
                    "lastDate"  : null,
                    "matrix"    : {}
                };
                db.timeseries.insert( doc );
            }
        }

    }
};


- process and fill gaps


var boundDates = db.thingHistory.aggregate( [
     { $group: {
         _id: 1 ,
         min: {$min: "$time" },
         max: {$max: "$time" } }
     }
  ] );
var dateStart = parseInt( boundDates.result[0].min.getTime() /3600000);
var dateEnd   = parseInt( boundDates.result[0].max.getTime() /3600000);


//var myCursor = db.things.find( { _id : { $lt : 40} } );
var myCursor = db.things.find( {} );

var drop = db.timeseries.drop();

while (myCursor.hasNext()) {
    var thing = myCursor.next();


    for ( d = dateStart; d <= dateEnd; d++ ) {
        var date  = new Date( d*3600000 );
        var year  = NumberInt( date.getUTCFullYear());
        var month = NumberInt( date.getUTCMonth());
        var day   = NumberInt( date.getUTCDate());
        var hour  = NumberInt( date.getUTCHours());

        var noTimeseriesFields = ["_id", "groupTypeId", "groupTypeName", "groupTypeCode", "groupId", "groupCode", "groupName",
            "thingTypeId", "thingTypeCode", "lastDetectTime", "name", "serialNumber" ];
        for ( var field in thing ) {
            if ( noTimeseriesFields.indexOf(field) == -1 ) {
                var doc = {
                    "_id"   : {
                        "id" : thing._id,
                        "field" : field,
                        "year"  : year,
                        "month" : month,
                        "day"   : day,
                        "hour"  : hour
                    },
                    "serialNumber" : thing.serialNumber,
                    "lastValue" : null,
                    "lastDate"  : null,
                    "matrix"    : {}
                };
                db.timeseries.insert( doc );
            }
        }

    }
};

var numThings       = db.things.count({});
var numTimeseries   = db.timeseries.count({});
var numThingHistory = db.thingHistory.count({});
print ("things:     " + numThings + "\ntimeseries: " + numTimeseries + "\nhistory:    " + numThingHistory);





-
- query for the reports


//var drop = db.report.drop();

var reportDate = new ISODate("2015-08-01T01:00:00Z");

var query = {
    "groupId" : { $in : [ NumberLong(3) ]},
    "thingTypeId" : { $in: [NumberLong(6)] },
    "fieldName"   : { $in: ["brand", "zone"] },
    "prevEnd"     : { $lt : reportDate.getTime()  },
    "nextStart"   : { $gt : reportDate.getTime()  }
};

var myCursor = db.timeseries.find( query );



while (myCursor.hasNext()) {
    var timeserie = myCursor.next();

    var str = [];
    var value = null;
    var time = null;
    for (var i = timeserie.time.length ; i >= 0; i-- ) {
        if (typeof timeserie.time[i] != "undefined" && timeserie.time[i] != 0 ) {
            if (value == null ) {
                value = timeserie.value[i];
                time  = timeserie.time[i];
            }
            if (timeserie.time[i] > reportDate ) {continue}

            //print (timeserie.prevEnd + " " + timeserie.time[i]);

            //str.push( new Date( timeserie.time[i] ) );
        }
    }
    var timeserieDate = new Date(time);
print ( myCursor.count() );
    print ( timeserie.serialNumber + " " + timeserie.time.length + " " + timeserie.prevEnd + " " + timeserieDate + " " + value);

}

