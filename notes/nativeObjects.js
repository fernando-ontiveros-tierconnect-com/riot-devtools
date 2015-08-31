/* 1   Native Objects with the flag multiple disabled */
{
    "_id" : NumberLong(23),
    "groupTypeId" : NumberLong(3),
    "groupTypeName" : "Store",
    "groupTypeCode" : "null",
    "groupId" : NumberLong(3),
    "groupCode" : "SM",
    "groupName" : "Santa Monica",
    "thingTypeId" : NumberLong(1),
    "thingTypeCode" : "default_rfid_thingtype",
    "thingTypeName" : "Default RFID Thing Type",
    "name" : "000000000000000010000",
    "serialNumber" : "000000000000000010000",
    "status" : {
        "thingTypeFieldId" : NumberLong(12),
        "time" : ISODate("2015-08-25T20:14:07.716Z"),
        "value" : "5 strawberries, 7 orange"
    },
    "logicalReader" : {
        "thingTypeFieldId" : NumberLong(3),
        "time" : ISODate("2015-08-27T15:38:18.088Z"),
        "value" : {
            "id" : NumberLong(1),
            "code" : "LR1",
            "name" : "LR1",
            "x" : 10.0000000000000000,
            "y" : 10.0000000000000000,
            "z" : 10.0000000000000000,
            "zoneInId" : NumberLong(1),
            "zoneOutId" : NumberLong(1)
        }
    },
    "shift" : {
        "thingTypeFieldId" : NumberLong(11),
        "time" : ISODate("2015-08-27T19:13:11.099Z"),
        "value" : {
            "id" : NumberLong(3),
            "name" : "weekends",
            "daysOfWeek" : "17",
            "startTimeOfDay" : NumberLong(100),
            "endTimeOfDay" : NumberLong(2359)
        }
    },
    "zone" : {
        "thingTypeFieldId" : NumberLong(4),
        "time" : ISODate("2015-08-27T19:14:59.259Z"),
        "value" : {
            "id" : NumberLong(3),
            "code" : "Stroom",
            "name" : "Stockroom",
            "points" : [
                [
                    -118.4439641438134601,
                    34.0482624093037174
                ],
                [
                    -118.4439424738065014,
                    34.0482158438293965
                ],
                [
                    -118.4438865665123330,
                    34.0482336229401596
                ],
                [
                    -118.4439078174241473,
                    34.0482776882189029
                ],
                [
                    -118.4439207044553086,
                    34.0482832536968658
                ],
                [
                    -118.4439596803050705,
                    34.0482716556122682
                ]
            ]
        }
    }
}


/* 2 Native Objects with multiple values per UDF*/
{
    "_id" : NumberLong(30049),
    "groupTypeId" : NumberLong(3),
    "groupTypeName" : "Store",
    "groupTypeCode" : "null",
    "groupId" : NumberLong(3),
    "groupCode" : "SM",
    "groupName" : "Santa , Monica",
    "thingTypeId" : NumberLong(9),
    "thingTypeCode" : "Native.Objects.Multiple",
    "thingTypeName" : "Native Objects Multiple",
    "name" : "000000000000000000100",
    "serialNumber" : "000000000000000000100",
    "multiLogicalReader" : {
        "thingTypeFieldId" : NumberLong(78),
        "time" : ISODate("2015-08-28T14:10:50.229Z"),
        "value" : [
            {
                "id" : NumberLong(3),
                "code" : "LR3",
                "name" : "LR3",
                "x" : 50.0000000000000000,
                "y" : 50.0000000000000000,
                "z" : 0.0000000000000000,
                "zoneInId" : NumberLong(1),
                "zoneOutId" : NumberLong(3)
            },
            {
                "id" : NumberLong(2),
                "code" : "L2",
                "name" : "L2",
                "x" : 20.0000000000000000,
                "y" : 20.0000000000000000,
                "z" : 1.0000000000000000,
                "zoneInId" : NumberLong(3),
                "zoneOutId" : NumberLong(1)
            }
        ]
    },
    "multiZone" : {
        "thingTypeFieldId" : NumberLong(79),
        "time" : ISODate("2015-08-28T14:18:39.376Z"),
        "value" : [
            {
                "id" : NumberLong(1),
                "code" : "Enance",
                "name" : "Entrance",
                "points" : [
                    [
                        -118.4439805447409952,
                        34.0481198168392751
                    ],
                    [
                        -118.4439724981129984,
                        34.0481025933026302
                    ],
                    [
                        -118.4439327245700042,
                        34.0481144227167363
                    ],
                    [
                        -118.4439406468810034,
                        34.0481312065954569
                    ]
                ]
            },
            {
                "id" : NumberLong(4),
                "code" : "Saloor",
                "name" : "Salesfloor",
                "points" : [
                    [
                        -118.4438802149873027,
                        34.0482288161937987
                    ],
                    [
                        -118.4438541638471065,
                        34.0481705318507437
                    ],
                    [
                        -118.4437470163530151,
                        34.0482032071532075
                    ],
                    [
                        -118.4437725959449921,
                        34.0482613730372492
                    ]
                ]
            }
        ]
    },
    "multiShift" : {
        "thingTypeFieldId" : NumberLong(77),
        "time" : ISODate("2015-08-28T14:19:02.452Z"),
        "value" : [
            {
                "id" : NumberLong(3),
                "name" : "weekends",
                "daysOfWeek" : "17",
                "startTimeOfDay" : NumberLong(100),
                "endTimeOfDay" : NumberLong(2359)
            },
            {
                "id" : NumberLong(1),
                "name" : "DAY-M-W",
                "daysOfWeek" : "23456",
                "startTimeOfDay" : NumberLong(800),
                "endTimeOfDay" : NumberLong(1700)
            },
            {
                "id" : NumberLong(2),
                "name" : "Nights",
                "daysOfWeek" : "23456",
                "startTimeOfDay" : NumberLong(2101),
                "endTimeOfDay" : NumberLong(600)
            }
        ]
    }
}
