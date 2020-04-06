PREMADE_REPORTS = [
    {
        "name": "Audience Overview",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:sessionsPerUser",
            "ga:pageviews",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:language",
            "ga:country",
            "ga:city",
            "ga:browser",
            "ga:operatingSystem",
            "ga:screnResolution",
            "ga:year",
            "ga:month",
            "ga:hour",
        ],
        "default_dimensions": [
            "ga:date"
        ]
    },
    {
        "name": "Audience Geo Location",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:country",
            "ga:city",
            "ga:continent",
            "ga:subContinent"
        ],
        "default_dimensions": [
            "ga:date",
            "ga:country",
            "ga:city",
            "ga:continent",
            "ga:subContinent"
        ]
    },
    {
        "name": "Audience Technology",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:browser",
            "ga:operatingSystem",
            "ga:screenResolution",
            "ga:screenColors",
            "ga:flashVersion",
            "ga:javaEnabled",
            "ga:hostname"
        ],
        "default_dimensions": [
            "ga:date",
            "ga:browser",
            "ga:operatingSystem",
        ]
    },
    {
        "name": "Acquisition Overview",
        "metrics": [
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:pageviewsPerSession",
            "ga:avgSessionDuration",
            "ga:bounceRate"
        ],
        "dimensions": [
            'ga:acquisitionMedium',
            'ga:acquisitionSource',
            'ga:acquisitionSourceMedium',
            'ga:acquisitionTrafficChannel'
        ],
        "default_dimensions": [
            "ga:date",
            "ga:acquisitionTrafficChannel",
            "ga:acquisitionSource",
            "ga:acquisitionSourceMedium",
            "ga:acquisitionMedium",
        ]
    },
    {
        "name": "Behavior Overview",
        "metrics": [
            "ga:pageviews",
            "ga:uniquePageviews",
            "ga:avgTimeOnPage",
            "ga:bounceRate",
            "ga:exitRate",
            "ga:exits"
        ],
        "dimensions": [
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour",
            "ga:pagePath",
            "ga:pageTitle",
            "ga:searchKeyword",
            "ga:eventCategory"
        ],
        "default_dimensions": [
            "ga:date",
            "ga:pagePath",
            "ga:pageTitle",
            "ga:searchKeyword"
        ]
    },
    {
        "name": "Ecommerce Overview",
        "metrics": ["ga:transactions"],
        "dimensions": [
            "ga:transactionId",
            "ga:campaign",
            "ga:source",
            "ga:medium",
            "ga:keyword",
            "ga:socialNetwork"
        ],
        "default_dimensions": [
            "ga:transactionId",
            "ga:campaign",
            "ga:source",
            "ga:medium",
            "ga:keyword",
            "ga:socialNetwork"
        ]
    }
]
