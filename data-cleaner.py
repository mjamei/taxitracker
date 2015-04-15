from db import DB
import pandas as pd
import csv


def download(start, end):
    db = DB(dbname="sffd", username="bayes_readonly", password="ymM7hkjU$6d",
        hostname="127.0.0.1", dbtype="postgres", port=5434)

    query = ("""
        SELECT
            *
        FROM marvlis 
        WHERE rptdatetime > to_date('%s', 'DD Mon YYYY')
        AND rptdatetime < to_date('%s', 'DD Mon YYYY');
        """) % (start, end)

    df = db.query(query)

    df['lat'] = df.latitude
    df['lon'] = df.longitude
    df.drop(['latitude', 'longitude'], axis=1, inplace=True)
    csv_file = open('data/oneday.csv', 'w')
    df.ix[:200].to_csv(csv_file)
    csv_file.close()

    # Read in raw data from csv
    rawData = csv.reader(open('data/oneday.csv', 'rb'))

    # the template. where data from the csv will be formatted to geojson
    template = \
        ''' \
        { "type" : "Feature",
            "id" : %s,
                "geometry" : {
                    "type" : "Point",
                    "coordinates" : ["%s","%s"]},
            "properties" : { "name" : "%s", "value" : "%s"}
            },
        '''

    # the head of the geojson file
    output = \
        ''' \
    { "type" : "Feature Collection",
        {"features" : [
        '''

    # loop through the csv by row skipping the first
    iter = 0
    for row in rawData:
        iter += 1
        if iter >= 2:
            id = row[0]
            lat = row[1]
            lon = row[2]
            name = row[3]
            pop = row[4]
            output += template % (row[0], row[1], row[2], row[3], row[4])
            
    # the tail of the geojson file
    output += \
        ''' \
        ]
    }
        '''
        
    # opens an geoJSON file to write the output to
    outFileHandle = open("data/oneday.json", "w")
    outFileHandle.write(output)
    outFileHandle.close()

        


    

    return csv

if __name__ == "__main__":
    download('07 Sep 2012', '08 Sep 2012')