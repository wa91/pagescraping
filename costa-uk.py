import requests,json,pandas,sqlalchemy,psycopg2,datetime,csv
rsa =[]
# read any import data
with open("C:/Local/pagescraping/scripts/uk-denser-ll.csv","r") as csvfile:
    sred = csv.reader(csvfile, delimiter=',', quotechar='"')
    # skip header
    next(sred)
    # loop through input - calling API and combining json
    # change acc.                       !
    for row in sred:
        url = 'https://www.costa.co.uk/api/locations/stores?latitude='+row[0]+'&longitude='+row[1]+'&maxrec=0'
        r = requests.get(url,verify=False)
        print(url)
        try:
            rs = json.loads(r.text)
            rsa.extend(rs['stores'])
        except:
            print("Error")
            print(r.text)

# flatten json acc.                     !
elevations = json.dumps(rsa)
df = pandas.read_json(elevations)

dfa = df['storeAddress'].apply(pandas.Series)

df = pandas.concat([dfa,df],axis=1)

# drop duplicates based on id
df.sort_values("storeNo8Digit", inplace=True)
df.drop_duplicates(subset="storeNo8Digit", keep='first', inplace=True)

#create address filed
df['address'] = df['addressLine1'] + df['addressLine2'] + df['addressLine3'] +', ' + df['city'] + ', ' + df['postCode'].astype(str)
# create fields - exclude extras           !
df = df[['storeNo8Digit','latitude','longitude','addressLine1','city','postCode','storeNameExternal','storeType','telephone','email','address']]
df = df.rename({"storeNo8Digit": "id","addressLine1": "street","postCode": "postcode","storeNameExternal": "name","storeType": "store_type","telephone": "phone"},axis=1)
df['brand'] = 'costa'
df['source'] = 'fme'
df['country'] = 'GB'
df['updated'] = str(datetime.datetime.now())


# trim values for PG - uncomment acc.   !
df['address'] = df['address'].str[:250]
df['postcode'] = df['postcode'].astype(str).str[:20]
df['city'] = df['city'].str[:50]
df['name'] = df['name'].str[:250]
df['latitude'] = df['latitude'].astype(str).str[:20]
df['longitude'] = df['longitude'].astype(str).str[:20]
df['phone'] = df['phone'].astype(str).str[:50]
#df['url'] = df['url'].str[:250]
df['id'] = df['id'].astype(str).str[:20]
df['street'] = df['street'].str[:20]

# connect to PG
engine = sqlalchemy.create_engine('postgresql://xxx:xxx@xxx:5432/xxx',echo=True)

# delete previous results - change acc.  !
connectiona = engine.connect()
my_query = 'delete from pagescraping where source = \'fme\' and brand = \'costa\' and country=\'GB\''
r1 = connectiona.execute(my_query)

# upload results
df.to_sql('pagescraping', engine, if_exists='append', index=False)

# create geometry - change filter acc.  !
connection = engine.connect()
my_query = 'update pagescraping set geom = ST_GeomFromText(\'POINT(\'||longitude||\' \'||latitude||\')\',4326) ' \
           'where source = \'fme\' and brand = \'costa\''
connection.execute(my_query)

print('Inserted ' + str(len(df.index)) + ' records.')
