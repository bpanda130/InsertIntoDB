import pandas as pd
from sqlalchemy import create_engine

def using_alchemy(df):
    try:
        engine = create_engine(connect_alchemy)
        df.to_sql(tableName, con=engine, index=False, if_exists='append',chunksize = 1000)
        print("Data inserted using to_sql()(sqlalchemy) done successfully...")
    except:
        # passing exception to function
        print("Error")

conn_params_dic = {
    "user":"postgres",
    "password":"postgres",
    "host":"localhost",
    "port":"5433",
    "database":"DASHBOARD"}

# tableName = "OP_CARPARK_BRANCH"
# columns = ["CARPARKNUMBER", "BRANCHCODE"]

# tableName = "PARKING_TRANSACTIONS"
# columns = ["SESSIONID", "PREVSESSIONID","LICENSEPLATE","CARPARKNUMBER","VEHICLETYPE","LABEL_TYPE","TRANSACTION_TYPE","SESSIONSTART","SESSIONEND","TOTAL_CHARGE","DURATION","FIRST_ADVISORY_SENT_AT","FINAL_ADVISORY_SENT_AT","PAYMENT_STATUS","BILLINGID","PAYMENTID","TXN_STATUS","PATCH_STATUS","REDEMPTION","EFFECTIVE_CHARGE","REDEMPTION_TYPE"]

tableName = "OP_LABEL_TYPES"
columns = ["SUBSESSIONTYPE", "COUNT"]


#Setup your DB Property here.
connect_alchemy = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
    conn_params_dic['user'],
    conn_params_dic['password'],
    conn_params_dic['host'],
    conn_params_dic['port'],
    conn_params_dic['database']
)
#Mention the csv file here.
# df = pd.read_csv("OP_CARPARK_BRANCH.csv")

# df = pd.read_csv("PARKING_TRANSACTIONS.csv")

df = pd.read_csv("OP_LABEL_TYPES.csv")
print(df)
using_alchemy(df)
