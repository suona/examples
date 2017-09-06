import dsdbc
import pandas as pd

df = pd.read_csv('ivp-german-data.csv', delimiter=',', 
                            names = ["checkingAccount",
                                     "duration",
                                     "creditHistory",
                                     "purpose",
                                     "amount",
                                     "savingsAccount",
                                     "employed",
                                     "installmentRate",
                                     "gender",
                                     "otherDebtors",
                                     "residentYears",
                                     "property",
                                     "age",
                                     "installmentPlans",
                                     "housing",
                                     "existingCredits",
                                     "job",
                                     "dependents",
                                     "telephone",
                                     "foreign",
                                     "risk"])

#Replace ssid with the subsystem ID of the local data service 
#server. If this is not specified, the name will be selected based on 
#the server group, 'sgrp', or if not provided, the first subsystem with 
#a Data Service will be used. For more information please run help(dsdbc)
ssid = "<SUBSYSTEM_ID_OF_LOCAL_DATA_SERVICE_SERVER>"

#Replace ps_dataset with the name of the empty physical sequential dataset
ps_dataset = "<NAME_OF_PHYSICAL_SEQUENTIAL_DATASET>"
conn = dsdbc.connect(ssid=ssid)
cursor = conn.cursor()
for index, row in df.iterrows():
    cursor.execute(
      "insert into " + ps_dataset + " values({0},'{1}',{2},'{3}','{4}',{5},'{6}','{7}',{8},'{9}','{10}',{11},'{12}',{13},'{14}','{15}',{16},'{17}',{18},'{19}','{20}',{21})".format(
      index,
      row[0],
      row[1],
      row[2],
      row[3],
      row[4],
      row[5],
      row[6],
      row[7],
      row[8],
      row[9],
      row[10],
      row[11],
      row[12],
      row[13],
      row[14],
      row[15],
      row[16],
      row[17],
      row[18],
      row[19],
      row[20]))
