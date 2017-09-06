import dsdbc
import pandas as pd

df = pd.read_csv('data/german.data', delimiter=' ', 
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

conn = dsdbc.connect(ssid="AZKS")
cursor = conn.cursor()
for index, row in df.iterrows():
    cursor.execute(
      "insert into CREDIT_DATA values({0},'{1}',{2},'{3}','{4}',{5},'{6}','{7}',{8},'{9}','{10}',{11},'{12}',{13},'{14}','{15}',{16},'{17}',{18},'{19}','{20}',{21})".format(
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

