from pyspark.sql import SparkSession
import pyspark.sql.functions as F

url = 'jdbc:postgresql://localhost:5432/spark'
table = 'coba'
driver = 'org.postgresql.Driver'
user = ''
password = ''

spark = SparkSession.builder.appName('SBAnational').getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')

# you can download csv here: https://www.kaggle.com/code/alditopatriza1999/sba-default-loan-classification/data
df = spark.read.csv('SBAnational.csv', inferSchema=True, header=True)

def convert_date(col, formats=('dd-MMM-yy')):
    return F.coalesce(*[F.to_date(col, x) for x in formats])

df = (
    df.withColumn('ApprovalDate', F.when(F.length('ApprovalDate') > 7, df['ApprovalDate']).otherwise(F.lit('01-Jan-70'))) # replace anomaly value to 01-Jan-1970
    .withColumn('ChgOffDate', F.when(F.length('ChgOffDate') > 7, df['ChgOffDate']).otherwise(F.lit('01-Jan-70')))
    .withColumn('DisbursementDate', F.when(F.length('DisbursementDate') > 7, df['DisbursementDate']).otherwise(F.lit('01-Jan-70')))
    .withColumn('ApprovalDate', convert_date('ApprovalDate'))
    .withColumn('ChgOffDate', convert_date('ChgOffDate'))
    .withColumn('DisbursementDate', convert_date('DisbursementDate'))
    .withColumn('LoanNr_ChkDgt', df['LoanNr_ChkDgt'].cast('string'))
    .withColumn('NAICS', df['NAICS'].cast('string'))
    .withColumn('FranchiseCode', df['FranchiseCode'].cast('string'))
    .withColumn('ApprovalFY', df['ApprovalFY'].cast('integer'))
    .withColumn('DisbursementGross', F.regexp_replace('DisbursementGross','[$,]','').cast('decimal(9,2)'))
    .withColumn('BalanceGross', F.regexp_replace('BalanceGross','[$,]','').cast('decimal(9,2)'))
    .withColumn('ChgOffPrinGr', F.regexp_replace('ChgOffPrinGr','[$,]','').cast('decimal(9,2)'))
    .withColumn('GrAppv', F.regexp_replace('GrAppv','[$,]','').cast('decimal(9,2)'))
    .withColumn('SBA_Appv', F.regexp_replace('SBA_Appv','[$,]','').cast('decimal(9,2)'))
)

df.write.format('jdbc').mode('append').option('driver', driver).option('url', url).option('dbtable', table).option('user', user).option('password', password).save()
