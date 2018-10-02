import pandas as pd
import numpy as np
import pyodbc
import sys


def nri_pull():
	try:
		connection = pyodbc.connect(r'Driver={SQL Server Native Client 11.0};'
									r'Server=SQLDW-L48.BP.Com;'
									r'Database=TeamOperationsAnalytics;'
									r'trusted_connection=yes'
									)
	except pyodbc.Error:
		print("Connection Error")
		sys.exit()

	cursor = connection.cursor()
	SQLCommand = ("""
		DROP TABLE IF EXISTS #PropertyTable
		DROP TABLE IF EXISTS #Final

		SELECT [ARRG_KEY]
				,[ARRG_DSCR]
				,[ARRG_STAT_CODE]
				,[LAND_CLS_CODE]
				,[AGMT_NUM]
				,[ARRG_PROP_STAT_CODE]
				,[ARRG_TYPE_CODE]
				,[AGMT_NAME]
				,CASE WHEN (AGMT_NAME LIKE '%CHAMPLIN%' AND (ARRG_PROP_STAT_CODE LIKE 'HPR' OR ARRG_PROP_STAT_CODE LIKE 'EXT' OR ARRG_PROP_STAT_CODE LIKE 'PAY')) THEN 'Champlin HBP'
					WHEN (AGMT_NAME LIKE '%BLM - W%' AND (ARRG_PROP_STAT_CODE LIKE 'HPR' OR ARRG_PROP_STAT_CODE LIKE 'EXT' OR ARRG_PROP_STAT_CODE LIKE 'PAY') AND EFFTV_DATE < '2008-08-02') THEN 'BLM/State HBP'
					WHEN (AGMT_NAME LIKE '%BLM W%' AND (ARRG_PROP_STAT_CODE LIKE 'HPR' OR ARRG_PROP_STAT_CODE LIKE 'EXT' OR ARRG_PROP_STAT_CODE LIKE 'PAY') AND EFFTV_DATE < '2008-08-02') THEN 'BLM/State HBP'
					WHEN (AGMT_NAME LIKE '%WYW%' AND (ARRG_PROP_STAT_CODE LIKE 'HPR' OR ARRG_PROP_STAT_CODE LIKE 'EXT' OR ARRG_PROP_STAT_CODE LIKE 'PAY') AND EFFTV_DATE < '2013-08-02') THEN 'BLM/State HBP'
					WHEN (AGMT_NAME LIKE '%WYOMING%' AND (ARRG_PROP_STAT_CODE LIKE 'HPR' OR ARRG_PROP_STAT_CODE LIKE 'EXT' OR ARRG_PROP_STAT_CODE LIKE 'PAY') AND EFFTV_DATE < '2013-08-02') THEN 'BLM/State HBP'
					WHEN (AGMT_NAME LIKE '%BLM - W%' AND EFFTV_DATE >= '2008-08-02' AND ARRG_PROP_STAT_CODE NOT LIKE '3PP') THEN 'Primary Term BLM'
					WHEN (AGMT_NAME LIKE '%BLM W%' AND EFFTV_DATE >= '2008-08-02' AND ARRG_PROP_STAT_CODE NOT LIKE '3PP') THEN 'Primary Term BLM'
					WHEN (AGMT_NAME LIKE '%WYOMING%' AND EFFTV_DATE >= '2013-08-02' AND ARRG_PROP_STAT_CODE NOT LIKE '3PP') THEN 'Primary Term State'
					WHEN (AGMT_NAME LIKE '%WYW%' AND EFFTV_DATE >= '2013-08-02' AND ARRG_PROP_STAT_CODE NOT LIKE '3PP') THEN 'Primary Term State'
					WHEN (ARRG_PROP_STAT_CODE LIKE '3PP') THEN 'Outside Operated'
					--WHEN ARRG_PROP_STAT_CODE LIKE 'HPR' AND EFFTV_DATE < '2013-08-02' THEN 'BLM/State HBP'
				ELSE 'other' END AS PropertyType
				,[SUBJ_CODE]
				,[EFFTV_DATE]
				,[EXPR_DATE]
				,[UPDT_DATE]
				,[AGMT_DATE_M]
				,[CORP_DATE_M]
			INTO #PropertyTable
			FROM [EDW].[QLS].[ALL_AGREEMENTS]
			WHERE SUBJ_CODE IN ('LSE', 'FDA', 'STA', 'STU')
			AND AGMT_NAME NOT LIKE '%FAMILY TREE%';
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		SELECT DISTINCT		AL.AGMT_NUM AS LeaseNum
							,SUBSTRING(LD.SECTION, PATINDEX('%[^0]%', LD.SECTION+'.'), LEN(LD.SECTION)) + ' ' +
								SUBSTRING(LD.TOWNSHIP, PATINDEX('%[^0]%', LD.TOWNSHIP+'.'), LEN(LD.TOWNSHIP)) + ' ' +
								SUBSTRING(LD.RANGE, PATINDEX('%[^0]%', LD.RANGE+'.'), LEN(LD.RANGE)) AS TRS
							,CASE WHEN PT.PropertyType = 'Champlin HBP' THEN 75
									WHEN PT.PropertyType = 'BLM/State HBP' THEN 50
									WHEN PT.PropertyType = 'Primary Term BLM' THEN 100
									WHEN PT.PropertyType = 'Primary Term State' THEN 100
									WHEN PT.PropertyType = 'Outside Operated' THEN 32.3
								END AS WorkingInterest
							,CASE WHEN PT.PropertyType = 'Champlin HBP' THEN 62.81
									WHEN PT.PropertyType = 'BLM/State HBP' THEN 43.33
									WHEN PT.PropertyType = 'Primary Term BLM' THEN 86.65
									WHEN PT.PropertyType = 'Primary Term State' THEN 82.91
									WHEN PT.PropertyType = 'Outside Operated' THEN 29.7
								END AS NRI
		  INTO #Final
		  FROM [EDW].[QLS].[LD_JEFF_HEADER] LD
		  JOIN [EDW].[QLS].[ALL_AGREEMENTS] AL
		    ON AL.ARRG_KEY = LD.LEGAL_DESC_KEY
		  JOIN #PropertyTable PT
		    ON PT.AGMT_NUM = AL.AGMT_NUM
		  WHERE AL.ARRG_TYPE_CODE IN ('LSE', 'FDA', 'STA', 'STU')
	""")

	cursor.execute(SQLCommand)

	SQLCommand = ("""
		SELECT	F.TRS
				,AVG(F.WorkingInterest) WorkingInterest
				,AVG(F.NRI) NRI
		FROM #Final F
		JOIN (SELECT TRS, COUNT(*) AS cnt
		      FROM #Final
			  WHERE WorkingInterest IS NOT NULL
			  GROUP BY TRS) Doops
		  ON F.TRS = Doops.TRS
		WHERE WorkingInterest IS NOT NULL
		GROUP BY F.TRS
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()

	df = pd.DataFrame.from_records(results)
	connection.close()

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
	except:
		df = None
		print('Dataframe is empty')

	return df.drop_duplicates()

def well_to_lease(w_df, n_df):
	df = pd.merge(w_df, n_df, on='AGMT_NUM')

	def sectwnrng(row):
		return str(row['SECT']) + ' ' + \
			   str(row['TWP']) + str(row['TDIR']) + ' ' + \
			   str(row['RNG']) + str(row['RDIR'])

	df['SecTwnRng'] = df.apply(sectwnrng, axis=1)
	df.drop_duplicates(inplace=True)
	leases = df[['SecTwnRng', 'AGMT_NUM']].groupby('SecTwnRng', as_index=False).min()
	match_df = df.loc[df['AGMT_NUM'].isin(leases['AGMT_NUM']), ['SecTwnRng', 'WorkingInterest', 'NRI']]
	match_df = match_df.groupby('SecTwnRng', as_index=False).min()

def tracker_format(df):
	df.rename(index=str, columns={'Sec':'Entry Sec', 'Twn':'Entry Twn',
								  'Rng':'Entry Rng', 'Sec.1':'Bottom Sec',
								  'Twn.1':'Bottom Twn', 'Rng.1':'Bottom Rng'},
					  inplace=True)
	df.loc[:, 'Entry Sec'] = pd.to_numeric(df.loc[:, 'Entry Sec'], errors='corece')
	df.loc[:, 'Entry Sec'].fillna(0, inplace=True)
	df.loc[:, 'Bottom Sec'] = pd.to_numeric(df.loc[:, 'Bottom Sec'], errors='corece')
	df.loc[:, 'Bottom Sec'].fillna(0, inplace=True)

	def esectwnrng(row):
		return str(int(row['Entry Sec'])) + ' ' + str(row['Entry Twn']) + ' ' + str(row['Entry Rng'])

	def bsectwnrng(row):
		return str(int(row['Bottom Sec'])) + ' ' + str(row['Bottom Twn']) + ' ' + str(row['Bottom Rng'])

	df['EntrySecTwnRng'] = df.apply(esectwnrng, axis=1)
	df['BottomSecTwnRng'] = df.apply(bsectwnrng, axis=1)

	return df


if __name__ == '__main__':
	# well_lease_df = pd.read_csv('data/Well to Lease.csv')
	# nri_df = nri_pull()
	# w_df = well_lease_df[['AGMT_NUM', 'SECT', 'RDIR', 'RNG', 'TDIR', 'TWP']]
	# n_df = nri_df[['AGMT_NUM', 'AGMT_NAME', 'PropertyType', 'WorkingInterest', 'NRI']]
	# n_df.loc[:, 'AGMT_NUM'] = n_df.loc[:, 'AGMT_NUM'].astype(int)
	# match_df = well_to_lease(well_lease_df, nri_df)

	match_df = nri_pull()

	concept_df = pd.read_csv('data/concept_tracker.csv', header=1, encoding='ISO-8859-1')
	concept_df = tracker_format(concept_df)

	return_df = concept_df.merge(match_df, left_on='EntrySecTwnRng', right_on='TRS', how='left')
	final_df = return_df.merge(match_df, left_on='BottomSecTwnRng', right_on='TRS', how='left')

	final_df.replace(np.nan, 0, inplace=True)

	final_df.loc[:, 'WorkingInterest'] = (final_df.loc[:, 'WorkingInterest_x'] + \
										  final_df.loc[:, 'WorkingInterest_y']) / 2
	final_df.loc[:, 'NRI'] = (final_df.loc[:, 'NRI_x'] + \
							  final_df.loc[:, 'NRI_y']) / 2
	final_df.to_csv('data/concept_wi.csv')
