import pandas as pd
from os import listdir
from os import path

# Specify the directory containing the data files
data_directory = 'data'

# List to hold individual dataframes
dataframes = []

# Loop through all files in the data directory
print('Processing the following files....')
for filename in listdir(data_directory):
	# Construct full file path
	file_path = path.join(data_directory, filename)
	# Check if the file is a CSV file
	if filename.endswith('.csv'):
		print(filename)
		# Read the CSV file into a DataFrame
		df = pd.read_csv(file_path, header=0, usecols=['APPLICATION_ID','ACTIVITY','ADMINISTERING_IC','APPLICATION_TYPE','ORG_NAME','PI_IDS','PI_NAMEs','PROJECT_TITLE','SUPPORT_YEAR','DIRECT_COST_AMT','TOTAL_COST','TOTAL_COST_SUB_PROJECT'], encoding_errors='replace', low_memory=False, sep=',')
		# Append the DataFrame to the list
		dataframes.append(df)

# Concatenate all DataFrames in the list into a single DataFrame
cd = pd.concat(dataframes, ignore_index=True)

# Remove all projects that are requests to change institutions or requests to change IC
indexApplication = cd[(cd['APPLICATION_TYPE']==7) | (cd['APPLICATION_TYPE']==9)].index
cd.drop(indexApplication , inplace=True)

# Calculate the number of projects received by each organization between 2014 and 2023 (inclusive)
org_total_awards = cd['ORG_NAME'].value_counts(ascending=False).reset_index()

# Identify all organizations receiving more than $3 billion in direct costs between 2014 and 2023 (inclusive)
org_direct_cost = cd.groupby(['ORG_NAME']).DIRECT_COST_AMT.sum().reset_index()
top_org_DC = org_direct_cost[org_direct_cost['DIRECT_COST_AMT']>=3000000000]
top_org_DC = top_org_DC.merge(org_total_awards, how='inner', on='ORG_NAME')

# Identify all organizations receiving more than $3 billion in total costs between 2014 and 2023 (inclusive)
org_total_cost = cd.groupby(['ORG_NAME']).TOTAL_COST.sum().reset_index()
top_org_TC = org_total_cost[org_total_cost['TOTAL_COST']>=3000000000]
top_org_TC = top_org_TC.merge(org_total_awards, how='inner', on='ORG_NAME')

# Create a smaller dataframe for the contact-PI-specific data
pi_df = cd[['ORG_NAME','PI_IDS','PI_NAMEs','PROJECT_TITLE','SUPPORT_YEAR','DIRECT_COST_AMT','TOTAL_COST','TOTAL_COST_SUB_PROJECT']]

# Define a function to split out each contact PI on their own row
def find_contact(row):
	pis = row['PI_IDS']
	if type(pis) != float:
		pis = pis.split(';')
		if len(pis) == 1:
			for pi in pis:
				pi = pi.strip(';')
				pi = pi.strip('(contact)')
				pi = pi.strip(' ')
				return str(pi) + '|'  + str(row['ORG_NAME']) + '|' + str(row['DIRECT_COST_AMT']) + '|' + str(row['TOTAL_COST'])
		else:
			for pi in pis:
				if 'contact' in pi:
					pi = pi.strip(' (contact)')
					return str(pi) + '|' + str(row['ORG_NAME']) + '|' + str(row['DIRECT_COST_AMT']) + '|' + str(row['TOTAL_COST'])

# Apply the function and create a new dataframe with each contact PI on a new row
cpi_df = pi_df.apply(find_contact, axis=1, result_type='expand')
api_df = cpi_df.to_frame()
api_df.columns=['ALL']
api_df[['PI_ID','ORG_NAME','DIRECT_COST_AMT','TOTAL_COST']] = api_df['ALL'].str.split(pat='|', expand=True)
api_df['DIRECT_COST_AMT'] = pd.to_numeric(api_df['DIRECT_COST_AMT'], errors='coerce')
api_df['TOTAL_COST'] = pd.to_numeric(api_df['TOTAL_COST'], errors='coerce')
api_df.drop(columns='ALL', inplace=True)

# Identify top contact PIs for direct costs between 2014 and 2023 (inclusive)
pi_direct_cost = api_df.groupby(['PI_ID']).DIRECT_COST_AMT.sum().reset_index()
top_pi_DC = pi_direct_cost[pi_direct_cost['DIRECT_COST_AMT']>=200000000]

# Identify top contact PIs for total costs between 2014 and 2023 (inclusive)
pi_total_cost = api_df.groupby(['PI_ID']).TOTAL_COST.sum().reset_index()
top_pi_TC = pi_total_cost[pi_total_cost['TOTAL_COST']>=200000000]

# Calculate number of unique contact PIs for each organization from 2014 to 2023 (inclusive)
bpi_df = api_df[['ORG_NAME','PI_ID']].copy()
org_total_pis = bpi_df.groupby('ORG_NAME')['PI_ID'].nunique()

# Add number of unique contact PIs to organizations receiving more than $3 billion
top_org_DC = top_org_DC.merge(org_total_pis, how='inner', on='ORG_NAME')
top_org_TC = top_org_TC.merge(org_total_pis, how='inner', on='ORG_NAME')

# format currency
top_org_DC['DIRECT_COST_AMT'] = top_org_DC['DIRECT_COST_AMT'] / 1000000000
top_org_DC['DIRECT_COST_AMT'] = top_org_DC['DIRECT_COST_AMT'].map("${:,.2f}B".format)
top_org_TC['TOTAL_COST'] = top_org_TC['TOTAL_COST'] / 1000000000
top_org_TC['TOTAL_COST'] = top_org_TC['TOTAL_COST'].map("${:,.2f}B".format)
top_pi_DC['DIRECT_COST_AMT'] = top_pi_DC['DIRECT_COST_AMT'] / 1000000
top_pi_DC['DIRECT_COST_AMT'] = top_pi_DC['DIRECT_COST_AMT'].map("${:,.2f}M".format)
top_pi_TC['TOTAL_COST'] = top_pi_TC['TOTAL_COST'] / 1000000
top_pi_TC['TOTAL_COST'] = top_pi_TC['TOTAL_COST'].map("${:,.2f}M".format)

# Rename column headers to be more informative
top_org_DC = top_org_DC.rename(columns={'ORG_NAME':'Organization Name','DIRECT_COST_AMT':'Total Direct Costs','count':'Total Projects','PI_ID':'Unique Contact PIs'})
top_org_TC = top_org_TC.rename(columns={'ORG_NAME':'Organization Name','TOTAL_COST':'Total Costs','count':'Total Projects','PI_ID':'Unique Contact PIs'})
top_pi_DC = top_pi_DC.rename(columns={'DIRECT_COST_AMT':'Total Direct Costs','PI_ID':'Contact PI ID'})
top_pi_TC = top_pi_TC.rename(columns={'TOTAL_COST':'Total Costs','PI_ID':'Contact PI ID'})

# Print titles and tables
print('Organizations that received more than $3 billion in direct costs from 2014-2023')
print(top_org_DC)
print('Organizations that received more than $3 billion in total costs from 2014-2023')
print(top_org_TC)
print('Contact PIs that received more than $200 million in direct costs from 2014-2023')
print(top_pi_DC)
print('Contact PIs that received more than $200 million in total costs from 2014-2023')
print(top_pi_TC)