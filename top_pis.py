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

top_pi = ['11529936','1860045','1872615','1873940','1882834','1889666','1986254','1991449','2059843','6297517','6365670','6765159','7897640']

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

#Remove all information for contact PIs that are not in the top_pi list
tpi_df = api_df[api_df['PI_ID'].isin(top_pi)]
tpi_df.to_csv('pi_data.tsv', sep='\t', index=False) 
